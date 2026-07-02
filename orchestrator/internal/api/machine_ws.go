package api

// machine_ws.go — WS-handshake аутентификации машины по UUID (тикет 2.3,
// FR B3, B6, Gherkin §2 «Аутентификация машины по UUID», «IP — необязательная
// проверка»).
//
// Назначение (бизнес): агент (демон на машине пользователя) держит исходящее
// WebSocket-соединение к оркестратору (docs/01_tech_stack_and_architecture.md
// [РЕШЕНИЕ 1], docs/protocol.md §1) и должен быть опознан как конкретная
// интеграция ДО того, как ему начнут адресовать команды. Опознание — по
// UUID-секрету, который владелец получил при создании интеграции (тикет 2.2,
// POST /integrations) и передал агенту при настройке (FR B2). Сам UUID в
// первом кадре («hello», docs/protocol.md §2/§4) ищется по HMAC-отпечатку
// (FR B6) — без хранения секрета в открытом виде, тем же путём, что и при
// создании (integrations.go, internal/crypto). Дополнительно, если владелец
// указал ip_hint при настройке интеграции, IP TCP-пира должен ему совпасть —
// это НЕОБЯЗАТЕЛЬНАЯ доп. проверка поверх основной (HMAC), а не отдельный
// механизм безопасности: пустой ip_hint пропускает любой IP (FR B3, §2 «IP —
// необязательная проверка»).
//
// Любая причина отказа (UUID не найден по HMAC, UUID найден но IP не совпал,
// первый кадр не парсится или не является hello) даёт ОДИН и тот же
// внешний сигнал — закрытие соединения кодом wsCloseUnauthorized с reason
// "unauthorized", без утечки причины клиенту. Это тот же принцип единого
// отказа, что уже применяется в auth.ParseAccessToken и PostAuthLogin
// (orchestrator/internal/api/auth.go, middleware.go) — разные коды/reason на
// разные причины отказа дали бы атакующему лишний сигнал (например, различили
// бы «такого UUID не существует» от «UUID верный, но IP не тот»). Причина
// логируется на стороне сервера (s.logger) для диагностики, но не
// возвращается клиенту.
//
// Защита от повторного UUID (тикет 2.4, FR B6, ADR 0002 "machine-ws duplicate
// uuid policy"): на одну интеграцию в любой момент существует не больше
// ОДНОГО активного WS-соединения. Если второй коннект успешно проходит ту же
// аутентификацию (тот же integration_id) при уже активном первом — сервер
// закрывает СТАРОЕ соединение кодом wsCloseSuperseded и принимает новое
// ("close old, accept new" / last-connection-wins). Решение и обоснование —
// см. ADR 0002: агент по дизайну (тикет 3.3) держит исходящий WS с
// авто-реконнектом, и после сетевого сбоя естественный сценарий — переподключиться
// до того, как сервер формально узнает о смерти старого TCP-соединения;
// политика "отвергнуть новое" заблокировала бы легитимный реконнект на срок
// до OFFLINE_THRESHOLD (§6). Реестр активных соединений — s.machineConns,
// см. registerMachineConn/unregisterMachineConn.
//
// Как устроено (тех): контракт (api/openapi.yaml, /machine/ws) формально
// перечисляет ответы "101"/"401" — это документальное упрощение, OpenAPI не
// умеет нормально моделировать WS-handshake. Физически после успешного
// WS-апгрейда (101 Switching Protocols) кастомный HTTP-статус вернуть уже
// нельзя — сервер не может "передумать" и ответить 401 поверх уже
// отправленного 101. Поэтому GetMachineWs ВСЕГДА выполняет апгрейд
// (websocket.Accept), затем читает первый кадр и парсит его как конверт
// docs/protocol.md §2 с type=="hello" и payload {uuid, agent_version,
// providers[]} (docs/protocol.md §4). Провал любой из этих проверок —
// функциональный эквивалент "401" из контракта: закрытие WS-соединения кодом
// close 4401 (приватный диапазон 4000-4999, RFC 6455 §7.4.2) и reason
// "unauthorized". При успехе соединение остаётся открытым; read-loop
// (тикеты 3.4/3.6/5.4/5.8/6.1/6.4/8.1) разбирает каждый дальнейший кадр (см.
// handleMachineFrame/parseAckFrame/handleMachineEvent/handleAgentQuestion/
// handleTaskAccepted/handleAgentError/handleCommandApprovalRequest/
// handleAgentCompleted) и активно обрабатывает семь типов:
// type==ack — пересылается зарегистрированному s.ackSink (мосту оркестратора
// machine.commands → WS, commit-after-ACK, protocol.md §5, см. godoc AckSink
// в server.go); type==heartbeat — публикуется через зарегистрированный
// s.eventSink (presence-подсистема, FR B4, protocol.md §6, см. godoc
// EventSink в server.go) с ПЕРЕЗАПИСАННЫМ на аутентифицированный DB id полем
// IntegrationID, после чего агенту отправляется ack; type==agent_question
// (FR F1, тикет 6.1) — переводит задачу running→waiting_user через
// taskTransitioner.TransitionWithEvent СИНХРОННО (без отдельного
// Redpanda-потребителя, тот же приём, что и у PostTasks/тикет 5.3), см.
// handleAgentQuestion; type==task_accepted (FR E1, тикет 5.4) — переводит
// задачу queued→running через taskTransitioner.Transition, см.
// handleTaskAccepted; type==error (FR E1, тикет 5.8) — переводит задачу
// running→failed через taskTransitioner.TransitionWithEvent, см.
// handleAgentError; type==command_approval_request (FR F3, тикет 6.4,
// Gherkin §5 «Команда вне allowlist требует согласования») — переводит
// задачу running→waiting_user через taskTransitioner.TransitionWithEvent, см.
// handleCommandApprovalRequest; type==agent_completed (FR E2, тикет 8.1) —
// переводит задачу running→awaiting_confirm через
// taskTransitioner.TransitionWithEvent (НЕ закрывает задачу, закрытие требует
// явного действия пользователя), см. handleAgentCompleted. Любой другой тип
// кадра и любой нераспознанный/битый кадр МОЛЧА игнорируются — ни паники, ни закрытия
// соединения (нужно и чтобы коннект не выглядел повисшим, и чтобы
// control-фреймы coder/websocket обрабатывались штатно — см. godoc
// websocket.Conn "You must always read from the connection").
//
// IP клиента берётся из net.SplitHostPort(r.RemoteAddr) — прямого TCP-пира,
// БЕЗ доверия заголовку X-Forwarded-For: в MVP нет инфраструктуры доверенных
// прокси/allowlist для него (Caddy — единственный реверс-прокси перед
// оркестратором, но его адрес не верифицируется на этом пути), а доверять
// клиентскому заголовку напрямую небезопасно — он тривиально подделывается.
// Сознательное ограничение MVP, не баг; ip_hint в любом случае
// необязательная проверка (см. выше), основной механизм — HMAC от UUID.
//
// Совместимость версий (тикет 4.7, FR C5, ADR 0003 "machine-ws protocol
// version compat"): authenticateMachineHello сравнивает env.ProtocolVersion
// (поле конверта hello) с bus.ProtocolVersion СРАЗУ после подтверждения, что
// первый кадр — hello, и ДО похода в БД за интеграцией (GetIntegrationByUUIDHMAC)
// — проверка дешёвая и не секретно-чувствительна, в отличие от auth. При
// несовпадении соединение закрывается ОТДЕЛЬНЫМ кодом wsCloseIncompatibleProtocolVersion
// (4426) с содержательной причиной (got/want) — в отличие от единого
// "unauthorized" для auth-провалов (см. выше), это НЕ auth-сигнал, и раскрытие
// причины не даёт атакующему ничего. Проверяется ТОЛЬКО protocol_version;
// agent_version (bus.HelloPayload.AgentVersion) сознательно НЕ проверяется —
// только логируется для диагностики (см. ADR 0003 "Альтернативы": semver/
// MinAgentVersion — вне объёма этого тикета).
import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/yarabey/agentify/internal/bus"
	"github.com/yarabey/agentify/internal/crypto"
	"github.com/yarabey/agentify/orchestrator/internal/db"
	"github.com/yarabey/agentify/orchestrator/internal/metrics"
	"github.com/yarabey/agentify/orchestrator/internal/notify"
	"github.com/yarabey/agentify/orchestrator/internal/task"
)

// wsCloseUnauthorized — close-код, которым GetMachineWs закрывает
// WS-соединение при провале аутентификации машины (см. godoc файла).
// 4401 выбран из приватного диапазона 4000-4999 (RFC 6455 §7.4.2,
// "Reserved for private use") как мнемоника к HTTP 401 Unauthorized из
// контракта — не зарезервирован спецификацией/библиотекой и однозначно
// отличим от стандартных кодов закрытия (1000-1015).
const wsCloseUnauthorized websocket.StatusCode = 4401

// wsCloseSuperseded — close-код, которым registerMachineConn закрывает
// СТАРОЕ WS-соединение интеграции, когда его вытесняет новое успешно
// аутентифицированное соединение той же интеграции (тикет 2.4, FR B6,
// ADR 0002, см. godoc файла). По аналогии с wsCloseUnauthorized — приватный
// диапазон 4000-4999 (RFC 6455 §7.4.2); 4409 — мнемоника к HTTP 409 Conflict
// (конфликтующее, вытесненное соединение), а не к 401: причина закрытия
// принципиально другая (не провал аутентификации — обе стороны конфликта
// предъявили одинаково валидный секрет).
const wsCloseSuperseded websocket.StatusCode = 4409

// wsCloseIncompatibleProtocolVersion — close-код, которым
// authenticateMachineHello закрывает соединение, когда protocol_version
// присланного hello-конверта не совпадает с bus.ProtocolVersion (тикет 4.7,
// FR C5, ADR 0003 "machine-ws protocol version compat", docs/protocol.md §7:
// "На hello оркестратор сверяет protocol_version; несовместимые —
// отклоняет"). Приватный диапазон 4000-4999 (RFC 6455 §7.4.2), как и у
// wsCloseUnauthorized/wsCloseSuperseded; 4426 — мнемоника к HTTP 426 Upgrade
// Required (клиенту с несовместимым протоколом нужно "обновиться").
//
// В отличие от wsCloseUnauthorized этот код — НЕ сигнал провала
// аутентификации: проверка protocol_version выполняется ДО похода в БД за
// интеграцией (см. authenticateMachineHello) и её результат не раскрывает
// атакующему ничего об UUID/HMAC. Поэтому, в отличие от единого нарочно-
// обобщённого "unauthorized" (см. godoc файла), здесь уместна и нужна
// содержательная close-reason (got/want) — см. ADR 0003.
const wsCloseIncompatibleProtocolVersion websocket.StatusCode = 4426

// machineHelloReadTimeout — сколько GetMachineWs ждёт первый кадр (hello)
// после успешного WS-апгрейда, прежде чем считать аутентификацию
// провалившейся. Конкретное значение не зафиксировано протоколом/тикетом —
// 10s достаточно агенту собрать и отправить hello сразу после установления
// соединения, не давая медленному/зависшему клиенту держать handshake
// бесконечно.
const machineHelloReadTimeout = 10 * time.Second

// machineEventAckWriteTimeout — сколько handleMachineEvent ждёт запись
// ack-кадра в ответ на успешно обработанное событие машины (heartbeat, тикет
// 3.6, protocol.md §5/§6), прежде чем считать запись провалившейся и просто
// залогировать ошибку. По аналогии с machineHelloReadTimeout/bridge.writeTimeout
// — разумный таймаут на одну сетевую операцию записи, не завязанный на
// HEARTBEAT_INTERVAL/OFFLINE_THRESHOLD.
const machineEventAckWriteTimeout = 10 * time.Second

// GetMachineWs реализует GET /machine/ws — WS-handshake с аутентификацией
// машины по UUID (FR B3, B6, тикет 2.3, см. godoc файла).
//
// Переопределяет 501-заглушку Unimplemented. Апгрейд выполняется всегда
// (см. godoc файла, почему); отказ аутентификации сигнализируется закрытием
// соединения кодом wsCloseUnauthorized, не HTTP-статусом.
func (s *Server) GetMachineWs(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		// websocket.Accept сам пишет ответ в w при ошибке (не WS-запрос,
		// нарушение handshake и т.п.) — это происходит ДО апгрейда, обычный
		// HTTP-путь, добавлять здесь нечего.
		return
	}
	defer func() { _ = conn.CloseNow() }()

	integrationID, ok := s.authenticateMachineHello(r, conn)
	if !ok {
		_ = conn.Close(wsCloseUnauthorized, "unauthorized")
		return
	}

	// Защита от повторного UUID (тикет 2.4, FR B6, ADR 0002): регистрируем
	// это соединение как активное для integrationID, вытесняя предыдущее,
	// если оно было. Снятие с регистрации — строго по compare-and-delete
	// (см. unregisterMachineConn), чтобы не задеть запись более нового
	// соединения, которое могло вытеснить ЭТО же между регистрацией и сюда.
	s.registerMachineConn(integrationID, conn)
	defer s.unregisterMachineConn(integrationID, conn)

	// Успешный hello: соединение остаётся открытым. Read-loop (тикеты
	// 3.4/3.6/5.4/6.1): разбираем каждый дальнейший кадр и маршрутизируем по
	// типу (см. handleMachineFrame) — ack пересылается мосту оркестратора
	// (s.ackSink), heartbeat публикуется presence-подсистеме (s.eventSink, см.
	// handleMachineEvent), agent_question переводит задачу в waiting_user (см.
	// handleAgentQuestion), task_accepted переводит задачу в running (см.
	// handleTaskAccepted), error переводит задачу в failed (FR E1, тикет
	// 5.8, см. handleAgentError), command_approval_request переводит задачу
	// в waiting_user (FR F3, тикет 6.4, см. handleCommandApprovalRequest),
	// agent_completed переводит задачу в awaiting_confirm, НЕ закрывая её
	// (FR E2, тикет 8.1, см. handleAgentCompleted); любой иной тип кадра
	// (а также нераспознанный/битый JSON) МОЛЧА
	// игнорируется — получение такого кадра не должно ронять или закрывать
	// соединение (см. godoc файла).
	for {
		_, data, err := conn.Read(r.Context())
		if err != nil {
			return
		}
		s.handleMachineFrame(r.Context(), conn, integrationID, data)
	}
}

// handleMachineFrame разбирает один кадр, полученный ПОСЛЕ успешного hello
// (тикеты 3.4/3.6/5.4/6.1, protocol.md §5/§6). Кадр разбирается как конверт
// ОДИН раз и маршрутизируется по env.Type:
//   - type==ack — commit-after-ack для machine.commands, пересылается
//     s.ackSink (см. godoc AckSink в server.go);
//   - type==heartbeat — событие машины (FR B4, protocol.md §6), см.
//     handleMachineEvent;
//   - type==agent_question — вопрос агента пользователю (FR F1, тикет 6.1),
//     см. handleAgentQuestion;
//   - type==task_accepted — агент принял задачу (FR E1, тикет 5.4), см.
//     handleTaskAccepted;
//   - type==error — ошибка агента/машины (FR E1, тикет 5.8), см.
//     handleAgentError;
//   - type==command_approval_request — команда вне allowlist требует
//     согласования пользователя (FR F3, тикет 6.4, Gherkin §5 «Команда вне
//     allowlist требует согласования»), см. handleCommandApprovalRequest;
//   - type==agent_completed — агент сообщил о завершении работы (FR E2,
//     тикет 8.1, Gherkin §7 «Агент сообщил о завершении — задача ещё не
//     закрыта»), см. handleAgentCompleted;
//   - type==agent_progress — прогресс/предупреждение агента, например об
//     отложенной из-за критической операции остановке (FR E6, тикет 8.5,
//     Gherkin §8 «Приоритет сохранности данных при отмене»), записывается в
//     task_events БЕЗ смены статуса задачи, см. handleAgentProgress;
//   - любой другой тип, а также нераспознанный/битый кадр — безопасно
//     игнорируется, без побочных эффектов (ни паники, ни закрытия
//     соединения).
func (s *Server) handleMachineFrame(ctx context.Context, conn *websocket.Conn, integrationID uuid.UUID, data []byte) {
	var env bus.Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return
	}

	switch env.Type {
	case bus.MessageTypeAck:
		// parseAckFrame — та же (само-достаточная) логика разбора ack-кадра,
		// что и раньше (тикет 3.4); повторный разбор data здесь дешёв и
		// сохраняет единственный источник правды для валидации ack-payload,
		// покрытый TestParseAckFrame_*.
		ackMessageID, ok := parseAckFrame(data)
		if !ok {
			return
		}
		sink := s.getAckSink()
		if sink == nil {
			// Мост не зарегистрирован (Redpanda отключён/тест без тикета 3.4) —
			// штатно игнорируем, см. godoc SetAckSink.
			return
		}
		sink.HandleAck(ackMessageID)
	case bus.MessageTypeHeartbeat:
		s.handleMachineEvent(ctx, conn, integrationID, env)
	case bus.MessageTypeAgentQuestion:
		s.handleAgentQuestion(ctx, conn, integrationID, env)
	case bus.MessageTypeTaskAccepted:
		s.handleTaskAccepted(ctx, conn, integrationID, env)
	case bus.MessageTypeError:
		s.handleAgentError(ctx, conn, integrationID, env)
	case bus.MessageTypeAgentCompleted:
		s.handleAgentCompleted(ctx, conn, integrationID, env)
	case bus.MessageTypeCommandApprovalRequest:
		s.handleCommandApprovalRequest(ctx, conn, integrationID, env)
	case bus.MessageTypeAgentProgress:
		s.handleAgentProgress(ctx, conn, integrationID, env)
	default:
		// Прочие типы событий — вне объёма, молча игнорируем.
	}
}

// handleMachineEvent обрабатывает событие машины (сейчас — только heartbeat,
// FR B4, тикет 3.6, protocol.md §6): публикует конверт через
// зарегистрированный s.eventSink и, при успехе, отвечает ack-кадром — тот же
// at-least-once принцип, что и у команд оркестратора (protocol.md §5): если
// публикация не удалась, ack НЕ отправляется, и агент повторит событие сам
// (durable outbox, тикет 3.5).
//
// КРИТИЧНО: env.IntegrationID здесь ВСЕГДА перезаписывается на
// integrationID — DB id этого уже аутентифицированного соединения
// (см. authenticateMachineHello), а НЕ то значение, что прислал агент в
// самом кадре (там — секрет интеграции, plaintext UUID, см.
// wsclient.Config.IntegrationUUID). Доверять присланному значению нельзя:
// во-первых, дальнейшие потребители шины (presence.Consumer и др.) ищут
// интеграцию по DB id, а не по секрету; во-вторых, публикация секрета в
// Redpanda была бы утечкой чувствительных данных на шину сообщений.
func (s *Server) handleMachineEvent(ctx context.Context, conn *websocket.Conn, integrationID uuid.UUID, env bus.Envelope) {
	env.IntegrationID = integrationID.String()

	sink := s.getEventSink()
	if sink == nil {
		// Presence-подсистема не зарегистрирована (Redpanda отключён/тест без
		// тикета 3.6) — штатно игнорируем, см. godoc SetEventSink.
		return
	}

	if err := sink.HandleEvent(ctx, env); err != nil {
		s.logError("EventSink.HandleEvent", err)
		return
	}

	s.writeMachineAck(ctx, conn, integrationID, env.MessageID, "heartbeat")
}

// handleAgentQuestion обрабатывает кадр agent_question (FR F1, тикет 6.1,
// Gherkin §5 «Агент задаёт вопрос и получает ответ»): переводит связанную
// задачу running→waiting_user, атомарно записывая событие agent_question в
// task_events (task.Transitioner.TransitionWithEvent — единственная точка
// смены tasks.status, тикет 5.2), и, при успехе, отвечает агенту ack-кадром
// — тот же at-least-once принцип, что и у handleMachineEvent: провал любого
// шага (невалидный payload/task_id, задача не найдена или принадлежит другой
// интеграции, недопустимый переход FSM, transitioner не настроен) молча
// пропускает ack — агент должен повторить попытку сам (durable outbox на
// стороне агента, тикет 3.5), соединение при этом не закрывается и не
// паникует (см. godoc файла).
//
// Обработка СИНХРОННАЯ, без отдельного Redpanda-потребителя — тот же приём,
// что и PostTasks (тикет 5.3): весь путь агент→FSM укладывается в один вызов
// в рамках уже открытого WS-соединения, отдельный consumer не добавляет
// ничего, кроме задержки и лишнего состояния.
//
// Задача ищется owner-scoped по integration_id (GetTaskByIDAndIntegration,
// НЕ по user_id — на этом пути аутентифицирована машина, а не пользователь,
// см. тот же приём в комментарии к запросу в queries/tasks.sql), что не даёт
// одной машине инжектировать событие в чужую задачу через подделанный
// task_id в конверте.
//
// eventPayload, записываемый в task_events, — это RAW env.Payload конверта
// (уже провалидированный как bus.AgentQuestionPayload здесь), а не повторно
// сериализованная структура: тот же байтовый payload, что реально пришёл от
// агента, без риска расхождения форм при последующем сопоставлении ответа
// пользователя по question_id (см. PostTasksIdAnswer, tasks.go).
//
// Уведомление (тикет 7.1, FR G1): при успешном переходе, если Notifier
// зарегистрирован (см. SetNotifier в server.go), формируется и публикуется
// notify.Notification{Kind: notify.KindAgentQuestion} — не блокирующий
// побочный эффект, ошибка которого только логируется и никак не мешает
// последующей отправке ack агенту (сама доставка уведомления пользователю —
// предмет тикетов 7.2/web WS и 7.3/Telegram, здесь её нет).
func (s *Server) handleAgentQuestion(ctx context.Context, conn *websocket.Conn, integrationID uuid.UUID, env bus.Envelope) {
	if env.TaskID == nil || *env.TaskID == "" {
		s.logError("handleAgentQuestion", errors.New("конверт agent_question без task_id"))
		return
	}
	taskUUID, err := uuid.Parse(*env.TaskID)
	if err != nil {
		s.logError("handleAgentQuestion: разобрать task_id", err)
		return
	}

	var payload bus.AgentQuestionPayload
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		s.logError("handleAgentQuestion: разобрать payload", err)
		return
	}
	if payload.QuestionID == "" {
		s.logError("handleAgentQuestion", errors.New("payload agent_question без question_id"))
		return
	}

	taskID := pgtype.UUID{Bytes: taskUUID, Valid: true}
	taskRow, err := s.queries.GetTaskByIDAndIntegration(ctx, db.GetTaskByIDAndIntegrationParams{
		ID:            taskID,
		IntegrationID: pgtype.UUID{Bytes: integrationID, Valid: true},
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			s.logError("handleAgentQuestion", fmt.Errorf("задача %s не найдена для интеграции %s", taskUUID, integrationID))
			return
		}
		s.logError("GetTaskByIDAndIntegration", err)
		return
	}

	transitioner := s.getTransitioner()
	if transitioner == nil {
		s.logError("handleAgentQuestion", errors.New("transitioner не настроен"))
		return
	}

	if _, _, err := transitioner.TransitionWithEvent(ctx, taskID, task.TriggerAgentQuestion, "agent_question", pgtype.UUID{}, env.Payload); err != nil {
		s.logError("TransitionWithEvent(agent_question)", err)
		return
	}

	if notifier := s.getNotifier(); notifier != nil {
		if err := notifier.Notify(ctx, notify.Notification{
			TaskID:    taskID,
			UserID:    taskRow.UserID,
			Kind:      notify.KindAgentQuestion,
			Payload:   env.Payload,
			CreatedAt: time.Now().UTC(),
		}); err != nil {
			s.logError("Notify(agent_question)", err)
		}
	}

	s.writeMachineAck(ctx, conn, integrationID, env.MessageID, "agent_question")
}

// handleAgentError обрабатывает кадр error (тикет 5.8, FR E1, protocol.md
// §4 «Агент → оркестратор»): переводит связанную задачу running→failed,
// атомарно записывая событие error в task_events
// (task.Transitioner.TransitionWithEvent — единственная точка смены
// tasks.status, тикет 5.2), и, при успехе, отвечает агенту ack-кадром — тот
// же at-least-once принцип, что и у handleAgentQuestion: провал любого шага
// (невалидный payload/task_id, задача не найдена или принадлежит другой
// интеграции, недопустимый переход FSM, transitioner не настроен) молча
// пропускает ack — агент должен повторить попытку сам (durable outbox на
// стороне агента, тикет 3.5), соединение при этом не закрывается и не
// паникует (см. godoc файла).
//
// Обработка СИНХРОННАЯ, без отдельного Redpanda-потребителя — тот же приём,
// что и у остальных обработчиков кадров машины (handleAgentQuestion,
// handleTaskAccepted).
//
// Задача ищется owner-scoped по integration_id (GetTaskByIDAndIntegration, НЕ
// по user_id — на этом пути аутентифицирована машина, а не пользователь), что
// не даёт одной машине завершить чужую задачу подделанным task_id в
// конверте.
//
// eventPayload, записываемый в task_events, — это RAW env.Payload конверта
// (уже провалидированный как bus.ErrorPayload здесь), а не повторно
// сериализованная структура — тот же приём, что и в handleAgentQuestion.
//
// Валидация payload: Message обязателен (пустое значение — невалидный
// error-кадр, аналогично QuestionID у agent_question); Code может быть
// пустым — протокол (protocol.md §4) не делает его обязательным.
func (s *Server) handleAgentError(ctx context.Context, conn *websocket.Conn, integrationID uuid.UUID, env bus.Envelope) {
	if env.TaskID == nil || *env.TaskID == "" {
		s.logError("handleAgentError", errors.New("конверт error без task_id"))
		return
	}
	taskUUID, err := uuid.Parse(*env.TaskID)
	if err != nil {
		s.logError("handleAgentError: разобрать task_id", err)
		return
	}

	var payload bus.ErrorPayload
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		s.logError("handleAgentError: разобрать payload", err)
		return
	}
	if payload.Message == "" {
		s.logError("handleAgentError", errors.New("payload error без message"))
		return
	}

	taskID := pgtype.UUID{Bytes: taskUUID, Valid: true}
	if _, err := s.queries.GetTaskByIDAndIntegration(ctx, db.GetTaskByIDAndIntegrationParams{
		ID:            taskID,
		IntegrationID: pgtype.UUID{Bytes: integrationID, Valid: true},
	}); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			s.logError("handleAgentError", fmt.Errorf("задача %s не найдена для интеграции %s", taskUUID, integrationID))
			return
		}
		s.logError("GetTaskByIDAndIntegration", err)
		return
	}

	transitioner := s.getTransitioner()
	if transitioner == nil {
		s.logError("handleAgentError", errors.New("transitioner не настроен"))
		return
	}

	if _, _, err := transitioner.TransitionWithEvent(ctx, taskID, task.TriggerAgentError, "error", pgtype.UUID{}, env.Payload); err != nil {
		s.logError("TransitionWithEvent(error)", err)
		return
	}

	s.writeMachineAck(ctx, conn, integrationID, env.MessageID, "error")
}

// handleAgentCompleted обрабатывает кадр agent_completed (тикет 8.1, FR E2,
// Gherkin §7 «Агент сообщил о завершении — задача ещё не закрыта»): переводит
// связанную задачу running→awaiting_confirm, атомарно записывая событие
// agent_completed в task_events (task.Transitioner.TransitionWithEvent —
// единственная точка смены tasks.status, тикет 5.2), и, при успехе, отвечает
// агенту ack-кадром — тот же at-least-once принцип, что и у handleAgentError:
// провал любого шага (невалидный payload/task_id, задача не найдена или
// принадлежит другой интеграции, недопустимый переход FSM, transitioner не
// настроен) молча пропускает ack — агент должен повторить попытку сам
// (durable outbox на стороне агента, тикет 3.5), соединение при этом не
// закрывается и не паникует (см. godoc файла).
//
// КРИТИЧНО (FR E2, docs/glossary.md): этот переход НЕ закрывает задачу —
// закрытой задачу делает только явное действие пользователя (тикеты 8.2/8.3,
// /confirm и /reject), которых здесь намеренно нет. awaiting_confirm — это
// промежуточный статус, ожидающий именно такого подтверждения; отчёт агента
// сам по себе не является достаточным основанием считать работу принятой.
//
// Обработка СИНХРОННАЯ, без отдельного Redpanda-потребителя — тот же приём,
// что и у остальных обработчиков кадров машины (handleAgentQuestion,
// handleTaskAccepted, handleAgentError).
//
// Задача ищется owner-scoped по integration_id (GetTaskByIDAndIntegration, НЕ
// по user_id — на этом пути аутентифицирована машина, а не пользователь), что
// не даёт одной машине завершить чужую задачу подделанным task_id в
// конверте.
//
// eventPayload, записываемый в task_events, — это RAW env.Payload конверта
// (уже провалидированный как bus.AgentCompletedPayload здесь), а не повторно
// сериализованная структура — тот же приём, что и в handleAgentQuestion/
// handleAgentError.
//
// Валидация payload: в отличие от handleAgentError (где Message обязателен),
// здесь Summary НЕ обязателен — протокол (protocol.md §4) не требует
// непустой сводки, пустой summary не делает кадр невалидным (агент мог
// просто не дать текстовое резюме завершения).
//
// Уведомление (тикет 9.5, FR E2/G1): при успешном переходе, если Notifier
// зарегистрирован (см. SetNotifier в server.go), формируется и публикуется
// notify.Notification{Kind: notify.KindAgentCompleted} — не блокирующий
// побочный эффект, ошибка которого только логируется и никак не мешает
// последующей отправке ack агенту (тот же приём, что и в
// handleAgentQuestion); сама доставка уведомления пользователю — предмет
// тикета 7.2 (web WS)/7.3 (Telegram), здесь её нет.
func (s *Server) handleAgentCompleted(ctx context.Context, conn *websocket.Conn, integrationID uuid.UUID, env bus.Envelope) {
	if env.TaskID == nil || *env.TaskID == "" {
		s.logError("handleAgentCompleted", errors.New("конверт agent_completed без task_id"))
		return
	}
	taskUUID, err := uuid.Parse(*env.TaskID)
	if err != nil {
		s.logError("handleAgentCompleted: разобрать task_id", err)
		return
	}

	var payload bus.AgentCompletedPayload
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		s.logError("handleAgentCompleted: разобрать payload", err)
		return
	}

	taskID := pgtype.UUID{Bytes: taskUUID, Valid: true}
	taskRow, err := s.queries.GetTaskByIDAndIntegration(ctx, db.GetTaskByIDAndIntegrationParams{
		ID:            taskID,
		IntegrationID: pgtype.UUID{Bytes: integrationID, Valid: true},
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			s.logError("handleAgentCompleted", fmt.Errorf("задача %s не найдена для интеграции %s", taskUUID, integrationID))
			return
		}
		s.logError("GetTaskByIDAndIntegration", err)
		return
	}

	transitioner := s.getTransitioner()
	if transitioner == nil {
		s.logError("handleAgentCompleted", errors.New("transitioner не настроен"))
		return
	}

	if _, _, err := transitioner.TransitionWithEvent(ctx, taskID, task.TriggerAgentCompleted, "agent_completed", pgtype.UUID{}, env.Payload); err != nil {
		s.logError("TransitionWithEvent(agent_completed)", err)
		return
	}

	if notifier := s.getNotifier(); notifier != nil {
		if err := notifier.Notify(ctx, notify.Notification{
			TaskID:    taskID,
			UserID:    taskRow.UserID,
			Kind:      notify.KindAgentCompleted,
			Payload:   env.Payload,
			CreatedAt: time.Now().UTC(),
		}); err != nil {
			s.logError("Notify(agent_completed)", err)
		}
	}

	s.writeMachineAck(ctx, conn, integrationID, env.MessageID, "agent_completed")
}

// handleAgentProgress обрабатывает кадр agent_progress (тикет 8.5, FR E6,
// Gherkin §8 «Приоритет сохранности данных при отмене»): агент сообщает о
// прогрессе/предупреждении (например, что отмена отложена до безопасного
// завершения критической операции, см. claudecode.Provider.warnCancelDeferred)
// — событие пишется в task_events БЕЗ изменения статуса задачи (см.
// task.Transitioner.RecordEvent) — это НЕ переход FSM, просто запись
// истории/аудита (FR H1/F4). Статус задачи не проверяется: событие может
// прийти уже после того, как задача переведена в cancelled (см. годок
// RecordEvent).
func (s *Server) handleAgentProgress(ctx context.Context, conn *websocket.Conn, integrationID uuid.UUID, env bus.Envelope) {
	if env.TaskID == nil || *env.TaskID == "" {
		s.logError("handleAgentProgress", errors.New("конверт agent_progress без task_id"))
		return
	}
	taskUUID, err := uuid.Parse(*env.TaskID)
	if err != nil {
		s.logError("handleAgentProgress: разобрать task_id", err)
		return
	}

	var payload bus.AgentProgressPayload
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		s.logError("handleAgentProgress: разобрать payload", err)
		return
	}

	taskID := pgtype.UUID{Bytes: taskUUID, Valid: true}
	if _, err := s.queries.GetTaskByIDAndIntegration(ctx, db.GetTaskByIDAndIntegrationParams{
		ID:            taskID,
		IntegrationID: pgtype.UUID{Bytes: integrationID, Valid: true},
	}); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			s.logError("handleAgentProgress", fmt.Errorf("задача %s не найдена для интеграции %s", taskUUID, integrationID))
			return
		}
		s.logError("GetTaskByIDAndIntegration", err)
		return
	}

	transitioner := s.getTransitioner()
	if transitioner == nil {
		s.logError("handleAgentProgress", errors.New("transitioner не настроен"))
		return
	}

	if _, err := transitioner.RecordEvent(ctx, taskID, "agent_progress", pgtype.UUID{}, env.Payload); err != nil {
		s.logError("RecordEvent(agent_progress)", err)
		return
	}

	s.writeMachineAck(ctx, conn, integrationID, env.MessageID, "agent_progress")
}

// handleTaskAccepted обрабатывает кадр task_accepted (FR E1, тикет 5.4,
// Gherkin §4 «Доставка на машину»): агент подтверждает, что забрал задачу,
// доставленную мостом (тикет 3.4) через machine.commands (type=task_assigned),
// и это событие переводит задачу queued→running (task.TriggerTaskAccepted,
// см. fsm.go) — единственная точка смены tasks.status (FR E1, тикет 5.2). В
// отличие от handleAgentQuestion у task_accepted нет дополнительного
// бизнес-payload для task_events (сам факт события уже полностью описан
// status_change), поэтому используется простой Transition, а не
// TransitionWithEvent.
//
// При успехе отвечает агенту ack-кадром — тот же at-least-once принцип, что
// и у остальных обработчиков кадров машины: провал любого шага (невалидный
// task_id, задача не найдена или принадлежит другой интеграции, недопустимый
// переход FSM — например, повторная доставка того же task_assigned после
// потери предыдущего ack, из-за чего задача уже не в queued, — или
// transitioner не настроен) молча пропускает ack без какой-либо специальной
// дедупликации сверху: агент сам повторит попытку через свой durable outbox
// (тот же принцип, что и в handleAgentQuestion), соединение при этом не
// закрывается и не паникует (см. godoc файла).
//
// Задача ищется owner-scoped по integration_id (GetTaskByIDAndIntegration, НЕ
// по user_id — на этом пути аутентифицирована машина, а не пользователь),
// что не даёт одной машине подтвердить приём чужой задачи через подделанный
// task_id в конверте.
func (s *Server) handleTaskAccepted(ctx context.Context, conn *websocket.Conn, integrationID uuid.UUID, env bus.Envelope) {
	if env.TaskID == nil || *env.TaskID == "" {
		s.logError("handleTaskAccepted", errors.New("конверт task_accepted без task_id"))
		return
	}
	taskUUID, err := uuid.Parse(*env.TaskID)
	if err != nil {
		s.logError("handleTaskAccepted: разобрать task_id", err)
		return
	}

	taskID := pgtype.UUID{Bytes: taskUUID, Valid: true}
	if _, err := s.queries.GetTaskByIDAndIntegration(ctx, db.GetTaskByIDAndIntegrationParams{
		ID:            taskID,
		IntegrationID: pgtype.UUID{Bytes: integrationID, Valid: true},
	}); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			s.logError("handleTaskAccepted", fmt.Errorf("задача %s не найдена для интеграции %s", taskUUID, integrationID))
			return
		}
		s.logError("GetTaskByIDAndIntegration", err)
		return
	}

	transitioner := s.getTransitioner()
	if transitioner == nil {
		s.logError("handleTaskAccepted", errors.New("transitioner не настроен"))
		return
	}

	if _, _, err := transitioner.Transition(ctx, taskID, task.TriggerTaskAccepted); err != nil {
		s.logError("Transition(task_accepted)", err)
		return
	}

	s.writeMachineAck(ctx, conn, integrationID, env.MessageID, "task_accepted")
}

// handleCommandApprovalRequest обрабатывает кадр command_approval_request (FR
// F3, тикет 6.4, Gherkin §5 «Команда вне allowlist требует согласования»):
// агент обнаружил, что команда, которую он хочет выполнить, не входит в
// allowlist (тикет 6.3), и просит владельца согласовать её выполнение —
// переводит связанную задачу running→waiting_user, атомарно записывая
// событие command_approval_request в task_events
// (task.Transitioner.TransitionWithEvent — единственная точка смены
// tasks.status, тикет 5.2), и, при успехе, отвечает агенту ack-кадром — тот
// же at-least-once принцип, что и у handleAgentQuestion/handleAgentError:
// провал любого шага (невалидный payload/task_id, задача не найдена или
// принадлежит другой интеграции, недопустимый переход FSM, transitioner не
// настроен) молча пропускает ack — агент должен повторить попытку сам
// (durable outbox на стороне агента, тикет 3.5), соединение при этом не
// закрывается и не паникует (см. godoc файла). Пока задача не покинула
// waiting_user (т.е. пока владелец не вызвал POST /tasks/{id}/approve, см.
// PostTasksIdApprove в tasks.go), сама команда НЕ выполняется — это и есть
// приёмочное требование тикета 6.4: «команда не выполняется, пока я её не
// одобрю».
//
// Обработка СИНХРОННАЯ, без отдельного Redpanda-потребителя — тот же приём,
// что и у остальных обработчиков кадров машины (handleAgentQuestion,
// handleTaskAccepted, handleAgentError).
//
// Задача ищется owner-scoped по integration_id (GetTaskByIDAndIntegration, НЕ
// по user_id — на этом пути аутентифицирована машина, а не пользователь), что
// не даёт одной машине запросить согласование для чужой задачи через
// подделанный task_id в конверте.
//
// eventPayload, записываемый в task_events, — это RAW env.Payload конверта
// (уже провалидированный как bus.CommandApprovalRequestPayload здесь), а не
// повторно сериализованная структура: тот же байтовый payload, что реально
// пришёл от агента, без риска расхождения форм при последующем сопоставлении
// решения пользователя по request_id (см. PostTasksIdApprove, tasks.go,
// ListCommandApprovalRequestEventsByTask, queries/tasks.sql) — тот же приём,
// что и у handleAgentQuestion с question_id.
//
// Валидация payload: RequestID обязателен (пустое значение — невалидный
// кадр, аналогично QuestionID у agent_question). Command/Reason не
// обязательны к валидации — протокол (bus.CommandApprovalRequestPayload) не
// делает их обязательными на этом пути; они лишь описательные для
// пользователя, принимающего решение.
//
// Уведомление (тикет 9.5, FR F1/G1): при успешном переходе, если Notifier
// зарегистрирован (см. SetNotifier в server.go), формируется и публикуется
// notify.Notification{Kind: notify.KindCommandApprovalRequest} — не
// блокирующий побочный эффект, ошибка которого только логируется и никак не
// мешает последующей отправке ack агенту (тот же приём, что и в
// handleAgentQuestion); сама доставка уведомления пользователю — предмет
// тикета 7.2 (web WS)/7.3 (Telegram), здесь её нет.
func (s *Server) handleCommandApprovalRequest(ctx context.Context, conn *websocket.Conn, integrationID uuid.UUID, env bus.Envelope) {
	if env.TaskID == nil || *env.TaskID == "" {
		s.logError("handleCommandApprovalRequest", errors.New("конверт command_approval_request без task_id"))
		return
	}
	taskUUID, err := uuid.Parse(*env.TaskID)
	if err != nil {
		s.logError("handleCommandApprovalRequest: разобрать task_id", err)
		return
	}

	var payload bus.CommandApprovalRequestPayload
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		s.logError("handleCommandApprovalRequest: разобрать payload", err)
		return
	}
	if payload.RequestID == "" {
		s.logError("handleCommandApprovalRequest", errors.New("payload command_approval_request без request_id"))
		return
	}

	taskID := pgtype.UUID{Bytes: taskUUID, Valid: true}
	taskRow, err := s.queries.GetTaskByIDAndIntegration(ctx, db.GetTaskByIDAndIntegrationParams{
		ID:            taskID,
		IntegrationID: pgtype.UUID{Bytes: integrationID, Valid: true},
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			s.logError("handleCommandApprovalRequest", fmt.Errorf("задача %s не найдена для интеграции %s", taskUUID, integrationID))
			return
		}
		s.logError("GetTaskByIDAndIntegration", err)
		return
	}

	transitioner := s.getTransitioner()
	if transitioner == nil {
		s.logError("handleCommandApprovalRequest", errors.New("transitioner не настроен"))
		return
	}

	if _, _, err := transitioner.TransitionWithEvent(ctx, taskID, task.TriggerApprovalRequested, "command_approval_request", pgtype.UUID{}, env.Payload); err != nil {
		s.logError("TransitionWithEvent(command_approval_request)", err)
		return
	}

	if notifier := s.getNotifier(); notifier != nil {
		if err := notifier.Notify(ctx, notify.Notification{
			TaskID:    taskID,
			UserID:    taskRow.UserID,
			Kind:      notify.KindCommandApprovalRequest,
			Payload:   env.Payload,
			CreatedAt: time.Now().UTC(),
		}); err != nil {
			s.logError("Notify(command_approval_request)", err)
		}
	}

	s.writeMachineAck(ctx, conn, integrationID, env.MessageID, "command_approval_request")
}

// writeMachineAck строит и отправляет ack-конверт (protocol.md §4/§5,
// bus.AckPayload) в ответ на успешно обработанный кадр машины (heartbeat —
// тикет 3.6, agent_question — тикет 6.1) — общая логика, вынесенная из
// handleMachineEvent, чтобы не дублировать построение/маршалинг/таймаут
// записи между обработчиками. logContext используется только в сообщении
// лога при ошибке (различить источник в логах), на поведение не влияет.
func (s *Server) writeMachineAck(ctx context.Context, conn *websocket.Conn, integrationID uuid.UUID, ackMessageID, logContext string) {
	ack := bus.Envelope{
		MessageID:       bus.NewMessageID(),
		IntegrationID:   integrationID.String(),
		Type:            bus.MessageTypeAck,
		Ts:              time.Now().UTC().Format(time.RFC3339),
		ProtocolVersion: bus.ProtocolVersion,
	}
	payload, err := json.Marshal(bus.AckPayload{AckMessageID: ackMessageID})
	if err != nil {
		s.logError("marshal AckPayload для "+logContext, err)
		return
	}
	ack.Payload = payload

	data, err := ack.Marshal()
	if err != nil {
		s.logError("marshal ack-конверта для "+logContext, err)
		return
	}

	writeCtx, cancel := context.WithTimeout(ctx, machineEventAckWriteTimeout)
	defer cancel()
	if err := conn.Write(writeCtx, websocket.MessageText, data); err != nil {
		s.logError("запись ack-кадра "+logContext+" в WS", err)
	}
}

// parseAckFrame пытается разобрать сырой WS-кадр как конверт type==ack
// (protocol.md §4/§5, bus.AckPayload). Возвращает (ack_message_id, true) при
// успехе; ("", false) для ЛЮБОГО иного случая — кадр не JSON, конверт другого
// типа, payload без ack_message_id и т.п. Причина не различается специально:
// вызывающий (handleMachineFrame) одинаково игнорирует кадр в любом из этих
// случаев.
func parseAckFrame(data []byte) (string, bool) {
	var env bus.Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return "", false
	}
	if env.Type != bus.MessageTypeAck {
		return "", false
	}
	var payload bus.AckPayload
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		return "", false
	}
	if payload.AckMessageID == "" {
		return "", false
	}
	return payload.AckMessageID, true
}

// authenticateMachineHello читает первый WS-кадр и проверяет его как hello
// (FR B3, B6): сначала — совместимость protocol_version (тикет 4.7, ADR 0003,
// см. godoc файла), ДО обращения к БД; затем UUID находится по HMAC-отпечатку
// через GetIntegrationByUUIDHMAC, затем (если у найденной интеграции непустой
// ip_hint) сверяется IP TCP-пира. При успехе возвращает (integration_id,
// true); при отказе по любой причине — (uuid.Nil, false).
//
// ВАЖНО: при отказе по несовместимому protocol_version эта функция САМА
// закрывает conn кодом wsCloseIncompatibleProtocolVersion (содержательная
// причина, см. ниже) перед возвратом false — в отличие от остальных веток
// отказа здесь, которые лишь логируют причину и возвращают false, оставляя
// закрытие соединения кодом wsCloseUnauthorized вызывающему (GetMachineWs, см.
// godoc файла про единый внешний сигнал auth-провалов). Повторный вызов
// conn.Close вызывающим для этой ветки безопасен и no-op (coder/websocket
// идемпотентен: первый Close фиксирует код/reason, последующие возвращают
// net.ErrClosed, который вызывающий уже игнорирует через `_ = conn.Close(...)`).
func (s *Server) authenticateMachineHello(r *http.Request, conn *websocket.Conn) (uuid.UUID, bool) {
	ctx, cancel := context.WithTimeout(r.Context(), machineHelloReadTimeout)
	defer cancel()

	_, data, err := conn.Read(ctx)
	if err != nil {
		s.logMachineAuthRejected("чтение первого кадра", "error", err)
		return uuid.Nil, false
	}

	var env bus.Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		s.logMachineAuthRejected("первый кадр не является валидным JSON-конвертом", "error", err)
		return uuid.Nil, false
	}
	if env.Type != bus.MessageTypeHello {
		s.logMachineAuthRejected("первый кадр не hello", "type", env.Type)
		return uuid.Nil, false
	}

	// Совместимость версий (тикет 4.7, FR C5, ADR 0003): сверяем
	// protocol_version ДО похода в БД за интеграцией (GetIntegrationByUUIDHMAC
	// ниже) — проверка дешёвая (уже распарсенное строковое поле конверта) и не
	// секретно-чувствительна (в отличие от результатов auth), поэтому и
	// закрывается ОТДЕЛЬНЫМ содержательным close-кодом/reason, а не единым
	// wsCloseUnauthorized (см. godoc wsCloseIncompatibleProtocolVersion). Точное
	// строковое сравнение — semver/диапазонная совместимость не входит в объём
	// этого тикета (см. ADR 0003, "Альтернативы"): agent_version НЕ проверяется
	// здесь и нигде далее, только логируется для диагностики (см. ниже).
	if env.ProtocolVersion != bus.ProtocolVersion {
		reason := fmt.Sprintf("incompatible protocol_version: got %q, want %q", env.ProtocolVersion, bus.ProtocolVersion)
		s.logMachineAuthRejected("несовместимый protocol_version", "got", env.ProtocolVersion, "want", bus.ProtocolVersion)
		_ = conn.Close(wsCloseIncompatibleProtocolVersion, reason)
		return uuid.Nil, false
	}

	var payload bus.HelloPayload
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		s.logMachineAuthRejected("payload hello не парсится", "error", err)
		return uuid.Nil, false
	}

	secret, err := uuid.Parse(payload.UUID)
	if err != nil {
		s.logMachineAuthRejected("uuid в hello не парсится", "error", err)
		return uuid.Nil, false
	}

	// Поиск интеграции по HMAC-отпечатку UUID-секрета — НЕ по расшифровке
	// uuid_enc: тот же ключ/примитив, что и при создании (PostIntegrations,
	// integrations.go), без фильтра по user_id (владелец на этом шаге ещё не
	// известен — поиск его и устанавливает, см. godoc Querier.GetIntegrationByUUIDHMAC).
	secretBytes := secret[:]
	uuidHMAC := hex.EncodeToString(crypto.HMACSHA256(s.integrationUUIDHMACKey, secretBytes))
	integration, err := s.queries.GetIntegrationByUUIDHMAC(ctx, uuidHMAC)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			s.logMachineAuthRejected("uuid не найден по hmac")
			return uuid.Nil, false
		}
		s.logError("GetIntegrationByUUIDHMAC", err)
		return uuid.Nil, false
	}

	if !machineIPHintMatches(integration.IpHint, r) {
		s.logMachineAuthRejected("ip_hint не совпал", "integration_id", integration.ID.String())
		return uuid.Nil, false
	}

	if s.logger != nil {
		// agent_version логируется ТОЛЬКО для диагностики (тикет 4.7, ADR 0003)
		// — в отличие от protocol_version выше, это поле НЕ проверяется и не
		// может стать причиной отказа (нет механизма MinAgentVersion, см. ADR).
		s.logger.Info("WS-аутентификация машины успешна",
			slog.String("integration_id", integration.ID.String()),
			slog.String("agent_version", payload.AgentVersion))
	}

	return uuid.UUID(integration.ID.Bytes), true
}

// machineIPHintMatches проверяет необязательную доп. проверку IP (FR B3,
// §2 «IP — необязательная проверка»): пустой/отсутствующий ipHint пропускает
// любой IP; непустой должен точно совпасть с IP TCP-пира запроса (см. godoc
// файла — почему именно RemoteAddr, не X-Forwarded-For).
func machineIPHintMatches(ipHint *string, r *http.Request) bool {
	if ipHint == nil || *ipHint == "" {
		return true
	}
	return *ipHint == clientIP(r)
}

// clientIP возвращает IP прямого TCP-пира запроса (net.SplitHostPort от
// r.RemoteAddr). Если разбор не удался (нестандартный формат RemoteAddr —
// на практике не встречается за net/http), возвращает RemoteAddr как есть:
// это всё равно не совпадёт ни с одним валидным ip_hint, то есть фейлит
// проверку безопасно (fail-closed), а не паникует/игнорирует её.
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// registerMachineConn регистрирует conn как единственное активное
// WS-соединение интеграции integrationID, вытесняя предыдущее, если оно было
// (тикет 2.4, FR B6, ADR 0002, см. godoc файла "Защита от повторного UUID").
//
// Замена записи реестра происходит под s.machineConnsMu, но закрытие старого
// соединения — НАМЕРЕННО вне критической секции: conn.Close выполняет полный
// close-handshake (сетевой I/O), и держать на нём мьютекс заблокировало бы
// регистрацию/снятие с регистрации других, никак не связанных интеграций.
func (s *Server) registerMachineConn(integrationID uuid.UUID, conn *websocket.Conn) {
	s.machineConnsMu.Lock()
	old, existed := s.machineConns[integrationID]
	s.machineConns[integrationID] = conn
	s.machineConnsMu.Unlock()

	if !existed {
		// Действительно новое активное соединение (не вытеснение) — ровно
		// здесь gauge растёт (тикет 11.4). Симметрично unregisterMachineConn
		// ниже: при вытеснении (existed==true) старое соединение закрывается,
		// но НЕ уменьшает gauge само по себе (его собственный defer
		// unregisterMachineConn не пройдёт compare-and-delete — карта уже
		// указывает на новый conn), поэтому здесь для этого пути gauge
		// намеренно НЕ увеличивается: соединение было и остаётся одно.
		metrics.MachineWSConnectionsActive.Inc()
		return
	}

	if s.logger != nil {
		// integration_id логируем для диагностики; сам UUID-секрет здесь не
		// фигурирует (аутентификация уже состоялась по HMAC) и специально не
		// извлекается (см. godoc файла).
		s.logger.Warn("WS-соединение машины вытеснено новым подключением той же интеграции",
			slog.String("integration_id", integrationID.String()))
	}
	_ = old.Close(wsCloseSuperseded, "superseded by newer connection")
}

// unregisterMachineConn снимает conn с регистрации интеграции integrationID
// ПРИ ДИСКОННЕКТЕ (вызывается из defer GetMachineWs) — НО только если запись
// реестра всё ещё указывает именно на conn (compare-and-delete).
//
// Без этой проверки возможна гонка session-takeover: новое соединение той же
// интеграции успело зарегистрироваться (registerMachineConn) раньше, чем
// старое соединение дошло до своего defer здесь — наивное безусловное
// delete(s.machineConns, integrationID) стёрло бы из реестра уже актуальное
// НОВОЕ соединение, оставив интеграцию без маршрутизируемой цели для команд
// (тикет 3.4) при формально живом новом WS.
func (s *Server) unregisterMachineConn(integrationID uuid.UUID, conn *websocket.Conn) {
	s.machineConnsMu.Lock()
	defer s.machineConnsMu.Unlock()
	if s.machineConns[integrationID] == conn {
		delete(s.machineConns, integrationID)
		// Симметрично инкременту в registerMachineConn (тикет 11.4): только
		// РЕАЛЬНОЕ снятие с регистрации (compare-and-delete совпал — conn всё
		// ещё актуален) уменьшает gauge. Отменённое из-за session-takeover
		// снятие (см. годок функции выше) до этой строки не доходит.
		metrics.MachineWSConnectionsActive.Dec()
	}
}

// logMachineAuthRejected пишет причину отказа WS-аутентификации машины в лог
// сервиса (если логгер задан, см. NewServer) — ТОЛЬКО для диагностики на
// стороне оркестратора. Причина НИКОГДА не уходит клиенту: GetMachineWs
// закрывает соединение одним и тем же кодом/reason независимо от того, какая
// именно из веток отказа сработала (см. godoc файла про единый внешний
// сигнал).
func (s *Server) logMachineAuthRejected(reason string, args ...any) {
	if s.logger == nil {
		return
	}
	s.logger.Warn("WS-аутентификация машины отклонена", append([]any{slog.String("reason", reason)}, args...)...)
}
