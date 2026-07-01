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
// (тикеты 3.4/3.6/6.1) разбирает каждый дальнейший кадр (см.
// handleMachineFrame/parseAckFrame/handleMachineEvent/handleAgentQuestion) и
// активно обрабатывает три типа: type==ack — пересылается зарегистрированному
// s.ackSink (мосту оркестратора machine.commands → WS, commit-after-ACK,
// protocol.md §5, см. godoc AckSink в server.go); type==heartbeat —
// публикуется через зарегистрированный s.eventSink (presence-подсистема,
// FR B4, protocol.md §6, см. godoc EventSink в server.go) с ПЕРЕЗАПИСАННЫМ на
// аутентифицированный DB id полем IntegrationID, после чего агенту
// отправляется ack; type==agent_question (FR F1, тикет 6.1) — переводит
// задачу running→waiting_user через taskTransitioner.TransitionWithEvent
// СИНХРОННО (без отдельного Redpanda-потребителя, тот же приём, что и у
// PostTasks/тикет 5.3), см. handleAgentQuestion. Любой другой тип кадра
// (task_accepted/command_approval_request/... — тикеты 3.5/5.x) и любой
// нераспознанный/битый кадр МОЛЧА игнорируются — ни паники, ни закрытия
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
	// 3.4/3.6/6.1): разбираем каждый дальнейший кадр и маршрутизируем по типу
	// (см. handleMachineFrame) — ack пересылается мосту оркестратора
	// (s.ackSink), heartbeat публикуется presence-подсистеме (s.eventSink, см.
	// handleMachineEvent), agent_question переводит задачу в waiting_user (см.
	// handleAgentQuestion); любой иной тип кадра (а также нераспознанный/битый
	// JSON) МОЛЧА игнорируется — обработка прочих типов
	// (task_accepted/command_approval_request/... — тикеты 3.5/5.x) вне
	// объёма этого тикета, но получение такого кадра не должно ронять или
	// закрывать соединение (см. godoc файла).
	for {
		_, data, err := conn.Read(r.Context())
		if err != nil {
			return
		}
		s.handleMachineFrame(r.Context(), conn, integrationID, data)
	}
}

// handleMachineFrame разбирает один кадр, полученный ПОСЛЕ успешного hello
// (тикеты 3.4/3.6/6.1, protocol.md §5/§6). Кадр разбирается как конверт ОДИН
// раз и маршрутизируется по env.Type:
//   - type==ack — commit-after-ack для machine.commands, пересылается
//     s.ackSink (см. godoc AckSink в server.go);
//   - type==heartbeat — событие машины (FR B4, protocol.md §6), см.
//     handleMachineEvent;
//   - type==agent_question — вопрос агента пользователю (FR F1, тикет 6.1),
//     см. handleAgentQuestion;
//   - любой другой тип, а также нераспознанный/битый кадр — безопасно
//     игнорируется, без побочных эффектов (ни паники, ни закрытия
//     соединения): обработка прочих типов кадров
//     (task_accepted/... — тикеты 3.5/5.x) не должна блокироваться/ломаться
//     из-за их временного отсутствия здесь.
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
	default:
		// Прочие типы событий (тикеты 3.5/5.x) — вне объёма, молча игнорируем.
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
	if _, err := s.queries.GetTaskByIDAndIntegration(ctx, db.GetTaskByIDAndIntegrationParams{
		ID:            taskID,
		IntegrationID: pgtype.UUID{Bytes: integrationID, Valid: true},
	}); err != nil {
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

	s.writeMachineAck(ctx, conn, integrationID, env.MessageID, "agent_question")
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
// (FR B3, B6): UUID находится по HMAC-отпечатку через
// GetIntegrationByUUIDHMAC, затем (если у найденной интеграции непустой
// ip_hint) сверяется IP TCP-пира. При успехе возвращает (integration_id,
// true); при отказе по любой причине — (uuid.Nil, false) (см. godoc файла
// про единый внешний сигнал).
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
