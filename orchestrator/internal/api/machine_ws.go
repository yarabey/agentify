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
// "unauthorized". При успехе соединение остаётся открытым; обработка
// payload-ов команд/событий, ack/offset-семантика и т.п. — вне объёма этого
// тикета (тикет 3.3 "WS-транспорт"), здесь после успешного hello — только
// минимальный read-loop, блокирующийся на чтении кадров до дисконнекта
// клиента, ничего с ними не делая (нужен, чтобы коннект не выглядел повисшим
// и control-фреймы coder/websocket обрабатывались штатно — см. godoc
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
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/yarabey/agentify/internal/crypto"
)

// wsCloseUnauthorized — close-код, которым GetMachineWs закрывает
// WS-соединение при провале аутентификации машины (см. godoc файла).
// 4401 выбран из приватного диапазона 4000-4999 (RFC 6455 §7.4.2,
// "Reserved for private use") как мнемоника к HTTP 401 Unauthorized из
// контракта — не зарезервирован спецификацией/библиотекой и однозначно
// отличим от стандартных кодов закрытия (1000-1015).
const wsCloseUnauthorized websocket.StatusCode = 4401

// machineHelloReadTimeout — сколько GetMachineWs ждёт первый кадр (hello)
// после успешного WS-апгрейда, прежде чем считать аутентификацию
// провалившейся. Конкретное значение не зафиксировано протоколом/тикетом —
// 10s достаточно агенту собрать и отправить hello сразу после установления
// соединения, не давая медленному/зависшему клиенту держать handshake
// бесконечно.
const machineHelloReadTimeout = 10 * time.Second

// wsEnvelope — JSON-конверт сообщения протокола машина↔оркестратор
// (docs/protocol.md §2). Здесь используется только для разбора самого
// первого кадра (hello) на WS-handshake; остальные поля, кроме Type и
// Payload, тикетом 2.3 не используются (полноценная обработка конверта —
// тикет 3.3), но включены для полноты разбора и будущего переиспользования.
type wsEnvelope struct {
	MessageID       string          `json:"message_id"`
	TaskID          *string         `json:"task_id"`
	IntegrationID   string          `json:"integration_id"`
	Type            string          `json:"type"`
	Seq             int64           `json:"seq"`
	Ts              string          `json:"ts"`
	ProtocolVersion string          `json:"protocol_version"`
	Payload         json.RawMessage `json:"payload"`
}

// helloMessageType — значение поля Type конверта, которым агент
// аутентифицируется сразу после WS-коннекта (docs/protocol.md §4).
const helloMessageType = "hello"

// helloPayload — payload сообщения type=="hello" (docs/protocol.md §4):
// {uuid, agent_version, providers[]}. agent_version/Providers тикетом 2.3 не
// проверяются (сверка версии протокола/агента — FR C5, тикет 4.7), но
// разбираются, чтобы провалидировать сам факт корректного hello-кадра.
type helloPayload struct {
	UUID         string   `json:"uuid"`
	AgentVersion string   `json:"agent_version"`
	Providers    []string `json:"providers"`
}

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

	if !s.authenticateMachineHello(r, conn) {
		_ = conn.Close(wsCloseUnauthorized, "unauthorized")
		return
	}

	// Успешный hello: соединение остаётся открытым. Полноценная обработка
	// дальнейших кадров (machine.commands/events, ack) — тикет 3.3; здесь —
	// минимальный read-loop до дисконнекта клиента (см. godoc файла).
	for {
		if _, _, err := conn.Read(r.Context()); err != nil {
			return
		}
	}
}

// authenticateMachineHello читает первый WS-кадр и проверяет его как hello
// (FR B3, B6): UUID находится по HMAC-отпечатку через
// GetIntegrationByUUIDHMAC, затем (если у найденной интеграции непустой
// ip_hint) сверяется IP TCP-пира. true — машина опознана, false — отказ по
// любой причине (см. godoc файла про единый внешний сигнал).
func (s *Server) authenticateMachineHello(r *http.Request, conn *websocket.Conn) bool {
	ctx, cancel := context.WithTimeout(r.Context(), machineHelloReadTimeout)
	defer cancel()

	_, data, err := conn.Read(ctx)
	if err != nil {
		s.logMachineAuthRejected("чтение первого кадра", "error", err)
		return false
	}

	var env wsEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		s.logMachineAuthRejected("первый кадр не является валидным JSON-конвертом", "error", err)
		return false
	}
	if env.Type != helloMessageType {
		s.logMachineAuthRejected("первый кадр не hello", "type", env.Type)
		return false
	}

	var payload helloPayload
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		s.logMachineAuthRejected("payload hello не парсится", "error", err)
		return false
	}

	secret, err := uuid.Parse(payload.UUID)
	if err != nil {
		s.logMachineAuthRejected("uuid в hello не парсится", "error", err)
		return false
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
			return false
		}
		s.logError("GetIntegrationByUUIDHMAC", err)
		return false
	}

	if !machineIPHintMatches(integration.IpHint, r) {
		s.logMachineAuthRejected("ip_hint не совпал", "integration_id", integration.ID.String())
		return false
	}

	return true
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
