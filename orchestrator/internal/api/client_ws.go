package api

// client_ws.go — WS-подключение браузера с аутентификацией по access-токену
// (тикет 7.2, FR G1, §6 «Уведомление в web по WebSocket», зависит от 7.1
// (orchestrator/internal/notify) и 9.1 (web/src/hooks/useWebSocket.ts)).
//
// Назначение (бизнес): пока вкладка web открыта, она должна узнавать о
// доменных событиях уведомления (сейчас — agent_question, тикет 6.1; в
// будущем — answer_reminder, тикет 6.7) БЕЗ перезагрузки страницы (Gherkin
// «Уведомление в web по WebSocket»: «Дано у меня открыт веб-интерфейс / Когда
// агент задаёт вопрос по моей задаче / Тогда я вижу уведомление в web без
// перезагрузки страницы»). handleAgentQuestion (machine_ws.go, тикет 7.1) уже
// формирует notify.Notification и передаёт его зарегистрированному
// api.Notifier — этот файл реализует РЕАЛЬНЫЙ канал доставки для web
// (Telegram — отдельный, ещё не реализованный тикет 7.3, канал не
// пересекается с этим файлом).
//
// Как устроено (тех) — auth ПОСЛЕ WS-апгрейда: контракт (api/openapi.yaml,
// /ws) формально перечисляет ответы "101"/"401" — то же документальное
// упрощение, что и у /machine/ws (см. godoc machine_ws.go): OpenAPI не умеет
// нормально моделировать WS-handshake. Причина, по которой аутентификация
// происходит ПЕРВЫМ КАДРОМ ПОСЛЕ апгрейда, а не заголовком Authorization на
// самом HTTP-запросе апгрейда, — та же архитектурная причина, что и у
// /machine/ws (нельзя передумать и ответить 401 поверх уже отправленного 101
// Switching Protocols), ПЛЮС дополнительное ограничение, которого нет у
// агента: браузерный конструктор `new WebSocket(url)` (web/src/hooks/
// useWebSocket.ts, тикет 9.1) физически не умеет выставлять произвольные
// заголовки запроса (в т.ч. Authorization) — это ограничение самого
// WHATWG-стандарта WebSocket API, не выбор реализации. Поэтому GetWs, как и
// GetMachineWs, ВСЕГДА выполняет апгрейд (websocket.Accept), затем читает
// первый кадр браузера и ожидает простой JSON {"type":"auth",
// "access_token":"<access JWT>"} — НЕ конверт машинного протокола
// bus.Envelope (docs/protocol.md), это другой протокол для другого клиента.
// Токен проверяется ТЕМ ЖЕ вызовом, что и authMiddleware для обычных REST-
// запросов (auth.ParseAccessToken(token, s.jwtSigningKey), см.
// middleware.go) — единственная точка проверки access-JWT в проекте не
// дублируется. Провал любой из проверок (первый кадр не пришёл за
// clientAuthReadTimeout, не JSON, type != "auth", пустой/битый/просроченный
// access_token) — функциональный эквивалент "401" из контракта: закрытие
// WS-соединения ТЕМ ЖЕ кодом wsCloseUnauthorized (4401) и той же причиной
// "unauthorized", что уже объявлены в machine_ws.go, — переиспользуется
// напрямую, не дублируется: это один и тот же внешний сигнал "аутентификация
// на WS-handshake не удалась", независимо от того, машина это или браузер.
//
// Реестр соединений и рассылка уведомлений — ClientConnHub, ниже в этом же
// файле (логично рядом с GetWs: оба используют один реестр). Ключевое
// архитектурное отличие от Server.machineConns/ADR 0002: там ключ —
// integration_id (одна машина, не более ОДНОГО активного WS, новый вытесняет
// старый), здесь ключ — user_id (один ЧЕЛОВЕК), и человек рутинно держит
// НЕСКОЛЬКО одновременно открытых вкладок браузера — все они должны получать
// уведомление, ни одна не должна быть вытеснена открытием другой. Это
// сознательное решение, а не упрощение, обосновано отдельным
// docs/adr/0005-client-ws-multi-connection-per-user.md — см. его до
// изменения структуры реестра ниже.
//
// После аутентификации канал строго ОДНОСТОРОННИЙ (сервер → браузер):
// GetWs всё равно должен ЧИТАТЬ из conn в цикле (см. godoc
// websocket.Conn "You must always read from the connection" — иначе control-
// кадры вроде ping/pong/close библиотека не обработает и клиентский
// дисконнект не будет обнаружен), но содержимое прочитанного кадра
// целенаправленно ИГНОРИРУЕТСЯ: браузеру в этом тикете нечего сказать
// серверу (никакого "живого диалога" — это отдельный будущий тикет 9.5).
// Ошибка чтения (в т.ч. закрытие клиентом вкладки) завершает read-loop и, тем
// самым, снимает соединение с регистрации через defer.
import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"

	"github.com/yarabey/agentify/internal/auth"
	"github.com/yarabey/agentify/orchestrator/internal/metrics"
	"github.com/yarabey/agentify/orchestrator/internal/notify"
)

// clientAuthReadTimeout — сколько GetWs ждёт первый кадр (auth) после
// успешного WS-апгрейда, прежде чем считать аутентификацию провалившейся.
// Меньше machineHelloReadTimeout (10s, machine_ws.go): браузер, в отличие от
// агента, отправляет auth-кадр сразу же в обработчике "соединение открыто"
// (см. web/src/context/NotificationsContext.tsx) без какой-либо
// сопутствующей работы (сбора hello-полей и т.п.) — 5s с большим запасом
// достаточно на сетевую задержку одного короткого кадра, не давая
// медленному/зависшему клиенту держать handshake значимо дольше.
const clientAuthReadTimeout = 5 * time.Second

// clientNotifyWriteTimeout — сколько ClientConnHub.Notify ждёт запись одного
// кадра уведомления в одно клиентское соединение, прежде чем считать эту
// конкретную запись провалившейся и перейти к следующему соединению. По
// аналогии с machineEventAckWriteTimeout (machine_ws.go) — разумный таймаут
// на одну сетевую операцию записи.
const clientNotifyWriteTimeout = 10 * time.Second

// clientAuthFrame — ожидаемый ПЕРВЫЙ кадр браузера после WS-апгрейда (см.
// godoc файла). Простой плоский JSON, специально НЕ конверт bus.Envelope
// машинного протокола — у клиентского WS свой, минимальный формат.
type clientAuthFrame struct {
	Type        string `json:"type"`
	AccessToken string `json:"access_token"`
}

// clientNotificationFrame — минимальный wire-формат уведомления, который
// ClientConnHub.Notify пишет в клиентское WS-соединение (FR G1). Намеренно
// НЕ содержит n.Payload целиком (сырой payload agent_question/будущих видов
// уведомлений) — полный контекст вопроса/диалога это предмет отдельного
// будущего тикета 9.5 «Живой диалог», а не простого уведомления «что-то
// произошло по задаче X, зайди посмотреть».
type clientNotificationFrame struct {
	Kind      string `json:"kind"`
	TaskID    string `json:"task_id"`
	CreatedAt string `json:"created_at"`
}

// ClientConnHub — реестр активных WS-соединений браузера по user_id (тикет
// 7.2, FR G1) и одновременно реализация api.Notifier для web-канала
// доставки уведомлений (тикет 7.1).
//
// В отличие от Server.machineConns (ADR 0002, ОДНО соединение на
// integration_id) реестр здесь допускает МНОЖЕСТВО одновременных соединений
// на один user_id — см. подробное обоснование в godoc файла и
// docs/adr/0005-client-ws-multi-connection-per-user.md. Один процесс
// оркестратора в MVP (docs/01_tech_stack_and_architecture.md [РЕШЕНИЕ 5]) —
// in-memory реестр под мьютексом одного процесса достаточен, распределённой
// координации не требуется (тот же принцип, что и у machineConns).
//
// Структурно реализует и api.Notifier (см. Notify ниже, для
// Server.SetNotifier), и узкий интерфейс answerNotifier пакета task (тикет
// 6.7, orchestrator/internal/task/answer_timeout_worker.go) — обе сигнатуры
// метода Notify идентичны, поэтому один и тот же *ClientConnHub можно
// передать в обе точки регистрации без адаптера (тот же структурный приём,
// что и у AckSink/EventSink/Notifier в server.go).
type ClientConnHub struct {
	logger *slog.Logger

	mu    sync.Mutex
	conns map[uuid.UUID]map[*websocket.Conn]struct{}
}

// NewClientConnHub создаёт пустой реестр клиентских WS-соединений. logger
// может быть nil (тогда ошибки записи уведомления в отдельное соединение
// просто не логируются — сам факт недоставленности не критичен, см. godoc
// Notify).
func NewClientConnHub(logger *slog.Logger) *ClientConnHub {
	return &ClientConnHub{
		logger: logger,
		conns:  make(map[uuid.UUID]map[*websocket.Conn]struct{}),
	}
}

// register добавляет conn в набор активных соединений userID, НЕ вытесняя
// уже имеющиеся (в отличие от registerMachineConn/ADR 0002 — см. godoc типа).
func (h *ClientConnHub) register(userID uuid.UUID, conn *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()

	set, ok := h.conns[userID]
	if !ok {
		set = make(map[*websocket.Conn]struct{})
		h.conns[userID] = set
	}
	set[conn] = struct{}{}
	// gauge активных web-соединений (тикет 11.4) — каждый register это ВСЕГДА
	// новая вкладка/соединение (conn — уникальный указатель на новое
	// websocket.Conn, в отличие от registerMachineConn здесь нет вытеснения,
	// см. годок типа), поэтому инкремент безусловный, симметричный unregister.
	metrics.ClientWSConnectionsActive.Inc()
}

// unregister снимает conn с регистрации userID при дисконнекте (вызывается
// из defer GetWs). Удаляет саму запись userID из реестра, если это было
// последнее соединение пользователя — чтобы реестр не рос бесконечно пустыми
// наборами простаивающих пользователей (не строго обязательно для
// корректности, но избегает утечки памяти на долго живущем процессе).
func (h *ClientConnHub) unregister(userID uuid.UUID, conn *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()

	set, ok := h.conns[userID]
	if !ok {
		return
	}
	if _, present := set[conn]; !present {
		// Не в наборе (например, повторный вызов unregister) — не в счёте
		// gauge, декрементировать нечего (симметрия с register).
		return
	}
	delete(set, conn)
	if len(set) == 0 {
		delete(h.conns, userID)
	}
	metrics.ClientWSConnectionsActive.Dec()
}

// Notify реализует api.Notifier (и структурно — task.answerNotifier, тикет
// 6.7): рассылает уведомление n ВСЕМ активным WS-соединениям n.UserID (см.
// godoc типа про политику "рассылка всем" вместо вытеснения).
//
// Отсутствие открытых соединений пользователя — НЕ ошибка: возвращает nil
// (пользователь просто не в сети ни в одной вкладке прямо сейчас; web —
// best-effort канал уведомлений, docs/adr/0005, «Последствия»). Ошибка
// возвращается ТОЛЬКО при реальном сбое сериализации кадра (что на практике
// означает баг, не штатную ситуацию) — ошибка записи в ОТДЕЛЬНОЕ соединение
// не прерывает рассылку остальным и не всплывает наверх вызывающему
// (handleAgentQuestion всё равно лишь логирует ошибку Notify и продолжает
// отправлять ack агенту, см. godoc Notifier в server.go) — она только
// логируется здесь, если задан logger.
func (h *ClientConnHub) Notify(ctx context.Context, n notify.Notification) error {
	userID := uuid.UUID(n.UserID.Bytes)

	h.mu.Lock()
	set := h.conns[userID]
	targets := make([]*websocket.Conn, 0, len(set))
	for conn := range set {
		targets = append(targets, conn)
	}
	h.mu.Unlock()

	if len(targets) == 0 {
		return nil
	}

	frame := clientNotificationFrame{
		Kind:      n.Kind,
		TaskID:    uuid.UUID(n.TaskID.Bytes).String(),
		CreatedAt: n.CreatedAt.Format(time.RFC3339),
	}
	data, err := json.Marshal(frame)
	if err != nil {
		return fmt.Errorf("marshal clientNotificationFrame: %w", err)
	}

	for _, conn := range targets {
		writeCtx, cancel := context.WithTimeout(ctx, clientNotifyWriteTimeout)
		writeErr := conn.Write(writeCtx, websocket.MessageText, data)
		cancel()
		if writeErr != nil && h.logger != nil {
			// Одно "мёртвое" (но ещё не удалённое из реестра read-loop'ом)
			// соединение не должно прерывать рассылку остальным активным
			// вкладкам того же пользователя.
			h.logger.Warn("запись WS-уведомления клиенту не удалась",
				slog.String("user_id", userID.String()), slog.String("error", writeErr.Error()))
		}
	}
	return nil
}

// GetWs реализует GET /ws — WS-handshake с аутентификацией браузера по
// access-токену (FR G1, тикет 7.2, см. godoc файла).
//
// Переопределяет 501-заглушку Unimplemented. Апгрейд выполняется всегда (см.
// godoc файла, почему); отказ аутентификации сигнализируется закрытием
// соединения кодом wsCloseUnauthorized (переиспользован из machine_ws.go),
// не HTTP-статусом.
func (s *Server) GetWs(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		// websocket.Accept сам пишет ответ в w при ошибке (не WS-запрос,
		// нарушение handshake и т.п.) — это происходит ДО апгрейда, обычный
		// HTTP-путь, добавлять здесь нечего.
		return
	}
	defer func() { _ = conn.CloseNow() }()

	userID, ok := s.authenticateClientWS(r, conn)
	if !ok {
		_ = conn.Close(wsCloseUnauthorized, "unauthorized")
		return
	}

	// Регистрация ДОБАВЛЯЕТ это соединение к уже открытым (если есть) той же
	// userID — НЕ вытесняет их (ADR 0005, см. godoc ClientConnHub).
	s.clientHub.register(userID, conn)
	defer s.clientHub.unregister(userID, conn)

	// Read-loop чисто для обнаружения дисконнекта — содержимое кадров после
	// auth игнорируется (см. godoc файла, "строго односторонний канал").
	for {
		if _, _, err := conn.Read(r.Context()); err != nil {
			return
		}
	}
}

// authenticateClientWS читает и проверяет ПЕРВЫЙ кадр WS-соединения браузера
// как {"type":"auth","access_token":"..."} (см. godoc файла). Возвращает
// (user_id, true) при успехе; (uuid.Nil, false) для ЛЮБОГО отказа — таймаут
// чтения, кадр не JSON, type != "auth", пустой access_token,
// auth.ParseAccessToken вернул ошибку (битый/просроченный/неверная подпись).
// Причина отказа намеренно не различается вызывающим (GetWs) — единый внешний
// сигнал "unauthorized", тот же принцип, что и у authenticateMachineHello
// (machine_ws.go).
func (s *Server) authenticateClientWS(r *http.Request, conn *websocket.Conn) (uuid.UUID, bool) {
	ctx, cancel := context.WithTimeout(r.Context(), clientAuthReadTimeout)
	defer cancel()

	_, data, err := conn.Read(ctx)
	if err != nil {
		return uuid.Nil, false
	}

	var frame clientAuthFrame
	if err := json.Unmarshal(data, &frame); err != nil {
		return uuid.Nil, false
	}
	if frame.Type != "auth" || frame.AccessToken == "" {
		return uuid.Nil, false
	}

	userID, err := auth.ParseAccessToken(frame.AccessToken, s.jwtSigningKey)
	if err != nil {
		return uuid.Nil, false
	}
	return userID, true
}
