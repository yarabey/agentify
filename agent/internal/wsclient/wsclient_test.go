package wsclient

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"

	"github.com/yarabey/agentify/internal/bus"
)

// Тесты agent/internal/wsclient — быстрые unit/fake-server тесты (без
// build-тега integration, без Docker), как и предписано тикетом 3.3: реальный
// E2E одним Go-тестом, поднимающим и серверную сторону
// (orchestrator/internal/api), и agent/internal/wsclient, структурно
// невозможен — обе стороны лежат за разными internal/-границами одного
// модуля (X/internal/... импортируется только кодом из дерева X/). Контрактная
// совместимость гарантируется тем, что обе стороны используют один и тот же
// internal/bus.Envelope/HelloPayload/MessageType* (см. machine_ws.go,
// рефакторинг того же тикета 3.3).
//
// Сервер здесь — fake-сервер на httptest.NewServer + coder/websocket,
// собранный прямо в тесте: достаточно, чтобы проверить, что КЛИЕНТ
// (1) шлёт корректный hello-конверт и (2) переподключается после разрыва.

// helloFrame — то, что fake-сервер увидел в первом кадре одного подключения.
type helloFrame struct {
	envelope bus.Envelope
	payload  bus.HelloPayload
}

// fakeServer — минимальный сервер /machine/ws для тестов клиента: на каждое
// входящее соединение читает первый кадр (ожидается hello), складывает его в
// канал hellos, затем либо сразу закрывает соединение (closeAfterHello),
// либо продолжает читать дальнейшие кадры (имитируя устойчивое соединение) —
// каждый разбирается как конверт события (см. handleEventFrame) и
// складывается в events; если ackEvents==true, на каждое такое событие
// сервер сразу отвечает ack-кадром (protocol.md §5) с тем же message_id
// (имитация оркестратора, подтверждающего событие агента).
type fakeServer struct {
	t               *testing.T
	closeAfterHello bool
	ackEvents       bool

	mu     sync.Mutex
	hellos []helloFrame
	events []bus.Envelope
	conns  []*websocket.Conn
}

func newFakeServer(t *testing.T, closeAfterHello bool) *fakeServer {
	t.Helper()
	return &fakeServer{t: t, closeAfterHello: closeAfterHello}
}

func (f *fakeServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer func() { _ = conn.CloseNow() }()

	f.mu.Lock()
	f.conns = append(f.conns, conn)
	f.mu.Unlock()

	ctx := r.Context()
	_, data, err := conn.Read(ctx)
	if err != nil {
		return
	}

	var env bus.Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		f.t.Errorf("fakeServer: первый кадр не парсится как bus.Envelope: %v", err)
		return
	}
	var payload bus.HelloPayload
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		f.t.Errorf("fakeServer: payload первого кадра не парсится как bus.HelloPayload: %v", err)
		return
	}

	f.mu.Lock()
	f.hellos = append(f.hellos, helloFrame{envelope: env, payload: payload})
	f.mu.Unlock()

	if f.closeAfterHello {
		_ = conn.Close(websocket.StatusNormalClosure, "test: closing after hello")
		return
	}

	// Соединение держим живым: читаем дальнейшие кадры (события агента,
	// тикет 3.5) до отмены ctx/разрыва соединения тестом.
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return
		}
		f.handleEventFrame(ctx, conn, data)
	}
}

// handleEventFrame разбирает кадр, пришедший ПОСЛЕ hello, как конверт
// события (тикет 3.5, wsclient.Client.SendEvent), складывает его в events и,
// если ackEvents включён, отвечает ack-кадром с тем же message_id.
func (f *fakeServer) handleEventFrame(ctx context.Context, conn *websocket.Conn, data []byte) {
	var env bus.Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		f.t.Errorf("fakeServer: кадр события не парсится как bus.Envelope: %v", err)
		return
	}

	f.mu.Lock()
	f.events = append(f.events, env)
	f.mu.Unlock()

	if !f.ackEvents {
		return
	}

	ackPayload, err := json.Marshal(bus.AckPayload{AckMessageID: env.MessageID})
	if err != nil {
		f.t.Errorf("fakeServer: маршалинг ack payload: %v", err)
		return
	}
	ackEnv := bus.Envelope{
		MessageID:       bus.NewMessageID(),
		IntegrationID:   env.IntegrationID,
		Type:            bus.MessageTypeAck,
		Ts:              time.Now().UTC().Format(time.RFC3339),
		ProtocolVersion: bus.ProtocolVersion,
		Payload:         ackPayload,
	}
	ackData, err := ackEnv.Marshal()
	if err != nil {
		f.t.Errorf("fakeServer: маршалинг ack-конверта: %v", err)
		return
	}
	_ = conn.Write(ctx, websocket.MessageText, ackData)
}

// lastConn возвращает последнее принятое ServeHTTP соединение — нужен
// тестам task_assigned (тикет 5.4), которым нужно писать команду СЕРВЕРОМ в
// уже установленное соединение (в отличие от событий, которые сервер только
// читает).
func (f *fakeServer) lastConn() *websocket.Conn {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.conns[len(f.conns)-1]
}

func (f *fakeServer) helloCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.hellos)
}

func (f *fakeServer) lastHello() helloFrame {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hellos[len(f.hellos)-1]
}

func (f *fakeServer) eventCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.events)
}

func (f *fakeServer) eventsSnapshot() []bus.Envelope {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]bus.Envelope, len(f.events))
	copy(out, f.events)
	return out
}

// closeAllConns принудительно рвёт ВСЕ WS-соединения, принятые этим
// сервером — имитация падения оркестратора "на живую" (клиент ещё держит
// сокет открытым). httptest.Server.CloseClientConnections/Close тут не
// годятся: websocket.Accept хиджекает http.Conn (см. coder/websocket), а
// net/http/httptest.Server при переходе в http.StateHijacked снимает
// соединение со своего внутреннего учёта (net/http/httptest/server.go,
// case http.StateHijacked) — то есть оба метода становятся no-op именно
// для уже захваченных WS-сокетов. Закрывать их приходится самим, храня
// ссылки на *websocket.Conn (см. ServeHTTP).
func (f *fakeServer) closeAllConns() {
	f.mu.Lock()
	conns := make([]*websocket.Conn, len(f.conns))
	copy(conns, f.conns)
	f.mu.Unlock()
	for _, c := range conns {
		_ = c.CloseNow()
	}
}

// fakeOutbox — потокобезопасная in-memory реализация wsclient.Outbox для
// тестов (без bbolt/файловой системы — durability проверяется отдельно,
// agent/internal/outbox/outbox_test.go; здесь важен только контракт
// Enqueue/Pending/Delete, который и использует Client). Порядок Pending —
// порядок Enqueue (FIFO), как и требует контракт Outbox.
type fakeOutbox struct {
	mu    sync.Mutex
	order []string
	byID  map[string]bus.Envelope
}

func newFakeOutbox() *fakeOutbox {
	return &fakeOutbox{byID: make(map[string]bus.Envelope)}
}

func (f *fakeOutbox) Enqueue(env bus.Envelope) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, exists := f.byID[env.MessageID]; !exists {
		f.order = append(f.order, env.MessageID)
	}
	f.byID[env.MessageID] = env
	return nil
}

func (f *fakeOutbox) Pending() ([]bus.Envelope, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]bus.Envelope, 0, len(f.order))
	for _, id := range f.order {
		if env, ok := f.byID[id]; ok {
			out = append(out, env)
		}
	}
	return out, nil
}

func (f *fakeOutbox) Delete(messageID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.byID, messageID)
	return nil
}

func (f *fakeOutbox) len() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.byID)
}

// testEventEnvelope собирает минимальный валидный конверт события для
// тестов SendEvent (message_id намеренно оставлен пустым — SendEvent должен
// сгенерировать его сам, см. godoc SendEvent).
func testEventEnvelope(integrationID string) bus.Envelope {
	return bus.Envelope{
		IntegrationID:   integrationID,
		Type:            bus.MessageTypeAgentProgress,
		Ts:              time.Now().UTC().Format(time.RFC3339),
		ProtocolVersion: bus.ProtocolVersion,
		Payload:         []byte(`{"text":"test progress"}`),
	}
}

// listenOn резервирует свободный TCP-порт и сразу отдаёт связанный слушатель
// — нужен тесту приёмки тикета 3.5 (TestSendEventSurvivesOrchestratorRestart),
// которому нужно поднять ВТОРОЙ fake-сервер на ТОМ ЖЕ адресе после того, как
// первый "упал" (имитация восстановления оркестратора на прежнем адресе).
func listenOn(t *testing.T, addr string) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("net.Listen(%s): %v", addr, err)
	}
	return ln
}

// newFakeServerAt поднимает httptest.Server на заранее выбранном слушателе
// (см. listenOn) вместо случайного порта.
func newFakeServerAt(ln net.Listener, handler http.Handler) *httptest.Server {
	ts := httptest.NewUnstartedServer(handler)
	ts.Listener = ln
	ts.Start()
	return ts
}

// wsURL преобразует http://-адрес httptest.NewServer в ws://-адрес для
// websocket.Dial (тот же приём, что и в
// orchestrator/internal/api/machine_ws_integration_test.go dialMachineWS).
func wsURL(ts *httptest.Server) string {
	return strings.Replace(ts.URL, "http://", "ws://", 1) + "/machine/ws"
}

// testLogger — тихий логгер для тестов (Discard), чтобы не засорять вывод go
// test, но прогоняющий тот же код логирования, что и в проде.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(discardWriter{}, nil))
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

// TestHelloFrameFieldsAndPayload проверяет, что клиент шлёт первым кадром
// синтаксически корректный hello-конверт (docs/protocol.md §2/§4):
// type == hello, integration_id/uuid == IntegrationUUID, providers/
// agent_version/protocol_version перенесены из Config без искажений.
func TestHelloFrameFieldsAndPayload(t *testing.T) {
	srv := newFakeServer(t, true /* closeAfterHello */)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	integrationUUID := uuid.NewString()
	cfg := Config{
		OrchestratorWSURL: wsURL(ts),
		IntegrationUUID:   integrationUUID,
		AgentVersion:      "test-agent/1.2.3",
		Providers:         []string{"claude", "claude-code"},
	}
	client, err := New(cfg, newFakeOutbox(), WithLogger(testLogger()), WithBackoff(5*time.Millisecond, 20*time.Millisecond))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	runDone := make(chan error, 1)
	go func() { runDone <- client.Run(ctx) }()

	waitForHellos(t, srv, 1, 3*time.Second)
	cancel()
	<-runDone

	got := srv.lastHello()
	if got.envelope.Type != bus.MessageTypeHello {
		t.Fatalf("type = %q, want %q", got.envelope.Type, bus.MessageTypeHello)
	}
	if got.envelope.IntegrationID != integrationUUID {
		t.Fatalf("integration_id = %q, want %q", got.envelope.IntegrationID, integrationUUID)
	}
	if got.envelope.ProtocolVersion != bus.ProtocolVersion {
		t.Fatalf("protocol_version = %q, want %q", got.envelope.ProtocolVersion, bus.ProtocolVersion)
	}
	if got.envelope.TaskID != nil {
		t.Fatalf("task_id = %v, want nil (machine-level сообщение)", got.envelope.TaskID)
	}
	if got.envelope.MessageID == "" {
		t.Fatal("message_id пуст")
	}
	if got.envelope.Ts == "" {
		t.Fatal("ts пуст")
	}
	if _, err := time.Parse(time.RFC3339, got.envelope.Ts); err != nil {
		t.Fatalf("ts не RFC3339: %v", err)
	}
	if got.payload.UUID != integrationUUID {
		t.Fatalf("payload.uuid = %q, want %q", got.payload.UUID, integrationUUID)
	}
	if got.payload.AgentVersion != cfg.AgentVersion {
		t.Fatalf("payload.agent_version = %q, want %q", got.payload.AgentVersion, cfg.AgentVersion)
	}
	if len(got.payload.Providers) != 2 || got.payload.Providers[0] != "claude" || got.payload.Providers[1] != "claude-code" {
		t.Fatalf("payload.providers = %v, want [claude claude-code]", got.payload.Providers)
	}
}

// TestReconnectsAfterServerCloses проверяет, что клиент с быстрым
// (миллисекундным) backoff переподключается после разрыва соединения
// сервером и шлёт hello повторно — приёмка тикета 3.3 "реконнект после
// обрыва".
func TestReconnectsAfterServerCloses(t *testing.T) {
	srv := newFakeServer(t, true /* closeAfterHello: каждое соединение рвётся сразу после hello */)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	cfg := Config{
		OrchestratorWSURL: wsURL(ts),
		IntegrationUUID:   uuid.NewString(),
		AgentVersion:      "test-agent/0.0.0",
		Providers:         []string{"claude"},
	}
	client, err := New(cfg, newFakeOutbox(), WithLogger(testLogger()), WithBackoff(2*time.Millisecond, 10*time.Millisecond))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	runDone := make(chan error, 1)
	go func() { runDone <- client.Run(ctx) }()

	// Сервер рвёт каждое соединение сразу после hello — за разумное время
	// клиент должен успеть прислать ХОТЯ БЫ 2 hello (исходный + минимум один
	// реконнект), используя инжектированный быстрый backoff (иначе тест
	// зависал бы на реальных секундах дефолтного backoff).
	waitForHellos(t, srv, 2, 5*time.Second)

	cancel()
	if err := <-runDone; err == nil {
		t.Fatal("Run должен был вернуть ошибку отмены ctx, получен nil")
	}
}

// TestRunStopsOnContextCancel проверяет, что отмена ctx останавливает Run без
// зависания, даже когда соединение установлено и клиент сидит в read-loop.
// context.WithTimeout на сам тест — лишь защита от зависания CI; сама логика
// клиента обязана среагировать на cancel() заметно раньше таймаута теста.
func TestRunStopsOnContextCancel(t *testing.T) {
	srv := newFakeServer(t, false /* держим соединение открытым */)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	cfg := Config{
		OrchestratorWSURL: wsURL(ts),
		IntegrationUUID:   uuid.NewString(),
		AgentVersion:      "test-agent/0.0.0",
		Providers:         []string{"claude"},
	}
	client, err := New(cfg, newFakeOutbox(), WithLogger(testLogger()), WithBackoff(2*time.Millisecond, 10*time.Millisecond))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	testCtx, testCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer testCancel()

	runCtx, runCancel := context.WithCancel(testCtx)

	runDone := make(chan error, 1)
	go func() { runDone <- client.Run(runCtx) }()

	waitForHellos(t, srv, 1, 3*time.Second)

	start := time.Now()
	runCancel()

	select {
	case err := <-runDone:
		if err == nil {
			t.Fatal("Run должен вернуть ошибку отмены ctx, получен nil")
		}
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Fatalf("Run завершился слишком медленно после отмены ctx: %s", elapsed)
		}
	case <-testCtx.Done():
		t.Fatal("Run не вернул управление после отмены ctx (завис)")
	}
}

// TestNewValidatesConfig проверяет, что New валидирует обязательные поля
// Config и возвращает ошибку конструктора, не паникуя (см. godoc New).
func TestNewValidatesConfig(t *testing.T) {
	validUUID := uuid.NewString()

	t.Run("empty url", func(t *testing.T) {
		_, err := New(Config{OrchestratorWSURL: "", IntegrationUUID: validUUID}, newFakeOutbox())
		if err == nil {
			t.Fatal("ожидалась ошибка на пустом OrchestratorWSURL")
		}
	})

	t.Run("invalid uuid", func(t *testing.T) {
		_, err := New(Config{OrchestratorWSURL: "ws://example.invalid/machine/ws", IntegrationUUID: "not-a-uuid"}, newFakeOutbox())
		if err == nil {
			t.Fatal("ожидалась ошибка на невалидном IntegrationUUID")
		}
	})

	t.Run("nil outbox", func(t *testing.T) {
		_, err := New(Config{OrchestratorWSURL: "ws://example.invalid/machine/ws", IntegrationUUID: validUUID}, nil)
		if !errors.Is(err, ErrNilOutbox) {
			t.Fatalf("ошибка = %v, want ErrNilOutbox", err)
		}
	})

	t.Run("valid", func(t *testing.T) {
		c, err := New(Config{OrchestratorWSURL: "ws://example.invalid/machine/ws", IntegrationUUID: validUUID}, newFakeOutbox())
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if c == nil {
			t.Fatal("New вернул nil-клиент без ошибки")
		}
	})
}

// waitForHellos ждёт, пока fake-сервер не увидит минимум n hello-кадров,
// опрашивая helloCount с коротким интервалом — без сна на полную длительность
// timeout, тест завершает ожидание сразу, как только условие выполнено.
func waitForHellos(t *testing.T, srv *fakeServer, n int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if srv.helloCount() >= n {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("не дождались %d hello-кадров за %s (получено %d)", n, timeout, srv.helloCount())
}

// waitForEvents ждёт, пока fake-сервер не увидит минимум n кадров событий
// (см. fakeServer.handleEventFrame), опрашивая eventCount с коротким
// интервалом — тот же приём, что waitForHellos.
func waitForEvents(t *testing.T, srv *fakeServer, n int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if srv.eventCount() >= n {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("не дождались %d событий за %s (получено %d)", n, timeout, srv.eventCount())
}

// waitForOutboxEmpty ждёт, пока outbox не опустеет (Pending() == 0),
// опрашивая с коротким интервалом — тот же приём, что waitForHellos.
func waitForOutboxEmpty(t *testing.T, out *fakeOutbox, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if out.len() == 0 {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("outbox не опустел за %s (осталось %d)", timeout, out.len())
}

// TestSendEventDeliversToConnectedServer проверяет базовый happy-path
// SendEvent: пока клиент подключён, событие durable-записывается в outbox и
// доставляется fake-серверу по WS (тикет 3.5, protocol.md §5, шаг 1
// "кладёт событие в локальный outbox → шлёт по WS").
func TestSendEventDeliversToConnectedServer(t *testing.T) {
	srv := newFakeServer(t, false /* держим соединение живым, не подтверждаем ack */)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	integrationUUID := uuid.NewString()
	cfg := Config{
		OrchestratorWSURL: wsURL(ts),
		IntegrationUUID:   integrationUUID,
		AgentVersion:      "test-agent/0.0.0",
		Providers:         []string{"claude"},
	}
	out := newFakeOutbox()
	client, err := New(cfg, out, WithLogger(testLogger()), WithBackoff(2*time.Millisecond, 10*time.Millisecond))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	runDone := make(chan error, 1)
	go func() { runDone <- client.Run(ctx) }()

	waitForHellos(t, srv, 1, 3*time.Second)

	env := testEventEnvelope(integrationUUID)
	if err := client.SendEvent(context.Background(), env); err != nil {
		t.Fatalf("SendEvent: %v", err)
	}

	waitForEvents(t, srv, 1, 3*time.Second)

	got := srv.eventsSnapshot()[0]
	if got.Type != bus.MessageTypeAgentProgress {
		t.Fatalf("type = %q, want %q", got.Type, bus.MessageTypeAgentProgress)
	}
	if got.IntegrationID != integrationUUID {
		t.Fatalf("integration_id = %q, want %q", got.IntegrationID, integrationUUID)
	}
	if got.MessageID == "" {
		t.Fatal("SendEvent должен был сгенерировать message_id (был пуст в исходном конверте)")
	}

	cancel()
	<-runDone
}

// TestSendEventBeforeConnectStaysInOutbox проверяет, что SendEvent,
// вызванный когда серверу физически некуда доставить событие (сервер ещё не
// поднят/недоступен), не блокируется навечно и не паникует — событие
// остаётся в outbox (Pending непуст), как и требует контракт SendEvent
// (durable-запись ДО сети, см. её godoc).
func TestSendEventBeforeConnectStaysInOutbox(t *testing.T) {
	// Резервируем адрес, но сервер на нём не поднимаем — dial будет
	// стабильно проваливаться (connection refused).
	ln := listenOn(t, "127.0.0.1:0")
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("ln.Close: %v", err)
	}

	out := newFakeOutbox()
	cfg := Config{
		OrchestratorWSURL: "ws://" + addr + "/machine/ws",
		IntegrationUUID:   uuid.NewString(),
		AgentVersion:      "test-agent/0.0.0",
		Providers:         []string{"claude"},
	}
	client, err := New(cfg, out, WithLogger(testLogger()), WithBackoff(2*time.Millisecond, 10*time.Millisecond))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	runDone := make(chan error, 1)
	go func() { runDone <- client.Run(ctx) }()

	env := testEventEnvelope(cfg.IntegrationUUID)
	sendCtx, sendCancel := context.WithTimeout(context.Background(), time.Second)
	defer sendCancel()
	if err := client.SendEvent(sendCtx, env); err != nil {
		t.Fatalf("SendEvent: %v", err)
	}

	// Durable-запись в outbox синхронна внутри SendEvent (см. её godoc) —
	// сразу после возврата событие уже должно быть видно в Pending,
	// независимо от состояния сети.
	pending, err := out.Pending()
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("len(pending) = %d, want 1 (событие должно остаться в outbox, сервер недоступен)", len(pending))
	}
	// env передавался в SendEvent по значению с пустым MessageID (см.
	// testEventEnvelope) — SendEvent сам сгенерировал id для durable-записи,
	// поэтому здесь просто проверяем, что в outbox лежит непустой message_id.
	if pending[0].MessageID == "" {
		t.Fatal("message_id в outbox пуст")
	}

	cancel()
	select {
	case <-runDone:
	case <-time.After(3 * time.Second):
		t.Fatal("client.Run не завершился после отмены ctx (завис)")
	}
}

// TestSendEventSurvivesOrchestratorRestart — ключевой тест приёмки тикета
// 3.5 (docs/MVP_TICKETS.md 3.5 "убить оркестратор на время → события агента
// не потеряны"): клиент подключается к первому fake-серверу, SendEvent
// кладёт событие в outbox и отправляет его по WS; сервер получает конверт,
// но НЕ отвечает ack — затем тест обрывает это соединение (имитация падения
// оркестратора ДО ack). Клиент должен обнаружить разрыв и уйти в
// backoff/реконнект, не потеряв событие (оно остаётся в outbox, т.к. ack не
// был получен). Когда на ТОМ ЖЕ адресе поднимается второй fake-сервер
// (имитация восстановления оркестратора) и уже отвечает ack, клиент должен
// переподключиться, переотправить событие (redelivery "при следующем
// коннекте", protocol.md §5) и получить ack — после чего outbox должен
// опустеть.
func TestSendEventSurvivesOrchestratorRestart(t *testing.T) {
	ln1 := listenOn(t, "127.0.0.1:0")
	addr := ln1.Addr().String()

	srv1 := newFakeServer(t, false /* держим соединение живым, ack НЕ шлём */)
	ts1 := newFakeServerAt(ln1, srv1)

	integrationUUID := uuid.NewString()
	cfg := Config{
		OrchestratorWSURL: "ws://" + addr + "/machine/ws",
		IntegrationUUID:   integrationUUID,
		AgentVersion:      "test-agent/0.0.0",
		Providers:         []string{"claude"},
	}
	out := newFakeOutbox()
	client, err := New(cfg, out, WithLogger(testLogger()), WithBackoff(2*time.Millisecond, 20*time.Millisecond))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	runDone := make(chan error, 1)
	go func() { runDone <- client.Run(ctx) }()

	waitForHellos(t, srv1, 1, 5*time.Second)

	env := testEventEnvelope(integrationUUID)
	if err := client.SendEvent(context.Background(), env); err != nil {
		t.Fatalf("SendEvent: %v", err)
	}

	// Сервер №1 получил конверт, но ack не пришлёт — событие должно
	// остаться в outbox (ack ещё не было).
	waitForEvents(t, srv1, 1, 5*time.Second)
	if out.len() != 1 {
		t.Fatalf("outbox.len() = %d, want 1 (событие ещё не подтверждено)", out.len())
	}

	// "Убиваем" оркестратор: разрываем соединение и останавливаем сервер №1
	// ДО получения ack. closeAllConns принудительно рвёт открытый WS-сокет
	// (см. её godoc — httptest.Server.CloseClientConnections/Close тут не
	// годятся, они не видят уже хиджекнутые соединения); только после этого
	// ts1.Close() не виснет, ожидая завершения хендлера.
	srv1.closeAllConns()
	ts1.Close()

	// Поднимаем сервер №2 на ТОМ ЖЕ адресе (имитация восстановления
	// оркестратора) — на этот раз он отвечает ack на полученные события.
	ln2 := listenOn(t, addr)
	srv2 := newFakeServer(t, false)
	srv2.ackEvents = true
	ts2 := newFakeServerAt(ln2, srv2)
	defer ts2.Close()

	// Клиент должен переподключиться (backoff — миллисекунды, см. New выше)
	// и переотправить неподтверждённое событие серверу №2.
	waitForHellos(t, srv2, 1, 10*time.Second)
	waitForEvents(t, srv2, 1, 10*time.Second)

	got := srv2.eventsSnapshot()[0]
	if got.IntegrationID != integrationUUID {
		t.Fatalf("integration_id = %q, want %q", got.IntegrationID, integrationUUID)
	}

	// Ack от сервера №2 должен опустошить outbox — "ничего не потеряно"
	// доказано: событие пережило обрыв соединения и было доставлено после
	// восстановления.
	waitForOutboxEmpty(t, out, 10*time.Second)

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("client.Run не завершился после отмены ctx (завис)")
	}
}

// fakeTaskAssignedHandler — подменный Config.OnTaskAssigned для тестов
// (тикет 5.4): запоминает все полученные конверты и возвращает
// настраиваемую ошибку (err), имитируя провал "не удалось даже начать
// задачу" (нет провайдера/невалидный payload — см. годок
// Config.OnTaskAssigned).
type fakeTaskAssignedHandler struct {
	err error

	mu       sync.Mutex
	received []bus.Envelope
}

func (h *fakeTaskAssignedHandler) handle(_ context.Context, env bus.Envelope) error {
	h.mu.Lock()
	h.received = append(h.received, env)
	h.mu.Unlock()
	return h.err
}

func (h *fakeTaskAssignedHandler) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.received)
}

// taskAssignedEnvelope собирает валидный конверт type==task_assigned
// (protocol.md §4, тикет 5.4) с заданным message_id/task_id.
func taskAssignedEnvelope(t *testing.T, messageID, integrationID, taskID string) bus.Envelope {
	t.Helper()
	payload, err := json.Marshal(bus.TaskAssignedPayload{Text: "сделай что-нибудь полезное"})
	if err != nil {
		t.Fatalf("marshal TaskAssignedPayload: %v", err)
	}
	return bus.Envelope{
		MessageID:       messageID,
		TaskID:          &taskID,
		IntegrationID:   integrationID,
		Type:            bus.MessageTypeTaskAssigned,
		Ts:              time.Now().UTC().Format(time.RFC3339),
		ProtocolVersion: bus.ProtocolVersion,
		Payload:         payload,
	}
}

// writeServerFrame пишет data от лица сервера (оркестратора) в conn — общий
// хелпер для тестов task_assigned, зеркало writeClientFrame в
// orchestrator/internal/api/machine_ws_test.go.
func writeServerFrame(t *testing.T, conn *websocket.Conn, data []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
		t.Fatalf("writeServerFrame: %v", err)
	}
}

// findAckFor ищет среди накопленных fake-сервером кадров (см.
// fakeServer.eventsSnapshot — сервер складывает туда ЛЮБОЙ кадр,
// полученный от клиента после hello, включая ack) конверт type==ack с
// заданным ack_message_id.
func findAckFor(envs []bus.Envelope, wantAckMessageID string) bool {
	for _, env := range envs {
		if env.Type != bus.MessageTypeAck {
			continue
		}
		var payload bus.AckPayload
		if err := json.Unmarshal(env.Payload, &payload); err != nil {
			continue
		}
		if payload.AckMessageID == wantAckMessageID {
			return true
		}
	}
	return false
}

// waitForAck ждёт, пока среди кадров, полученных fake-сервером от клиента,
// не появится ack с заданным ack_message_id (тот же приём опроса, что и
// waitForHellos/waitForEvents).
func waitForAck(t *testing.T, srv *fakeServer, wantAckMessageID string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if findAckFor(srv.eventsSnapshot(), wantAckMessageID) {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return false
}

// TestHandleFrame_TaskAssigned_HappyPath_SendsAck — task_assigned с
// настроенным OnTaskAssigned, вернувшим nil, → клиент вызывает колбэк и
// немедленно отвечает ack с правильным ack_message_id (тикет 5.4, FR E1).
func TestHandleFrame_TaskAssigned_HappyPath_SendsAck(t *testing.T) {
	srv := newFakeServer(t, false /* держим соединение живым */)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	handler := &fakeTaskAssignedHandler{}
	integrationUUID := uuid.NewString()
	cfg := Config{
		OrchestratorWSURL: wsURL(ts),
		IntegrationUUID:   integrationUUID,
		AgentVersion:      "test-agent/0.0.0",
		Providers:         []string{"claude-code"},
		OnTaskAssigned:    handler.handle,
	}
	client, err := New(cfg, newFakeOutbox(), WithLogger(testLogger()), WithBackoff(2*time.Millisecond, 10*time.Millisecond))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	runDone := make(chan error, 1)
	go func() { runDone <- client.Run(ctx) }()

	waitForHellos(t, srv, 1, 3*time.Second)

	messageID := bus.NewMessageID()
	taskID := uuid.NewString()
	env := taskAssignedEnvelope(t, messageID, integrationUUID, taskID)
	data, err := env.Marshal()
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	writeServerFrame(t, srv.lastConn(), data)

	if !waitForAck(t, srv, messageID, 3*time.Second) {
		t.Fatal("сервер не получил ack на task_assigned, хотя OnTaskAssigned вернул nil")
	}
	if handler.count() != 1 {
		t.Fatalf("OnTaskAssigned вызван %d раз(а), ожидался 1", handler.count())
	}

	cancel()
	<-runDone
}

// TestHandleFrame_TaskAssigned_HandlerError_NoAck — OnTaskAssigned вернул
// ошибку (задачу не удалось даже начать) → клиент НЕ отправляет ack (агент
// получит редоставку той же команды от моста, тикет 5.4).
func TestHandleFrame_TaskAssigned_HandlerError_NoAck(t *testing.T) {
	srv := newFakeServer(t, false)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	handler := &fakeTaskAssignedHandler{err: errors.New("нет доступного провайдера для задачи")}
	integrationUUID := uuid.NewString()
	cfg := Config{
		OrchestratorWSURL: wsURL(ts),
		IntegrationUUID:   integrationUUID,
		AgentVersion:      "test-agent/0.0.0",
		Providers:         []string{"claude-code"},
		OnTaskAssigned:    handler.handle,
	}
	client, err := New(cfg, newFakeOutbox(), WithLogger(testLogger()), WithBackoff(2*time.Millisecond, 10*time.Millisecond))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	runDone := make(chan error, 1)
	go func() { runDone <- client.Run(ctx) }()

	waitForHellos(t, srv, 1, 3*time.Second)

	messageID := bus.NewMessageID()
	env := taskAssignedEnvelope(t, messageID, integrationUUID, uuid.NewString())
	data, err := env.Marshal()
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	writeServerFrame(t, srv.lastConn(), data)

	// handler.count()==1 подтверждает, что колбэк реально был вызван (не
	// просто гонка "сервер ещё не отправил кадр") — только после этого имеет
	// смысл проверять отсутствие ack.
	deadline := time.Now().Add(2 * time.Second)
	for handler.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if handler.count() != 1 {
		t.Fatal("OnTaskAssigned не был вызван за отведённое время")
	}

	if waitForAck(t, srv, messageID, 300*time.Millisecond) {
		t.Fatal("сервер получил ack, хотя OnTaskAssigned вернул ошибку")
	}

	cancel()
	<-runDone
}

// TestHandleFrame_TaskAssigned_NoHandlerConfigured_NoAckNoPanic —
// OnTaskAssigned не настроен (nil, значение по умолчанию Config) →
// task_assigned молча игнорируется: ack не отправляется, клиент не
// паникует и продолжает работать (обратная совместимость с тикетом 3.5,
// тикет 5.4).
func TestHandleFrame_TaskAssigned_NoHandlerConfigured_NoAckNoPanic(t *testing.T) {
	srv := newFakeServer(t, false)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	integrationUUID := uuid.NewString()
	cfg := Config{
		OrchestratorWSURL: wsURL(ts),
		IntegrationUUID:   integrationUUID,
		AgentVersion:      "test-agent/0.0.0",
		Providers:         []string{"claude-code"},
		// OnTaskAssigned намеренно не задан.
	}
	client, err := New(cfg, newFakeOutbox(), WithLogger(testLogger()), WithBackoff(2*time.Millisecond, 10*time.Millisecond))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	runDone := make(chan error, 1)
	go func() { runDone <- client.Run(ctx) }()

	waitForHellos(t, srv, 1, 3*time.Second)

	messageID := bus.NewMessageID()
	env := taskAssignedEnvelope(t, messageID, integrationUUID, uuid.NewString())
	data, err := env.Marshal()
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	writeServerFrame(t, srv.lastConn(), data)

	if waitForAck(t, srv, messageID, 300*time.Millisecond) {
		t.Fatal("сервер получил ack, хотя OnTaskAssigned не настроен")
	}

	cancel()
	if err := <-runDone; err == nil {
		t.Fatal("Run должен был вернуть ошибку отмены ctx, получен nil")
	}
}

// fakeCommandDecisionHandler — подменный Config.OnCommandDecision для тестов
// (тикет 6.5): запоминает все полученные конверты и возвращает
// настраиваемую ошибку (err), имитируя провал применения решения (нет
// активной задачи/невалидный payload/неизвестный request_id — см. годок
// Config.OnCommandDecision). Зеркало fakeTaskAssignedHandler.
type fakeCommandDecisionHandler struct {
	err error

	mu       sync.Mutex
	received []bus.Envelope
}

func (h *fakeCommandDecisionHandler) handle(_ context.Context, env bus.Envelope) error {
	h.mu.Lock()
	h.received = append(h.received, env)
	h.mu.Unlock()
	return h.err
}

func (h *fakeCommandDecisionHandler) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.received)
}

// commandDecisionEnvelope собирает валидный конверт type==command_decision
// (protocol.md §4, тикет 6.5) с заданными message_id/task_id/request_id/decision.
func commandDecisionEnvelope(t *testing.T, messageID, integrationID, taskID, requestID, decision string) bus.Envelope {
	t.Helper()
	payload, err := json.Marshal(bus.CommandDecisionPayload{RequestID: requestID, Decision: decision})
	if err != nil {
		t.Fatalf("marshal CommandDecisionPayload: %v", err)
	}
	return bus.Envelope{
		MessageID:       messageID,
		TaskID:          &taskID,
		IntegrationID:   integrationID,
		Type:            bus.MessageTypeCommandDecision,
		Ts:              time.Now().UTC().Format(time.RFC3339),
		ProtocolVersion: bus.ProtocolVersion,
		Payload:         payload,
	}
}

// TestHandleFrame_CommandDecision_HappyPath_SendsAck — command_decision с
// настроенным OnCommandDecision, вернувшим nil, → клиент вызывает колбэк и
// немедленно отвечает ack с правильным ack_message_id (тикет 6.5, FR F3).
func TestHandleFrame_CommandDecision_HappyPath_SendsAck(t *testing.T) {
	srv := newFakeServer(t, false /* держим соединение живым */)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	handler := &fakeCommandDecisionHandler{}
	integrationUUID := uuid.NewString()
	cfg := Config{
		OrchestratorWSURL: wsURL(ts),
		IntegrationUUID:   integrationUUID,
		AgentVersion:      "test-agent/0.0.0",
		Providers:         []string{"claude-code"},
		OnCommandDecision: handler.handle,
	}
	client, err := New(cfg, newFakeOutbox(), WithLogger(testLogger()), WithBackoff(2*time.Millisecond, 10*time.Millisecond))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	runDone := make(chan error, 1)
	go func() { runDone <- client.Run(ctx) }()

	waitForHellos(t, srv, 1, 3*time.Second)

	messageID := bus.NewMessageID()
	taskID := uuid.NewString()
	env := commandDecisionEnvelope(t, messageID, integrationUUID, taskID, "req-1", "reject")
	data, err := env.Marshal()
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	writeServerFrame(t, srv.lastConn(), data)

	if !waitForAck(t, srv, messageID, 3*time.Second) {
		t.Fatal("сервер не получил ack на command_decision, хотя OnCommandDecision вернул nil")
	}
	if handler.count() != 1 {
		t.Fatalf("OnCommandDecision вызван %d раз(а), ожидался 1", handler.count())
	}

	cancel()
	<-runDone
}

// TestHandleFrame_CommandDecision_HandlerError_NoAck — OnCommandDecision
// вернул ошибку (решение не удалось применить) → клиент НЕ отправляет ack
// (агент получит редоставку того же решения от моста, тикет 6.5).
func TestHandleFrame_CommandDecision_HandlerError_NoAck(t *testing.T) {
	srv := newFakeServer(t, false)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	handler := &fakeCommandDecisionHandler{err: errors.New("нет активной задачи для command_decision")}
	integrationUUID := uuid.NewString()
	cfg := Config{
		OrchestratorWSURL: wsURL(ts),
		IntegrationUUID:   integrationUUID,
		AgentVersion:      "test-agent/0.0.0",
		Providers:         []string{"claude-code"},
		OnCommandDecision: handler.handle,
	}
	client, err := New(cfg, newFakeOutbox(), WithLogger(testLogger()), WithBackoff(2*time.Millisecond, 10*time.Millisecond))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	runDone := make(chan error, 1)
	go func() { runDone <- client.Run(ctx) }()

	waitForHellos(t, srv, 1, 3*time.Second)

	messageID := bus.NewMessageID()
	env := commandDecisionEnvelope(t, messageID, integrationUUID, uuid.NewString(), "req-2", "approve")
	data, err := env.Marshal()
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	writeServerFrame(t, srv.lastConn(), data)

	// handler.count()==1 подтверждает, что колбэк реально был вызван (не
	// просто гонка "сервер ещё не отправил кадр") — только после этого имеет
	// смысл проверять отсутствие ack.
	deadline := time.Now().Add(2 * time.Second)
	for handler.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if handler.count() != 1 {
		t.Fatal("OnCommandDecision не был вызван за отведённое время")
	}

	if waitForAck(t, srv, messageID, 300*time.Millisecond) {
		t.Fatal("сервер получил ack, хотя OnCommandDecision вернул ошибку")
	}

	cancel()
	<-runDone
}

// TestHandleFrame_CommandDecision_NoHandlerConfigured_NoAckNoPanic —
// OnCommandDecision не настроен (nil, значение по умолчанию Config) →
// command_decision молча игнорируется: ack не отправляется, клиент не
// паникует и продолжает работать (тикет 6.5).
func TestHandleFrame_CommandDecision_NoHandlerConfigured_NoAckNoPanic(t *testing.T) {
	srv := newFakeServer(t, false)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	integrationUUID := uuid.NewString()
	cfg := Config{
		OrchestratorWSURL: wsURL(ts),
		IntegrationUUID:   integrationUUID,
		AgentVersion:      "test-agent/0.0.0",
		Providers:         []string{"claude-code"},
		// OnCommandDecision намеренно не задан.
	}
	client, err := New(cfg, newFakeOutbox(), WithLogger(testLogger()), WithBackoff(2*time.Millisecond, 10*time.Millisecond))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	runDone := make(chan error, 1)
	go func() { runDone <- client.Run(ctx) }()

	waitForHellos(t, srv, 1, 3*time.Second)

	messageID := bus.NewMessageID()
	env := commandDecisionEnvelope(t, messageID, integrationUUID, uuid.NewString(), "req-3", "reject")
	data, err := env.Marshal()
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	writeServerFrame(t, srv.lastConn(), data)

	if waitForAck(t, srv, messageID, 300*time.Millisecond) {
		t.Fatal("сервер получил ack, хотя OnCommandDecision не настроен")
	}

	cancel()
	if err := <-runDone; err == nil {
		t.Fatal("Run должен был вернуть ошибку отмены ctx, получен nil")
	}
}
