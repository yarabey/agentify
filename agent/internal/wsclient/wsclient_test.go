package wsclient

import (
	"context"
	"encoding/json"
	"log/slog"
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
// либо блокируется на чтении (имитируя устойчивое соединение) до отмены ctx
// теста/закрытия сервера.
type fakeServer struct {
	t               *testing.T
	closeAfterHello bool

	mu     sync.Mutex
	hellos []helloFrame
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

	// Соединение держим живым (минимальный read-loop), пока клиент/тест не
	// разорвёт его сам.
	for {
		if _, _, err := conn.Read(ctx); err != nil {
			return
		}
	}
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
	client, err := New(cfg, WithLogger(testLogger()), WithBackoff(5*time.Millisecond, 20*time.Millisecond))
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
	client, err := New(cfg, WithLogger(testLogger()), WithBackoff(2*time.Millisecond, 10*time.Millisecond))
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
	client, err := New(cfg, WithLogger(testLogger()), WithBackoff(2*time.Millisecond, 10*time.Millisecond))
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
		_, err := New(Config{OrchestratorWSURL: "", IntegrationUUID: validUUID})
		if err == nil {
			t.Fatal("ожидалась ошибка на пустом OrchestratorWSURL")
		}
	})

	t.Run("invalid uuid", func(t *testing.T) {
		_, err := New(Config{OrchestratorWSURL: "ws://example.invalid/machine/ws", IntegrationUUID: "not-a-uuid"})
		if err == nil {
			t.Fatal("ожидалась ошибка на невалидном IntegrationUUID")
		}
	})

	t.Run("valid", func(t *testing.T) {
		c, err := New(Config{OrchestratorWSURL: "ws://example.invalid/machine/ws", IntegrationUUID: validUUID})
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
