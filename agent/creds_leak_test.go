// agent/creds_leak_test.go — тикет 4.6 "Безопасное хранение кредов":
// интеграционный тест приёмки FR C4 "оркестратор не получает кредов —
// проверка трафика в интеграционном тесте" (docs/MVP_TICKETS.md 4.6).
//
// Почему тест лежит здесь, а не в agent/internal/wsclient
// (agent/internal/wsclient/wsclient_test.go): именно в пакете main
// (agent/main.go) креды провайдера (config.ClaudeAPIKey/ClaudeCodeAPIKey) и
// wsclient.Client впервые встречаются в одном месте — только тут можно
// напрямую собрать полноценный config{} с заданными кредами и убедиться, что
// структурно ни один из типов исходящих сообщений (hello, sendHeartbeat),
// которые main.go реально отправляет сегодня, не способен протащить эти поля
// наружу; в internal/wsclient типа с кредами вообще не существует, так что
// там доказывать нечего.
//
// Сценарий: поднимаем fake WS-сервер оркестратора (тот же приём, что
// agent/internal/wsclient/wsclient_test.go: httptest.NewServer +
// coder/websocket), но вместо разбора конвертов сохраняем СЫРЫЕ байты
// каждого полученного кадра — ровно то, что реально ушло бы по сети.
// Запускаем настоящий wsclient.Client (New + Run) с конфигом, где
// IntegrationUUID задан, и настоящий outbox.Store (bbolt, как в run()).
// Задаём config.ClaudeAPIKey/ClaudeCodeAPIKey = сентинел-значения и вручную
// вызываем sendHeartbeat — ровно то, что делает runHeartbeatLoop/main.go:run.
// После остановки клиента проверяем, что ни в одном сыром кадре, дошедшем до
// fake-сервера, сентинел-подстрока не встречается.
package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"

	"github.com/yarabey/agentify/agent/internal/outbox"
	"github.com/yarabey/agentify/agent/internal/wsclient"
)

// rawFrameCapturingServer — минимальный fake-сервер /machine/ws, который НЕ
// пытается разобрать кадры как bus.Envelope (в отличие от fakeServer в
// wsclient_test.go) — вместо этого сохраняет сырые байты КАЖДОГО кадра,
// пришедшего от клиента, ровно так, как они были переданы по сети. Это и
// есть "проверка трафика" из приёмки тикета 4.6: сентинел ищется по сырым
// байтам, а не по распарсенным полям — так тест ловит утечку даже в
// гипотетическом будущем поле, о котором сегодня не знает распарсенная
// структура.
type rawFrameCapturingServer struct {
	mu     sync.Mutex
	frames [][]byte
}

func (s *rawFrameCapturingServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer func() { _ = conn.CloseNow() }()

	ctx := r.Context()
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return
		}
		cp := make([]byte, len(data))
		copy(cp, data)
		s.mu.Lock()
		s.frames = append(s.frames, cp)
		s.mu.Unlock()
	}
}

func (s *rawFrameCapturingServer) snapshot() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([][]byte, len(s.frames))
	copy(out, s.frames)
	return out
}

func (s *rawFrameCapturingServer) frameCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.frames)
}

// waitForCredsLeakFrames ждёт минимум n сырых кадров у fake-сервера — тот же
// приём, что waitForHellos в wsclient_test.go.
func waitForCredsLeakFrames(t *testing.T, srv *rawFrameCapturingServer, n int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if srv.frameCount() >= n {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("не дождались %d кадров за %s (получено %d)", n, timeout, srv.frameCount())
}

// TestOrchestratorNeverReceivesProviderCredentials — приёмка тикета 4.6 (FR
// C4, §3 "Пользователь выбирает провайдера сам"): ни при hello, ни при
// heartbeat — единственных типах исходящих сообщений, которые агент умеет
// отправлять сегодня (bus.MessageTypeHello, bus.MessageTypeHeartbeat;
// полный task-loop — EPIC 5.x/тикет 4.5, вне объёма этого тикета) —
// сентинел-значения кредов провайдера, заданные в
// config.ClaudeAPIKey/ClaudeCodeAPIKey (ровно те поля, что заполняет
// agent/setup.go и грузит platform.LoadConfig в run()), не должны попасть НИ
// В ОДИН сырой байтовый кадр, дошедший до оркестратора.
func TestOrchestratorNeverReceivesProviderCredentials(t *testing.T) {
	const claudeSecret = "sk-secret-canary-VALUE-12345"
	const claudeCodeSecret = "cc-secret-canary-VALUE-67890"

	srv := &rawFrameCapturingServer{}
	ts := httptest.NewServer(srv)
	defer ts.Close()

	// cfg — полноценный config агента (agent/main.go), как его собрал бы
	// platform.LoadConfig из env/agent.env после `agentify-agent setup`:
	// креды присутствуют в cfg, но НЕ являются частью wsclient.Config ниже.
	cfg := config{
		OrchestratorWSURL: strings.Replace(ts.URL, "http://", "ws://", 1) + "/machine/ws",
		IntegrationUUID:   uuid.NewString(),
		Providers:         []string{"claude", "claude-code"},
		ClaudeAPIKey:      claudeSecret,
		ClaudeCodeAPIKey:  claudeCodeSecret,
	}

	store, err := outbox.Open(filepath.Join(t.TempDir(), "agent-outbox.db"))
	if err != nil {
		t.Fatalf("outbox.Open: %v", err)
	}
	defer func() { _ = store.Close() }()

	wsClient, err := wsclient.New(wsclient.Config{
		OrchestratorWSURL: cfg.OrchestratorWSURL,
		IntegrationUUID:   cfg.IntegrationUUID,
		AgentVersion:      "test-agent/creds-leak",
		Providers:         cfg.Providers,
	}, store, wsclient.WithLogger(discardLogger()), wsclient.WithBackoff(2*time.Millisecond, 10*time.Millisecond))
	if err != nil {
		t.Fatalf("wsclient.New: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	runDone := make(chan error, 1)
	go func() { runDone <- wsClient.Run(ctx) }()

	// Ждём hello — первый кадр после коннекта (см. wsclient.Client.sendHello).
	waitForCredsLeakFrames(t, srv, 1, 3*time.Second)

	// Ровно то же самое, что делает runHeartbeatLoop/main.go:run — один
	// heartbeat через тот же durable-путь outbox -> WS, что и остальные
	// события агент->оркестратор.
	sendHeartbeat(ctx, wsClient, cfg.IntegrationUUID, discardLogger())
	waitForCredsLeakFrames(t, srv, 2, 3*time.Second)

	cancel()
	<-runDone

	frames := srv.snapshot()
	if len(frames) < 2 {
		t.Fatalf("получено %d кадров, ожидалось минимум 2 (hello + heartbeat)", len(frames))
	}
	for i, frame := range frames {
		if strings.Contains(string(frame), claudeSecret) {
			t.Fatalf("кадр #%d содержит ClaudeAPIKey сентинел — креды провайдера утекли в трафик к оркестратору:\n%s", i, frame)
		}
		if strings.Contains(string(frame), claudeCodeSecret) {
			t.Fatalf("кадр #%d содержит ClaudeCodeAPIKey сентинел — креды провайдера утекли в трафик к оркестратору:\n%s", i, frame)
		}
	}
}
