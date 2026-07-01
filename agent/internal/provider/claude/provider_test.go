package claude

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yarabey/agentify/internal/bus"
)

const testTimeout = 10 * time.Second

// fakePublisher — Publisher в памяти (см. claudecode/provider_test.go —
// тот же приём): фиксирует вызовы Enqueue для проверки в тестах, не поднимая
// bbolt.
type fakePublisher struct {
	mu    sync.Mutex
	calls []bus.Envelope
}

func (f *fakePublisher) Enqueue(env bus.Envelope) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, env)
	return nil
}

func (f *fakePublisher) Calls() []bus.Envelope {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]bus.Envelope, len(f.calls))
	copy(out, f.calls)
	return out
}

// allowFunc — AllowChecker из функции (см. claudecode/provider_test.go).
type allowFunc func(toolName, command string) bool

func (f allowFunc) Allowed(toolName, command string) bool { return f(toolName, command) }

// waitForCalls ждёт, пока pub не зафиксирует хотя бы want вызовов Enqueue, не
// дольше testTimeout (poll вместо sleep — публикация происходит в горутине
// Run).
func waitForCalls(t *testing.T, pub *fakePublisher, want int) {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for time.Now().Before(deadline) {
		if len(pub.Calls()) >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("не дождались %d вызовов Publisher.Enqueue, получено %d", want, len(pub.Calls()))
}

// recordingServer — фейковый /v1/messages: отдаёт заранее заданные
// JSON-ответы по очереди, по одному на входящий запрос, и запоминает разобранные
// запросы для проверки в тестах (аналогично testdata/fakeclaude в claudecode,
// но здесь это просто httptest-хендлер, без подпроцесса).
type recordingServer struct {
	mu        sync.Mutex
	responses [][]byte
	idx       int
	requests  []messagesRequest
}

func newRecordingServer(t *testing.T, responses ...string) (*httptest.Server, *recordingServer) {
	t.Helper()
	rs := &recordingServer{}
	for _, r := range responses {
		rs.responses = append(rs.responses, []byte(r))
	}
	srv := httptest.NewServer(http.HandlerFunc(rs.handle))
	t.Cleanup(srv.Close)
	return srv, rs
}

func (s *recordingServer) handle(w http.ResponseWriter, r *http.Request) {
	var req messagesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	s.requests = append(s.requests, req)
	idx := s.idx
	s.idx++
	var resp []byte
	if idx < len(s.responses) {
		resp = s.responses[idx]
	}
	s.mu.Unlock()

	if resp == nil {
		http.Error(w, fmt.Sprintf("recordingServer: нет заготовленного ответа для запроса #%d", idx), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(resp)
}

func (s *recordingServer) requestCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.requests)
}

func (s *recordingServer) requestAt(i int) messagesRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requests[i]
}

// textResponse строит JSON-тело ответа Messages API без tool_use
// (stop_reason=="end_turn") — сигнал завершения задачи.
func textResponse(text string) string {
	body, err := json.Marshal(messagesResponse{
		Content:    []contentBlock{{Type: "text", Text: text}},
		StopReason: "end_turn",
	})
	if err != nil {
		panic(err)
	}
	return string(body)
}

// toolUseResponse строит JSON-тело ответа Messages API с одним tool_use
// блоком (stop_reason=="tool_use").
func toolUseResponse(id, toolName, command string) string {
	input, err := json.Marshal(map[string]string{"command": command})
	if err != nil {
		panic(err)
	}
	body, err := json.Marshal(messagesResponse{
		Content:    []contentBlock{{Type: "tool_use", ID: id, Name: toolName, Input: input}},
		StopReason: "tool_use",
	})
	if err != nil {
		panic(err)
	}
	return string(body)
}

// newTestProvider собирает Provider поверх httptest.Server srv.
func newTestProvider(t *testing.T, srv *httptest.Server, allow claudecodeAllowChecker, pub *fakePublisher, workDir string) *Provider {
	t.Helper()
	if workDir == "" {
		workDir = t.TempDir()
	}
	p, err := New(Config{
		APIKey:        "test-key",
		BaseURL:       srv.URL,
		WorkDir:       workDir,
		AllowChecker:  allow,
		Publisher:     pub,
		TaskID:        "task-1",
		IntegrationID: "integration-1",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return p
}

// runAsync запускает p.Run в горутине и возвращает канал с её результатом —
// тесты не должны блокироваться, ожидая Approve (см. claudecode/provider_test.go).
func runAsync(ctx context.Context, p *Provider, taskText string) <-chan error {
	done := make(chan error, 1)
	go func() {
		done <- p.Run(ctx, taskText)
	}()
	return done
}

// TestRun_HappyPath_NoToolUse — приёмочный сценарий "модель завершает задачу
// без вызова инструмента": один запрос/ответ, Run возвращает nil.
func TestRun_HappyPath_NoToolUse(t *testing.T) {
	srv, rs := newRecordingServer(t, textResponse("задача выполнена"))
	pub := &fakePublisher{}
	denyAll := allowFunc(func(string, string) bool { return false })

	p := newTestProvider(t, srv, denyAll, pub, "")

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	if err := p.Run(ctx, "сделай что-нибудь"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if rs.requestCount() != 1 {
		t.Fatalf("сделано %d запросов, ожидался 1", rs.requestCount())
	}
	if len(pub.Calls()) != 0 {
		t.Fatalf("Publisher.Enqueue не должен вызываться, вызван %d раз(а)", len(pub.Calls()))
	}
}

// TestRun_AllowlistedCommand_ExecutesWithoutApproval — приёмочный сценарий
// "команда из allowlist выполняется": Provider выполняет команду реально
// (echo hello), без обращения к Publisher, и отправляет второй запрос с
// tool_result.
func TestRun_AllowlistedCommand_ExecutesWithoutApproval(t *testing.T) {
	srv, rs := newRecordingServer(t,
		toolUseResponse("tu-1", "bash", "echo hello"),
		textResponse("готово"),
	)
	pub := &fakePublisher{}
	allowAll := allowFunc(func(string, string) bool { return true })

	p := newTestProvider(t, srv, allowAll, pub, "")

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	if err := p.Run(ctx, "выполни echo hello"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if rs.requestCount() != 2 {
		t.Fatalf("сделано %d запросов, ожидалось 2", rs.requestCount())
	}
	if len(pub.Calls()) != 0 {
		t.Fatalf("Publisher.Enqueue не должен вызываться для allowlist-команды, вызван %d раз(а)", len(pub.Calls()))
	}

	req2 := rs.requestAt(1)
	toolResult := findToolResult(t, req2, "tu-1")
	if toolResult.IsError {
		t.Fatalf("tool_result.is_error = true, ожидался false")
	}
	if !containsSubstring(toolResult.Content, "hello") {
		t.Fatalf("tool_result.content = %q, ожидалось содержание %q", toolResult.Content, "hello")
	}
}

// TestRun_NonAllowlistedCommand_PublishesApprovalRequest_ThenApprove —
// приёмочный сценарий "команда вне allowlist уходит на согласование": Run не
// продолжается, пока не придёт Approve(id, "approve"); после approve
// команда реально выполняется.
func TestRun_NonAllowlistedCommand_PublishesApprovalRequest_ThenApprove(t *testing.T) {
	srv, rs := newRecordingServer(t,
		toolUseResponse("tu-2", "bash", "echo outside-allowlist"),
		textResponse("готово"),
	)
	pub := &fakePublisher{}
	denyAll := allowFunc(func(string, string) bool { return false })

	p := newTestProvider(t, srv, denyAll, pub, "")

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	done := runAsync(ctx, p, "выполни команду вне allowlist")

	waitForCalls(t, pub, 1)
	calls := pub.Calls()
	if calls[0].Type != bus.MessageTypeCommandApprovalRequest {
		t.Fatalf("Type = %q, want %q", calls[0].Type, bus.MessageTypeCommandApprovalRequest)
	}
	if calls[0].TaskID == nil || *calls[0].TaskID != "task-1" {
		t.Fatalf("TaskID = %v, want ptr to task-1", calls[0].TaskID)
	}
	var payload bus.CommandApprovalRequestPayload
	if err := json.Unmarshal(calls[0].Payload, &payload); err != nil {
		t.Fatalf("демаршалинг payload: %v", err)
	}
	if payload.RequestID != "tu-2" {
		t.Fatalf("RequestID = %q, want %q", payload.RequestID, "tu-2")
	}
	if payload.Command != "echo outside-allowlist" {
		t.Fatalf("Command = %q, want %q", payload.Command, "echo outside-allowlist")
	}
	if payload.Reason == "" {
		t.Fatal("Reason пуст, ожидалось человекочитаемое объяснение")
	}

	// Run не должен завершиться до Approve.
	select {
	case err := <-done:
		t.Fatalf("Run завершился до Approve (err=%v) — запрос вне allowlist не должен проходить без согласования", err)
	case <-time.After(150 * time.Millisecond):
	}

	if err := p.Approve("tu-2", "approve"); err != nil {
		t.Fatalf("Approve: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run после Approve: %v", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("Run не завершился вовремя после Approve")
	}

	if rs.requestCount() != 2 {
		t.Fatalf("сделано %d запросов, ожидалось 2", rs.requestCount())
	}
	toolResult := findToolResult(t, rs.requestAt(1), "tu-2")
	if toolResult.IsError {
		t.Fatalf("tool_result.is_error = true после approve, ожидался false")
	}
	if !containsSubstring(toolResult.Content, "outside-allowlist") {
		t.Fatalf("tool_result.content = %q, ожидалось содержание %q", toolResult.Content, "outside-allowlist")
	}
}

// TestRun_RejectDecision_DoesNotExecuteCommand — Approve(..., "reject"):
// Provider шлёт tool_result с is_error=true, НЕ выполняя команду локально
// (файл, который команда создала бы, не появляется).
func TestRun_RejectDecision_DoesNotExecuteCommand(t *testing.T) {
	workDir := t.TempDir()
	marker := filepath.Join(workDir, "marker.txt")

	srv, rs := newRecordingServer(t,
		toolUseResponse("tu-3", "bash", "touch "+marker),
		textResponse("готово"),
	)
	pub := &fakePublisher{}
	denyAll := allowFunc(func(string, string) bool { return false })

	p := newTestProvider(t, srv, denyAll, pub, workDir)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	done := runAsync(ctx, p, "создай файл-маркер")
	waitForCalls(t, pub, 1)

	if err := p.Approve("tu-3", "reject"); err != nil {
		t.Fatalf("Approve: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run после reject: %v", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("Run не завершился вовремя после Approve(reject)")
	}

	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("файл-маркер %q существует — отклонённая команда была выполнена", marker)
	}

	if rs.requestCount() != 2 {
		t.Fatalf("сделано %d запросов, ожидалось 2", rs.requestCount())
	}
	toolResult := findToolResult(t, rs.requestAt(1), "tu-3")
	if !toolResult.IsError {
		t.Fatal("tool_result.is_error = false после reject, ожидался true")
	}
	if toolResult.Content == "" {
		t.Fatal("tool_result.content пуст после reject, ожидался текст отказа")
	}
}

// TestApprove_UnknownRequestID проверяет sentinel-ошибку: неизвестный/уже
// разрешённый request_id — не паника, а ErrUnknownRequest.
func TestApprove_UnknownRequestID(t *testing.T) {
	srv, _ := newRecordingServer(t)
	pub := &fakePublisher{}
	p := newTestProvider(t, srv, claudeEmptyAllowChecker{}, pub, "")

	if err := p.Approve("does-not-exist", "approve"); !errors.Is(err, ErrUnknownRequest) {
		t.Fatalf("Approve(unknown) = %v, want ErrUnknownRequest", err)
	}
}

// TestApprove_InvalidDecision проверяет, что decision вне
// "approve"/"reject" — понятная ошибка, а не тихий no-op.
func TestApprove_InvalidDecision(t *testing.T) {
	srv, _ := newRecordingServer(t)
	pub := &fakePublisher{}
	p := newTestProvider(t, srv, claudeEmptyAllowChecker{}, pub, "")

	if err := p.Approve("whatever", "maybe"); !errors.Is(err, ErrInvalidDecision) {
		t.Fatalf("Approve(invalid decision) = %v, want ErrInvalidDecision", err)
	}
}

// TestClose_DuringCriticalOperation_DefersCancellation — приёмочный сценарий
// тикета 8.5 (FR E6), реализованный так же строго для этого провайдера, как и
// для claudecode.Provider: Close, вызванный пока разрешённая команда ещё
// выполняется (criticalInFlight), НЕ отменяет context немедленно — вместо
// этого публикует agent_progress-предупреждение, а отмена доводится до конца
// только после того, как команда сама естественно завершится.
func TestClose_DuringCriticalOperation_DefersCancellation(t *testing.T) {
	srv, _ := newRecordingServer(t,
		toolUseResponse("tu-critical", "bash", "sleep 0.4"),
	)
	pub := &fakePublisher{}
	allowAll := allowFunc(func(string, string) bool { return true })

	p := newTestProvider(t, srv, allowAll, pub, "")

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	done := runAsync(ctx, p, "критическая операция")

	// Дадим Run время дойти до executeCommand и выставить criticalInFlight
	// (allowlist-путь синхронен, но exec.Command должен успеть стартовать).
	time.Sleep(100 * time.Millisecond)

	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Run НЕ должен завершиться немедленно — критическая операция (sleep
	// 0.4) ещё не доиграла.
	select {
	case err := <-done:
		t.Fatalf("Run завершился сразу после Close (err=%v) — критическая операция не должна прерываться немедленно (FR E6)", err)
	case <-time.After(150 * time.Millisecond):
	}

	waitForCalls(t, pub, 1)
	calls := pub.Calls()
	var found *bus.Envelope
	for i := range calls {
		if calls[i].Type == bus.MessageTypeAgentProgress {
			found = &calls[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("не найден вызов Publisher.Enqueue с Type=%q среди %+v", bus.MessageTypeAgentProgress, calls)
	}
	var payload bus.AgentProgressPayload
	if err := json.Unmarshal(found.Payload, &payload); err != nil {
		t.Fatalf("демаршалинг AgentProgressPayload: %v", err)
	}
	if payload.Text == "" {
		t.Fatal("AgentProgressPayload.Text пуст, ожидалось предупреждение")
	}

	// В итоге, после того как sleep 0.4 доиграет, Run должен вернуть
	// управление (отменённый ctx) — без проверки конкретной ошибки, важен сам
	// факт, что цикл не завис.
	select {
	case <-done:
	case <-time.After(testTimeout):
		t.Fatal("Run не завершился вовремя — подозрение, что отложенная отмена не была доведена до конца")
	}
}

// TestNew_ValidatesConfig проверяет, что конструктор не паникует на
// невалидном конфиге, а возвращает понятные sentinel-ошибки (та же
// дисциплина, что claudecode.New).
func TestNew_ValidatesConfig(t *testing.T) {
	base := Config{
		APIKey:        "test-key",
		Publisher:     &fakePublisher{},
		TaskID:        "task-1",
		IntegrationID: "integration-1",
	}

	t.Run("empty api key", func(t *testing.T) {
		cfg := base
		cfg.APIKey = ""
		if _, err := New(cfg); !errors.Is(err, ErrEmptyAPIKey) {
			t.Fatalf("New = %v, want ErrEmptyAPIKey", err)
		}
	})
	t.Run("nil publisher", func(t *testing.T) {
		cfg := base
		cfg.Publisher = nil
		if _, err := New(cfg); !errors.Is(err, ErrNilPublisher) {
			t.Fatalf("New = %v, want ErrNilPublisher", err)
		}
	})
	t.Run("empty task id", func(t *testing.T) {
		cfg := base
		cfg.TaskID = ""
		if _, err := New(cfg); !errors.Is(err, ErrEmptyTaskID) {
			t.Fatalf("New = %v, want ErrEmptyTaskID", err)
		}
	})
	t.Run("empty integration id", func(t *testing.T) {
		cfg := base
		cfg.IntegrationID = ""
		if _, err := New(cfg); !errors.Is(err, ErrEmptyIntegrationID) {
			t.Fatalf("New = %v, want ErrEmptyIntegrationID", err)
		}
	})
}

// --- вспомогательные утилиты тестов ---

// claudecodeAllowChecker — псевдоним локального имени для интерфейса,
// принимаемого newTestProvider, чтобы не импортировать claudecode только
// ради имени типа в тестовом хелпере (см. Config.AllowChecker —
// claudecode.AllowChecker) — allowFunc и claudeEmptyAllowChecker в этом файле
// удовлетворяют ему структурно.
type claudecodeAllowChecker interface {
	Allowed(toolName, command string) bool
}

// claudeEmptyAllowChecker — локальный аналог claudecode.EmptyAllowChecker
// (allowlist "по умолчанию пуст") для тестов, которым не нужен реальный Run —
// не тянем импорт claudecode только ради этого нулевого значения.
type claudeEmptyAllowChecker struct{}

func (claudeEmptyAllowChecker) Allowed(string, string) bool { return false }

// findToolResult ищет tool_result блок с данным ToolUseID среди сообщений
// запроса req (ожидается в последнем user-сообщении) — паникует тестом, если
// не найден.
func findToolResult(t *testing.T, req messagesRequest, toolUseID string) contentBlock {
	t.Helper()
	for _, msg := range req.Messages {
		for _, block := range msg.Content {
			if block.Type == "tool_result" && block.ToolUseID == toolUseID {
				return block
			}
		}
	}
	t.Fatalf("tool_result с tool_use_id=%q не найден среди сообщений запроса: %+v", toolUseID, req.Messages)
	return contentBlock{}
}

func containsSubstring(s, substr string) bool {
	return strings.Contains(s, substr)
}
