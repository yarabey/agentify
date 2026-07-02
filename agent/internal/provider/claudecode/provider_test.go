package claudecode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yarabey/agentify/internal/bus"
)

// fakeClaudeBin — путь к собранному тестовому двойнику CLI `claude` (см.
// godoc testdata/fakeclaude), собирается один раз в TestMain на весь пакет.
var fakeClaudeBin string

// TestMain собирает testdata/fakeclaude в отдельный временный бинарь ПЕРЕД
// прогоном тестов (тикет 4.5: все committed-тесты обязаны говорить с
// тестовым двойником, а не с реальным платным CLI). testdata/ — каталог,
// который сам `go build`/`go vet ./...` игнорирует, поэтому собираем его
// явно указанным путём.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "fakeclaude-bin")
	if err != nil {
		fmt.Fprintln(os.Stderr, "provider_test: MkdirTemp:", err)
		os.Exit(1)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	bin := filepath.Join(dir, "fakeclaude")
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}

	buildCmd := exec.Command("go", "build", "-o", bin, "./testdata/fakeclaude")
	buildCmd.Stdout = os.Stderr
	buildCmd.Stderr = os.Stderr
	if err := buildCmd.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "provider_test: сборка testdata/fakeclaude:", err)
		os.Exit(1)
	}
	fakeClaudeBin = bin

	os.Exit(m.Run())
}

// fakePublisher — Publisher в памяти (см. godoc Publisher): фиксирует
// вызовы Enqueue для проверки в тестах, не поднимая bbolt (agent/internal/outbox).
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

// allowFunc — AllowChecker из функции, чтобы каждый тест задавал свою
// политику одной строкой, не заводя именованный тип на каждый случай.
type allowFunc func(toolName, command string) bool

func (f allowFunc) Allowed(toolName, command string) bool { return f(toolName, command) }

// scriptStep — зеркало testdata/fakeclaude.scriptStep (независимая копия:
// testdata/fakeclaude — отдельный `package main`, недоступный для импорта).
type scriptStep struct {
	RequestID string `json:"request_id"`
	ToolName  string `json:"tool_name"`
	Command   string `json:"command"`
	// SleepAfterApproveMs — см. testdata/fakeclaude.scriptStep (тикет 8.5):
	// сколько миллисекунд fakeclaude "выполняет" инструмент после allow,
	// прежде чем перейти к следующему шагу/result.
	SleepAfterApproveMs int `json:"sleep_after_approve_ms"`
}

// newTestProvider собирает Provider поверх fakeClaudeBin со сценарием
// script (передаётся двойнику через FAKE_CLAUDE_SCRIPT, см. godoc
// testdata/fakeclaude).
func newTestProvider(t *testing.T, allow AllowChecker, pub Publisher, script []scriptStep) *Provider {
	t.Helper()

	scriptJSON, err := json.Marshal(script)
	if err != nil {
		t.Fatalf("маршалинг сценария: %v", err)
	}

	cfg := Config{
		BinPath:       fakeClaudeBin,
		WorkDir:       t.TempDir(),
		AllowChecker:  allow,
		Publisher:     pub,
		TaskID:        "task-1",
		IntegrationID: "integration-1",
		Env:           []string{"FAKE_CLAUDE_SCRIPT=" + string(scriptJSON)},
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return p
}

// runAsync запускает p.Run в горутине и возвращает канал, в который придёт
// её результат — тестам нужно наблюдать за Run, не блокируя основной поток
// (Run может ждать Approve сколь угодно долго).
func runAsync(ctx context.Context, p *Provider, taskText string) <-chan error {
	done := make(chan error, 1)
	go func() {
		done <- p.Run(ctx, taskText)
	}()
	return done
}

const testTimeout = 10 * time.Second

// TestProvider_AllowlistedCommand_RunsWithoutApproval — приёмочный сценарий
// тикета 4.5 "команда из allowlist выполняется": Provider отвечает CLI
// allow немедленно, БЕЗ обращения к Publisher.
func TestProvider_AllowlistedCommand_RunsWithoutApproval(t *testing.T) {
	pub := &fakePublisher{}
	allowAll := allowFunc(func(string, string) bool { return true })

	p := newTestProvider(t, allowAll, pub, []scriptStep{
		{RequestID: "req-1", ToolName: "Bash", Command: "echo hi"},
	})

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	done := runAsync(ctx, p, "echo hi via Bash")
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("Run не завершился вовремя — allowlist-команда не должна требовать согласования")
	}

	if calls := pub.Calls(); len(calls) != 0 {
		t.Fatalf("Publisher.Enqueue не должен вызываться для allowlist-команды, вызван %d раз(а): %+v", len(calls), calls)
	}
}

// TestProvider_NonAllowlistedCommand_PublishesApprovalRequest — приёмочный
// сценарий тикета 4.5 "вне allowlist — уходит на согласование": Provider
// НЕ отвечает CLI сразу, публикует command_approval_request и ждёт Approve.
func TestProvider_NonAllowlistedCommand_PublishesApprovalRequest(t *testing.T) {
	pub := &fakePublisher{}
	denyAll := allowFunc(func(string, string) bool { return false })

	p := newTestProvider(t, denyAll, pub, []scriptStep{
		{RequestID: "req-2", ToolName: "Bash", Command: "rm -rf /"},
	})

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	done := runAsync(ctx, p, "делай осторожно")

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
	if payload.RequestID != "req-2" {
		t.Fatalf("RequestID = %q, want %q", payload.RequestID, "req-2")
	}
	if payload.Command != "rm -rf /" {
		t.Fatalf("Command = %q, want %q", payload.Command, "rm -rf /")
	}
	if payload.Reason == "" {
		t.Fatal("Reason пуст, ожидалось человекочитаемое объяснение")
	}

	// Run не должен завершиться, пока согласование не пришло.
	select {
	case err := <-done:
		t.Fatalf("Run завершился до Approve (err=%v) — запрос вне allowlist не должен проходить без согласования", err)
	case <-time.After(200 * time.Millisecond):
	}

	if err := p.Approve("req-2", "approve"); err != nil {
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
}

// TestProvider_Approve_AllowWritesAllowResponse проверяет, что решение
// "approve" доходит до подпроцесса как control_response{behavior:"allow"} —
// наблюдаем это через stderr fakeclaude (см. её godoc: она логирует туда
// разобранный behavior полученного ответа).
func TestProvider_Approve_AllowWritesAllowResponse(t *testing.T) {
	behavior := runSingleApprovalScenario(t, "req-allow", "approve")
	if behavior != "allow" {
		t.Fatalf("fakeclaude увидела behavior=%q, want %q", behavior, "allow")
	}
}

// TestProvider_Approve_RejectWritesDenyResponse проверяет, что решение
// "reject" доходит до подпроцесса как control_response{behavior:"deny"}.
func TestProvider_Approve_RejectWritesDenyResponse(t *testing.T) {
	behavior := runSingleApprovalScenario(t, "req-reject", "reject")
	if behavior != "deny" {
		t.Fatalf("fakeclaude увидела behavior=%q, want %q", behavior, "deny")
	}
}

// runSingleApprovalScenario прогоняет один вне-allowlist запрос через
// полный цикл Provider (публикация → Approve(decision)) и возвращает
// behavior, который fakeclaude реально прочитала из control_response,
// записанного Provider'ом в её stdin (fakeclaude логирует его в stderr,
// см. testdata/fakeclaude/main.go).
func runSingleApprovalScenario(t *testing.T, requestID, decision string) string {
	t.Helper()

	pub := &fakePublisher{}
	denyAll := allowFunc(func(string, string) bool { return false })

	var stderr synchronizedBuffer
	scriptJSON, err := json.Marshal([]scriptStep{
		{RequestID: requestID, ToolName: "Bash", Command: "echo hi"},
	})
	if err != nil {
		t.Fatalf("маршалинг сценария: %v", err)
	}
	cfg := Config{
		BinPath:       fakeClaudeBin,
		WorkDir:       t.TempDir(),
		AllowChecker:  denyAll,
		Publisher:     pub,
		TaskID:        "task-1",
		IntegrationID: "integration-1",
		Env:           []string{"FAKE_CLAUDE_SCRIPT=" + string(scriptJSON)},
		Stderr:        &stderr,
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	done := runAsync(ctx, p, "задача")
	waitForCalls(t, pub, 1)

	if err := p.Approve(requestID, decision); err != nil {
		t.Fatalf("Approve: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("Run не завершился вовремя после Approve")
	}

	want := "request_id=" + requestID
	if !waitForSubstring(&stderr, want, testTimeout) {
		t.Fatalf("stderr fakeclaude не содержит %q; stderr=%q", want, stderr.String())
	}
	return extractBehavior(stderr.String(), requestID)
}

// TestProvider_Approve_UnknownRequestID проверяет sentinel-ошибку ticket'а
// 4.5: неизвестный/уже разрешённый request_id — не паника, а ErrUnknownRequest.
func TestProvider_Approve_UnknownRequestID(t *testing.T) {
	pub := &fakePublisher{}
	p := newTestProvider(t, EmptyAllowChecker{}, pub, nil)

	if err := p.Approve("does-not-exist", "approve"); !errors.Is(err, ErrUnknownRequest) {
		t.Fatalf("Approve(unknown) = %v, want ErrUnknownRequest", err)
	}
}

// TestProvider_Approve_InvalidDecision проверяет, что decision вне
// "approve"/"reject" (protocol.md §4) — понятная ошибка, а не тихий no-op.
func TestProvider_Approve_InvalidDecision(t *testing.T) {
	pub := &fakePublisher{}
	p := newTestProvider(t, EmptyAllowChecker{}, pub, nil)

	if err := p.Approve("whatever", "maybe"); !errors.Is(err, ErrInvalidDecision) {
		t.Fatalf("Approve(invalid decision) = %v, want ErrInvalidDecision", err)
	}
}

// TestProvider_Close_TerminatesSubprocess проверяет чистую остановку
// подпроцесса (тикет 4.5, задел под 8.4): Close должен разблокировать Run
// без зависших процессов, даже если CLI ждёт согласования, которое так и
// не пришло.
//
// Этот тест использует denyAll — запрос уходит в pending, ни один вызов
// инструмента не получает allow, поэтому criticalInFlight никогда не
// становится true (тикет 8.5). Тем самым он же служит регрессионным
// доказательством того, что при criticalInFlight==false поведение Close
// НЕ изменилось этим тикетом: немедленный kill, как и раньше.
func TestProvider_Close_TerminatesSubprocess(t *testing.T) {
	pub := &fakePublisher{}
	denyAll := allowFunc(func(string, string) bool { return false })

	p := newTestProvider(t, denyAll, pub, []scriptStep{
		{RequestID: "req-hang", ToolName: "Bash", Command: "sleep forever"},
	})

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	done := runAsync(ctx, p, "задача")
	waitForCalls(t, pub, 1)

	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case <-done:
		// Run вернулся (с любой ошибкой — подпроцесс убит, это ожидаемо) —
		// значит, подпроцесс не завис и был корректно дождан (cmd.Wait
		// внутри Run), без зомби-процесса.
	case <-time.After(testTimeout):
		t.Fatal("Run не завершился вовремя после Close — подозрение на зависший подпроцесс")
	}
}

// TestProvider_ContextCancellation_StopsSubprocess проверяет, что отмена
// ctx (задел под тикет 8.4 "отмена долетает до машины") останавливает
// подпроцесс и Run возвращает ctx.Err().
func TestProvider_ContextCancellation_StopsSubprocess(t *testing.T) {
	pub := &fakePublisher{}
	denyAll := allowFunc(func(string, string) bool { return false })

	p := newTestProvider(t, denyAll, pub, []scriptStep{
		{RequestID: "req-cancel", ToolName: "Bash", Command: "sleep forever"},
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := runAsync(ctx, p, "задача")
	waitForCalls(t, pub, 1)

	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run после отмены ctx = %v, want context.Canceled", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("Run не завершился вовремя после отмены ctx")
	}
}

// TestProvider_Close_DuringCriticalOperation_DefersKillAndWarns — приёмочный
// сценарий тикета 8.5 (FR E6, Gherkin §8 «Приоритет сохранности данных при
// отмене»): Close, вызванный пока разрешённый (allow) вызов инструмента ещё
// выполняется (criticalInFlight), НЕ убивает подпроцесс немедленно, а
// публикует agent_progress-предупреждение и откладывает остановку до
// безопасного завершения текущей операции.
func TestProvider_Close_DuringCriticalOperation_DefersKillAndWarns(t *testing.T) {
	pub := &fakePublisher{}
	allowAll := allowFunc(func(string, string) bool { return true })

	p := newTestProvider(t, allowAll, pub, []scriptStep{
		{RequestID: "req-critical", ToolName: "Bash", Command: "echo critical", SleepAfterApproveMs: 500},
	})

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	done := runAsync(ctx, p, "критическая операция")

	// Allowlist-путь синхронен и почти мгновенен (Provider отвечает allow
	// сам, без обращения к Publisher) — небольшая пауза достаточна, чтобы
	// fakeclaude успела получить control_response и уйти в
	// SleepAfterApproveMs, а Provider успел выставить criticalInFlight.
	time.Sleep(100 * time.Millisecond)

	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Run НЕ должен завершиться немедленно — критическая операция ещё не
	// доиграла свой SleepAfterApproveMs.
	select {
	case err := <-done:
		t.Fatalf("Run завершился сразу после Close (err=%v) — критическая операция не должна прерываться немедленно (FR E6)", err)
	case <-time.After(200 * time.Millisecond):
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

	// В итоге, после того как fakeclaude "доиграет" sleep и закроет stdout,
	// Run должен всё же завершиться (отложенная остановка доводится до
	// конца, см. Provider.Run про fallback-Kill после readLoop) — без
	// проверки конкретной ошибки, важен сам факт, что процесс не завис.
	select {
	case <-done:
	case <-time.After(testTimeout):
		t.Fatal("Run не завершился вовремя — подозрение, что отложенная остановка не была доведена до конца")
	}
}

// TestNew_ValidatesConfig проверяет, что конструктор не паникует на
// невалидном конфиге, а возвращает понятные sentinel-ошибки (та же
// дисциплина, что agent/internal/wsclient.New).
func TestNew_ValidatesConfig(t *testing.T) {
	base := Config{
		BinPath:       fakeClaudeBin,
		Publisher:     &fakePublisher{},
		TaskID:        "task-1",
		IntegrationID: "integration-1",
	}

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

// waitForCalls ждёт, пока pub не зафиксирует ровно want вызовов Enqueue
// (или больше — тогда тест ниже сам сравнит точное число), не дольше
// testTimeout — poll вместо sleep, т.к. публикация происходит в горутине
// Run.
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

// synchronizedBuffer — bytes.Buffer с мьютексом: cmd.Stderr пишет из
// горутины подпроцесса конкурентно с тестом, читающим содержимое.
type synchronizedBuffer struct {
	mu  sync.Mutex
	buf []byte
}

func (b *synchronizedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, p...)
	return len(p), nil
}

func (b *synchronizedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.buf)
}

// waitForSubstring ждёт появления substr в буфере, не дольше timeout.
func waitForSubstring(b *synchronizedBuffer, substr string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if strings.Contains(b.String(), substr) {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// extractBehavior парсит строку "fakeclaude: request_id=<id> behavior=<b>"
// из накопленного stderr fakeclaude (см. testdata/fakeclaude/main.go) и
// возвращает <b> для строки с данным requestID.
func extractBehavior(stderr, requestID string) string {
	marker := "request_id=" + requestID + " behavior="
	idx := strings.Index(stderr, marker)
	if idx < 0 {
		return ""
	}
	rest := stderr[idx+len(marker):]
	if end := strings.IndexByte(rest, '\n'); end >= 0 {
		rest = rest[:end]
	}
	return rest
}
