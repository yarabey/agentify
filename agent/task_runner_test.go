// Юнит-тесты приёма задачи (тикет 5.4, FR E1, agent/task_runner.go) — без
// реального провайдера claude-code/подпроцесса: newProvider подменяется
// фабрикой fakeRunner, тот же приём, что и fakeEventSender в main_test.go.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/yarabey/agentify/agent/internal/provider/claudecode"
	"github.com/yarabey/agentify/internal/bus"
)

// fakeRunner — фейковая реализация taskRunner: потокобезопасно считает вызовы
// Run и запоминает переданный текст задачи. Если block не nil, Run
// блокируется до его закрытия (или отмены ctx) — используется, чтобы
// сымитировать «ещё выполняющуюся» задачу в тестах дедупликации.
type fakeRunner struct {
	mu    sync.Mutex
	calls int
	texts []string
	block chan struct{}
	err   error
}

func (f *fakeRunner) Run(ctx context.Context, text string) error {
	f.mu.Lock()
	f.calls++
	f.texts = append(f.texts, text)
	f.mu.Unlock()

	if f.block != nil {
		select {
		case <-f.block:
		case <-ctx.Done():
		}
	}
	return f.err
}

func (f *fakeRunner) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// fakePublisher — заглушка claudecode.Publisher: в тестах этого файла
// newProvider подменяется, поэтому Enqueue реально никогда не вызывается, но
// поле claudecode.Config.Publisher должно быть чем-то заполнено при реальном
// вызове newProvider (не в этих тестах).
type fakePublisher struct{}

func (fakePublisher) Enqueue(bus.Envelope) error { return nil }

// taskAssignedEnvelope собирает тестовый конверт type=="task_assigned" с
// заданным task_id и текстом задачи.
func taskAssignedEnvelope(t *testing.T, taskID, text string) bus.Envelope {
	t.Helper()
	payload, err := json.Marshal(bus.TaskAssignedPayload{Text: text})
	if err != nil {
		t.Fatalf("marshal TaskAssignedPayload: %v", err)
	}
	return bus.Envelope{
		MessageID:       bus.NewMessageID(),
		TaskID:          &taskID,
		IntegrationID:   "44444444-4444-4444-4444-444444444444",
		Type:            bus.MessageTypeTaskAssigned,
		Ts:              time.Now().UTC().Format(time.RFC3339),
		ProtocolVersion: bus.ProtocolVersion,
		Payload:         payload,
	}
}

// waitForCount ждёт, пока f вернёт значение >= want, либо истечёт timeout.
func waitForCount(t *testing.T, timeout time.Duration, want int, f func() int) {
	t.Helper()
	deadline := time.After(timeout)
	for f() < want {
		select {
		case <-deadline:
			t.Fatalf("не дождались count() >= %d за %v (получено %d)", want, timeout, f())
		case <-time.After(2 * time.Millisecond):
		}
	}
}

func configuredCfg() config {
	var cfg config
	cfg.Providers = []string{"claude-code"}
	cfg.ClaudeCodeAPIKey = "test-key"
	cfg.IntegrationUUID = "44444444-4444-4444-4444-444444444444"
	return cfg
}

// TestTaskAcceptor_OnTaskAssigned_HappyPath_StartsRunnerAndSendsTaskAccepted —
// нормальный путь: newProvider вызывается один раз, Run стартует в фоне,
// task_accepted синхронно уходит в outbox ДО завершения самой задачи.
func TestTaskAcceptor_OnTaskAssigned_HappyPath_StartsRunnerAndSendsTaskAccepted(t *testing.T) {
	runner := &fakeRunner{}
	orig := newProvider
	newProvider = func(claudecode.Config) (taskRunner, error) { return runner, nil }
	defer func() { newProvider = orig }()

	sender := &fakeEventSender{}
	acceptor, err := newTaskAcceptor(configuredCfg(), fakePublisher{}, discardLogger())
	if err != nil {
		t.Fatalf("newTaskAcceptor: %v", err)
	}
	acceptor.sender = sender

	env := taskAssignedEnvelope(t, "11111111-1111-1111-1111-111111111111", "сделай что-нибудь")
	if err := acceptor.onTaskAssigned(context.Background(), env); err != nil {
		t.Fatalf("onTaskAssigned вернул ошибку: %v", err)
	}

	if sender.count() != 1 {
		t.Fatalf("SendEvent вызван %d раз(а), ожидался 1", sender.count())
	}
	sent := sender.last()
	if sent.Type != bus.MessageTypeTaskAccepted {
		t.Fatalf("type=%q, ожидался %q", sent.Type, bus.MessageTypeTaskAccepted)
	}
	if sent.TaskID == nil || *sent.TaskID != "11111111-1111-1111-1111-111111111111" {
		t.Fatalf("task_id=%v, ожидался %q", sent.TaskID, "11111111-1111-1111-1111-111111111111")
	}

	waitForCount(t, 2*time.Second, 1, runner.count)
	runner.mu.Lock()
	texts := runner.texts
	runner.mu.Unlock()
	if len(texts) != 1 || texts[0] != "сделай что-нибудь" {
		t.Fatalf("Run получил texts=%v, ожидался ['сделай что-нибудь']", texts)
	}
}

// TestTaskAcceptor_OnTaskAssigned_DuplicateTaskID_DoesNotStartSecondRun —
// повторная доставка того же task_id, пока задача ещё выполняется, НЕ
// запускает второй Run, но task_accepted шлётся снова и hook возвращает nil.
func TestTaskAcceptor_OnTaskAssigned_DuplicateTaskID_DoesNotStartSecondRun(t *testing.T) {
	block := make(chan struct{})
	runner := &fakeRunner{block: block}
	orig := newProvider
	newProvider = func(claudecode.Config) (taskRunner, error) { return runner, nil }
	defer func() { newProvider = orig }()

	sender := &fakeEventSender{}
	acceptor, err := newTaskAcceptor(configuredCfg(), fakePublisher{}, discardLogger())
	if err != nil {
		t.Fatalf("newTaskAcceptor: %v", err)
	}
	acceptor.sender = sender

	env := taskAssignedEnvelope(t, "22222222-2222-2222-2222-222222222222", "долгая задача")
	if err := acceptor.onTaskAssigned(context.Background(), env); err != nil {
		t.Fatalf("onTaskAssigned (первый вызов) вернул ошибку: %v", err)
	}
	waitForCount(t, 2*time.Second, 1, runner.count)

	// Повторная доставка того же task_assigned (тот же task_id) — задача ещё
	// не завершилась (block не закрыт).
	if err := acceptor.onTaskAssigned(context.Background(), env); err != nil {
		t.Fatalf("onTaskAssigned (повтор) вернул ошибку: %v", err)
	}

	if got := runner.count(); got != 1 {
		t.Fatalf("Run вызван %d раз(а) после дублирующей доставки, ожидался 1 (без повторного запуска)", got)
	}
	if sender.count() != 2 {
		t.Fatalf("SendEvent вызван %d раз(а), ожидалось 2 (task_accepted шлётся при каждой доставке)", sender.count())
	}

	close(block) // отпускаем горутину, чтобы не утекала за пределы теста
	waitForCount(t, 2*time.Second, 1, func() int {
		acceptor.mu.Lock()
		defer acceptor.mu.Unlock()
		if _, ok := acceptor.active["22222222-2222-2222-2222-222222222222"]; ok {
			return 0
		}
		return 1
	})
}

// TestTaskAcceptor_OnTaskAssigned_NoProviderConfigured_ReturnsError — если
// claude-code не заявлен в AGENT_PROVIDERS или не задан
// AGENT_CLAUDE_CODE_API_KEY, hook возвращает errNoProvider и НЕ шлёт
// task_accepted (нет ack на сам task_assigned — агент получит редоставку).
func TestTaskAcceptor_OnTaskAssigned_NoProviderConfigured_ReturnsError(t *testing.T) {
	var cfg config
	cfg.IntegrationUUID = "44444444-4444-4444-4444-444444444444"
	// Providers/ClaudeCodeAPIKey намеренно не заданы.

	sender := &fakeEventSender{}
	acceptor, err := newTaskAcceptor(cfg, fakePublisher{}, discardLogger())
	if err != nil {
		t.Fatalf("newTaskAcceptor: %v", err)
	}
	acceptor.sender = sender

	env := taskAssignedEnvelope(t, "33333333-3333-3333-3333-333333333333", "задача без провайдера")
	err = acceptor.onTaskAssigned(context.Background(), env)
	if !errors.Is(err, errNoProvider) {
		t.Fatalf("onTaskAssigned вернул %v, ожидался errNoProvider", err)
	}
	if sender.count() != 0 {
		t.Fatalf("SendEvent вызван %d раз(а), ожидалось 0 (ack на task_assigned не должен уйти)", sender.count())
	}
}

// TestTaskAcceptor_OnTaskAssigned_MissingTaskID_ReturnsError — конверт
// task_assigned без task_id — невалидная команда, hook возвращает ошибку без
// паники и без отправки task_accepted.
func TestTaskAcceptor_OnTaskAssigned_MissingTaskID_ReturnsError(t *testing.T) {
	sender := &fakeEventSender{}
	acceptor, err := newTaskAcceptor(configuredCfg(), fakePublisher{}, discardLogger())
	if err != nil {
		t.Fatalf("newTaskAcceptor: %v", err)
	}
	acceptor.sender = sender

	env := taskAssignedEnvelope(t, "not-used", "текст")
	env.TaskID = nil

	if err := acceptor.onTaskAssigned(context.Background(), env); err == nil {
		t.Fatal("onTaskAssigned вернул nil при отсутствующем task_id, ожидалась ошибка")
	}
	if sender.count() != 0 {
		t.Fatalf("SendEvent вызван %d раз(а), ожидалось 0", sender.count())
	}
}

// TestTaskAcceptor_OnTaskAssigned_InvalidPayload_ReturnsError — payload,
// который не разбирается как bus.TaskAssignedPayload, — ошибка без ack.
func TestTaskAcceptor_OnTaskAssigned_InvalidPayload_ReturnsError(t *testing.T) {
	sender := &fakeEventSender{}
	acceptor, err := newTaskAcceptor(configuredCfg(), fakePublisher{}, discardLogger())
	if err != nil {
		t.Fatalf("newTaskAcceptor: %v", err)
	}
	acceptor.sender = sender

	env := taskAssignedEnvelope(t, "55555555-5555-5555-5555-555555555555", "неважно")
	env.Payload = json.RawMessage(`{"text": 123}`) // текст не строка — не разбирается

	if err := acceptor.onTaskAssigned(context.Background(), env); err == nil {
		t.Fatal("onTaskAssigned вернул nil при невалидном payload, ожидалась ошибка")
	}
	if sender.count() != 0 {
		t.Fatalf("SendEvent вызван %d раз(а), ожидалось 0", sender.count())
	}
}

// TestBuildRunner_PassesConfiguredAllowChecker — тикет 6.3: buildRunner
// передаёт в claudecode.Config именно тот AllowChecker, что построен
// newTaskAcceptor из cfg.AllowlistPatterns (AGENT_ALLOWLIST_PATTERNS), а не
// nil/EmptyAllowChecker.
func TestBuildRunner_PassesConfiguredAllowChecker(t *testing.T) {
	var capturedCfg claudecode.Config
	orig := newProvider
	newProvider = func(cfg claudecode.Config) (taskRunner, error) {
		capturedCfg = cfg
		return &fakeRunner{}, nil
	}
	defer func() { newProvider = orig }()

	cfg := configuredCfg()
	cfg.AllowlistPatterns = []string{"Bash(git *)"}

	acceptor, err := newTaskAcceptor(cfg, fakePublisher{}, discardLogger())
	if err != nil {
		t.Fatalf("newTaskAcceptor: %v", err)
	}

	if _, err := acceptor.buildRunner("some-task-id"); err != nil {
		t.Fatalf("buildRunner вернул ошибку: %v", err)
	}

	if capturedCfg.AllowChecker == nil {
		t.Fatal("claudecode.Config.AllowChecker == nil, ожидался сконфигурированный PatternAllowChecker")
	}
	if !capturedCfg.AllowChecker.Allowed("Bash", "git status") {
		t.Fatal(`AllowChecker.Allowed("Bash", "git status") = false, ожидался true`)
	}
	if capturedCfg.AllowChecker.Allowed("Bash", "rm -rf /") {
		t.Fatal(`AllowChecker.Allowed("Bash", "rm -rf /") = true, ожидался false`)
	}
}

// TestNewTaskAcceptor_InvalidAllowlistPattern_ReturnsError — невалидный
// паттерн в AGENT_ALLOWLIST_PATTERNS должен фейлить конструктор
// newTaskAcceptor целиком (см. годок newTaskAcceptor), а не молча
// игнорироваться.
func TestNewTaskAcceptor_InvalidAllowlistPattern_ReturnsError(t *testing.T) {
	cfg := configuredCfg()
	cfg.AllowlistPatterns = []string{"invalid"}

	acceptor, err := newTaskAcceptor(cfg, fakePublisher{}, discardLogger())
	if err == nil {
		t.Fatal("newTaskAcceptor вернул nil при невалидном паттерне allowlist, ожидалась ошибка")
	}
	if acceptor != nil {
		t.Fatal("newTaskAcceptor вернул не-nil acceptor при ошибке")
	}
}
