package main

// Unit-тесты бизнес-логики действий из Telegram (тикет 10.3, FR D1): постановка
// задачи (обычным текстом и `/task`), отмена (`/cancel`), ответ на вопрос
// (`/answer`). Проверяют все ветки без реального telebot.Context/сети через
// фейковый telegramActor (см. годок bot/task.go про разделение чистой логики
// и telebot-обвязки — тот же приём, что и у startReplyText/fakeLinker в
// bot/start_test.go).
import (
	"context"
	"errors"
	"testing"

	"github.com/yarabey/agentify/bot/internal/orchestrator"
)

// fakeActor — подменный telegramActor для unit-тестов этого файла.
type fakeActor struct {
	tokenErr   error
	token      string
	gotTgUser  string
	tokenCalls int

	integrations    []orchestrator.Integration
	integrationsErr error

	createTaskResult orchestrator.Task
	createTaskErr    error
	gotIntegrationID string
	gotText          string
	gotIdempotency   string
	createTaskCalled bool

	answerErr    error
	gotTaskID    string
	gotQuestion  string
	gotAnswer    string
	answerCalled bool

	cancelErr    error
	gotCancelID  string
	cancelCalled bool
}

func (f *fakeActor) GetActingToken(_ context.Context, telegramUserID string) (string, error) {
	f.tokenCalls++
	f.gotTgUser = telegramUserID
	if f.tokenErr != nil {
		return "", f.tokenErr
	}
	if f.token == "" {
		return "test-token", nil
	}
	return f.token, nil
}

func (f *fakeActor) ListIntegrations(context.Context, string) ([]orchestrator.Integration, error) {
	if f.integrationsErr != nil {
		return nil, f.integrationsErr
	}
	return f.integrations, nil
}

func (f *fakeActor) CreateTask(_ context.Context, _, integrationID, text, idempotencyKey string) (orchestrator.Task, error) {
	f.createTaskCalled = true
	f.gotIntegrationID = integrationID
	f.gotText = text
	f.gotIdempotency = idempotencyKey
	if f.createTaskErr != nil {
		return orchestrator.Task{}, f.createTaskErr
	}
	return f.createTaskResult, nil
}

func (f *fakeActor) AnswerTask(_ context.Context, _, taskID, questionID, text string) error {
	f.answerCalled = true
	f.gotTaskID = taskID
	f.gotQuestion = questionID
	f.gotAnswer = text
	return f.answerErr
}

func (f *fakeActor) CancelTask(_ context.Context, _, taskID string) error {
	f.cancelCalled = true
	f.gotCancelID = taskID
	return f.cancelErr
}

// --- createTaskReplyText (обычный текст) ---------------------------------

// TestCreateTaskReplyText_NilActor — BOT_ORCHESTRATOR_URL не задан (actor —
// nil-интерфейс) → "недоступно", без паники.
func TestCreateTaskReplyText_NilActor(t *testing.T) {
	got := createTaskReplyText(context.Background(), nil, 1, true, 1, "текст задачи", nil)
	if got == "" {
		t.Fatal("пустой ответ при nil actor")
	}
}

// TestCreateTaskReplyText_EmptyText — пустое сообщение → отказ, GetActingToken
// не вызывается (нечего ставить в задачу, даже не привязанному пользователю).
func TestCreateTaskReplyText_EmptyText(t *testing.T) {
	actor := &fakeActor{}
	got := createTaskReplyText(context.Background(), actor, 1, true, 1, "   ", nil)
	if actor.tokenCalls != 0 {
		t.Fatal("GetActingToken не должен вызываться для пустого текста")
	}
	if got == "" {
		t.Fatal("пустой ответ на пустое сообщение")
	}
}

// TestCreateTaskReplyText_NotLinked — GetActingToken вернул ErrNotLinked →
// подсказка привязать аккаунт, ListIntegrations/CreateTask не вызываются.
func TestCreateTaskReplyText_NotLinked(t *testing.T) {
	actor := &fakeActor{tokenErr: orchestrator.ErrNotLinked}
	got := createTaskReplyText(context.Background(), actor, 42, true, 1, "сделай дело", nil)
	if got != unlinkedAccountReplyText {
		t.Fatalf("ответ = %q, ожидался общий текст подсказки привязки", got)
	}
	if actor.createTaskCalled {
		t.Fatal("CreateTask не должен вызываться для непривязанного пользователя")
	}
}

// TestCreateTaskReplyText_NoIntegrations — привязан, но нет ни одной
// интеграции → понятный отказ, CreateTask не вызывается.
func TestCreateTaskReplyText_NoIntegrations(t *testing.T) {
	actor := &fakeActor{integrations: nil}
	got := createTaskReplyText(context.Background(), actor, 1, true, 1, "сделай дело", nil)
	if actor.createTaskCalled {
		t.Fatal("CreateTask не должен вызываться без интеграций")
	}
	if got == "" {
		t.Fatal("пустой ответ без интеграций")
	}
}

// TestCreateTaskReplyText_SingleIntegration — ровно одна интеграция →
// CreateTask вызывается с её id, корректным text и детерминированным
// Idempotency-Key от (senderID, messageID) (FR E7, тикет 5.5).
func TestCreateTaskReplyText_SingleIntegration(t *testing.T) {
	actor := &fakeActor{
		integrations:     []orchestrator.Integration{{ID: "int-1", Name: "laptop"}},
		createTaskResult: orchestrator.Task{ID: "task-1", Status: "queued"},
	}
	got := createTaskReplyText(context.Background(), actor, 777, true, 55, "  собери проект  ", nil)
	if !actor.createTaskCalled {
		t.Fatal("CreateTask не вызван")
	}
	if actor.gotIntegrationID != "int-1" {
		t.Fatalf("integration_id = %q, ожидался int-1", actor.gotIntegrationID)
	}
	if actor.gotText != "собери проект" {
		t.Fatalf("text = %q, ожидался обрезанный текст без пробелов по краям", actor.gotText)
	}
	wantIdempotency := idempotencyKeyForMessage(777, 55)
	if actor.gotIdempotency != wantIdempotency {
		t.Fatalf("Idempotency-Key = %q, ожидался %q", actor.gotIdempotency, wantIdempotency)
	}
	if got == "" {
		t.Fatal("пустой ответ на успешную постановку")
	}
}

// TestCreateTaskReplyText_MultipleIntegrations — больше одной интеграции →
// CreateTask НЕ вызывается (бот не угадывает машину), ответ перечисляет
// интеграции и подсказывает /task.
func TestCreateTaskReplyText_MultipleIntegrations(t *testing.T) {
	actor := &fakeActor{integrations: []orchestrator.Integration{
		{ID: "int-1", Name: "laptop"},
		{ID: "int-2", Name: "server"},
	}}
	got := createTaskReplyText(context.Background(), actor, 1, true, 1, "сделай дело", nil)
	if actor.createTaskCalled {
		t.Fatal("CreateTask не должен вызываться при нескольких интеграциях")
	}
	if got == "" {
		t.Fatal("пустой ответ при нескольких интеграциях")
	}
}

// TestCreateTaskReplyText_CreateTaskNotFound — CreateTask вернул ErrNotFound
// (интеграция не найдена/недоступна) → понятный отказ, не паника.
func TestCreateTaskReplyText_CreateTaskNotFound(t *testing.T) {
	actor := &fakeActor{
		integrations:  []orchestrator.Integration{{ID: "int-1", Name: "laptop"}},
		createTaskErr: orchestrator.ErrNotFound,
	}
	got := createTaskReplyText(context.Background(), actor, 1, true, 1, "сделай дело", nil)
	if got == "" {
		t.Fatal("пустой ответ на ErrNotFound")
	}
}

// --- createTaskForIntegrationReplyText (/task) ---------------------------

// TestCreateTaskForIntegrationReplyText_Usage — пустой/неполный payload →
// подсказка использования, GetActingToken не вызывается.
func TestCreateTaskForIntegrationReplyText_Usage(t *testing.T) {
	cases := []string{"", "   ", "int-1"}
	for _, payload := range cases {
		actor := &fakeActor{}
		got := createTaskForIntegrationReplyText(context.Background(), actor, 1, true, 1, payload, nil)
		if actor.tokenCalls != 0 {
			t.Errorf("payload %q: GetActingToken не должен вызываться", payload)
		}
		if got == "" {
			t.Errorf("payload %q: пустой ответ", payload)
		}
	}
}

// TestCreateTaskForIntegrationReplyText_Success — валидный payload →
// CreateTask вызван с указанным integration_id и остатком текста.
func TestCreateTaskForIntegrationReplyText_Success(t *testing.T) {
	actor := &fakeActor{createTaskResult: orchestrator.Task{ID: "task-9", Status: "queued"}}
	got := createTaskForIntegrationReplyText(context.Background(), actor, 1, true, 1, "int-42 сделай две вещи по очереди", nil)
	if !actor.createTaskCalled {
		t.Fatal("CreateTask не вызван")
	}
	if actor.gotIntegrationID != "int-42" {
		t.Fatalf("integration_id = %q, ожидался int-42", actor.gotIntegrationID)
	}
	if actor.gotText != "сделай две вещи по очереди" {
		t.Fatalf("text = %q, ожидался остаток payload с сохранёнными пробелами", actor.gotText)
	}
	if got == "" {
		t.Fatal("пустой ответ на успешную постановку")
	}
}

// --- cancelTaskReplyText (/cancel) ---------------------------------------

// TestCancelTaskReplyText_Usage — пустой payload → подсказка использования.
func TestCancelTaskReplyText_Usage(t *testing.T) {
	actor := &fakeActor{}
	got := cancelTaskReplyText(context.Background(), actor, 1, true, "  ", nil)
	if actor.tokenCalls != 0 {
		t.Fatal("GetActingToken не должен вызываться для пустого payload")
	}
	if got == "" {
		t.Fatal("пустой ответ")
	}
}

// TestCancelTaskReplyText_NotLinked — непривязанный пользователь → подсказка
// привязки, CancelTask не вызывается.
func TestCancelTaskReplyText_NotLinked(t *testing.T) {
	actor := &fakeActor{tokenErr: orchestrator.ErrNotLinked}
	got := cancelTaskReplyText(context.Background(), actor, 1, true, "task-1", nil)
	if got != unlinkedAccountReplyText {
		t.Fatalf("ответ = %q, ожидался текст подсказки привязки", got)
	}
	if actor.cancelCalled {
		t.Fatal("CancelTask не должен вызываться для непривязанного пользователя")
	}
}

// TestCancelTaskReplyText_Success — валидный id задачи → CancelTask вызван
// именно с ним.
func TestCancelTaskReplyText_Success(t *testing.T) {
	actor := &fakeActor{}
	got := cancelTaskReplyText(context.Background(), actor, 1, true, "task-777", nil)
	if !actor.cancelCalled {
		t.Fatal("CancelTask не вызван")
	}
	if actor.gotCancelID != "task-777" {
		t.Fatalf("task_id = %q, ожидался task-777", actor.gotCancelID)
	}
	if got == "" {
		t.Fatal("пустой ответ на успешную отмену")
	}
}

// TestCancelTaskReplyText_NotFound — CancelTask вернул ErrNotFound → понятный
// отказ, не паника.
func TestCancelTaskReplyText_NotFound(t *testing.T) {
	actor := &fakeActor{cancelErr: orchestrator.ErrNotFound}
	got := cancelTaskReplyText(context.Background(), actor, 1, true, "task-unknown", nil)
	if got == "" {
		t.Fatal("пустой ответ на ErrNotFound")
	}
}

// --- answerTaskReplyText (/answer) ----------------------------------------

// TestAnswerTaskReplyText_Usage — неполный payload (меньше трёх токенов) →
// подсказка использования, GetActingToken не вызывается.
func TestAnswerTaskReplyText_Usage(t *testing.T) {
	cases := []string{"", "task-1", "task-1 question-1", "task-1 question-1   "}
	for _, payload := range cases {
		actor := &fakeActor{}
		got := answerTaskReplyText(context.Background(), actor, 1, true, payload, nil)
		if actor.tokenCalls != 0 {
			t.Errorf("payload %q: GetActingToken не должен вызываться", payload)
		}
		if got == "" {
			t.Errorf("payload %q: пустой ответ", payload)
		}
	}
}

// TestAnswerTaskReplyText_Success — валидный payload с многословным текстом
// ответа → AnswerTask вызван с корректными task_id/question_id и ПОЛНЫМ
// текстом ответа (пробелы внутри текста ответа не теряются, в отличие от
// tele.Context.Args()).
func TestAnswerTaskReplyText_Success(t *testing.T) {
	actor := &fakeActor{}
	got := answerTaskReplyText(context.Background(), actor, 1, true, "task-1 question-2 да, продолжай с первым шагом", nil)
	if !actor.answerCalled {
		t.Fatal("AnswerTask не вызван")
	}
	if actor.gotTaskID != "task-1" {
		t.Fatalf("task_id = %q, ожидался task-1", actor.gotTaskID)
	}
	if actor.gotQuestion != "question-2" {
		t.Fatalf("question_id = %q, ожидался question-2", actor.gotQuestion)
	}
	if actor.gotAnswer != "да, продолжай с первым шагом" {
		t.Fatalf("text = %q, ожидался полный текст ответа с сохранёнными пробелами", actor.gotAnswer)
	}
	if got == "" {
		t.Fatal("пустой ответ на успешный ответ")
	}
}

// TestAnswerTaskReplyText_NotFound — AnswerTask вернул ErrNotFound (задача
// или вопрос не найдены) → понятный отказ, не паника.
func TestAnswerTaskReplyText_NotFound(t *testing.T) {
	actor := &fakeActor{answerErr: orchestrator.ErrNotFound}
	got := answerTaskReplyText(context.Background(), actor, 1, true, "task-1 question-1 ответ", nil)
	if got == "" {
		t.Fatal("пустой ответ на ErrNotFound")
	}
}

// --- вспомогательные функции ----------------------------------------------

// TestSplitFirstToken — таблица случаев разбиения на первый токен и остаток
// со СОХРАНЁННЫМИ внутренними пробелами (см. годок splitFirstToken).
func TestSplitFirstToken(t *testing.T) {
	cases := []struct {
		in        string
		wantFirst string
		wantRest  string
		wantOK    bool
	}{
		{"", "", "", false},
		{"   ", "", "", false},
		{"a", "a", "", true},
		{"a b", "a", "b", true},
		{"a   b c", "a", "b c", true},
		{"  a  b c  ", "a", "b c", true},
	}
	for _, tc := range cases {
		first, rest, ok := splitFirstToken(tc.in)
		if first != tc.wantFirst || rest != tc.wantRest || ok != tc.wantOK {
			t.Errorf("splitFirstToken(%q) = (%q, %q, %v), ожидалось (%q, %q, %v)",
				tc.in, first, rest, ok, tc.wantFirst, tc.wantRest, tc.wantOK)
		}
	}
}

// TestActingTokenOrReplyText_NoSender — апдейт без Sender() → отказ,
// GetActingToken не вызывается (тот же принцип, что и startReplyText).
func TestActingTokenOrReplyText_NoSender(t *testing.T) {
	actor := &fakeActor{}
	token, replyText := actingTokenOrReplyText(context.Background(), actor, 0, false, nil)
	if token != "" {
		t.Fatal("token должен быть пуст без Sender()")
	}
	if replyText == "" {
		t.Fatal("пустой replyText без Sender()")
	}
	if actor.tokenCalls != 0 {
		t.Fatal("GetActingToken не должен вызываться без Sender()")
	}
}

// TestActingTokenOrReplyText_PassesTelegramUserID — senderID передаётся в
// GetActingToken как десятичная строка (FR D1).
func TestActingTokenOrReplyText_PassesTelegramUserID(t *testing.T) {
	actor := &fakeActor{}
	if _, replyText := actingTokenOrReplyText(context.Background(), actor, 123456, true, nil); replyText != "" {
		t.Fatalf("неожиданный replyText = %q", replyText)
	}
	if actor.gotTgUser != "123456" {
		t.Fatalf("telegram_user_id = %q, ожидался 123456", actor.gotTgUser)
	}
}

// TestActingTokenOrReplyText_UnknownError — неизвестная ошибка GetActingToken
// (сеть/внутренняя) → общий текст, не паника, отличается от unlinkedAccountReplyText.
func TestActingTokenOrReplyText_UnknownError(t *testing.T) {
	actor := &fakeActor{tokenErr: errors.New("сеть недоступна")}
	token, replyText := actingTokenOrReplyText(context.Background(), actor, 1, true, nil)
	if token != "" {
		t.Fatal("token должен быть пуст при ошибке")
	}
	if replyText == "" || replyText == unlinkedAccountReplyText {
		t.Fatalf("replyText = %q, ожидался отдельный общий текст ошибки", replyText)
	}
}

// TestFormatIntegrationList — список интеграций форматируется как "id (name)"
// через ", ".
func TestFormatIntegrationList(t *testing.T) {
	got := formatIntegrationList([]orchestrator.Integration{
		{ID: "int-1", Name: "laptop"},
		{ID: "int-2", Name: "server"},
	})
	want := "int-1 (laptop), int-2 (server)"
	if got != want {
		t.Fatalf("formatIntegrationList = %q, ожидалось %q", got, want)
	}
}
