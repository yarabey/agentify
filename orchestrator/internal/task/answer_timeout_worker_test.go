// Юнит-тесты AnswerTimeoutWorker (тикет 6.7, FR F5) — без Postgres:
// sqlc-запрос подменяется фейком (answerTimeoutLister), переход FSM —
// функцией-шпионом (transitionFunc, тот же приём, что и в
// stale_worker_test.go), а доставка уведомления — фейковым notifier
// (answerNotifier). Опции WithAnswerTimeoutPollInterval/
// WithAnswerTimeoutThreshold позволяют прогонять цикл на миллисекундах.
package task

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/yarabey/agentify/orchestrator/internal/db"
	"github.com/yarabey/agentify/orchestrator/internal/notify"
)

// fakeAnswerTimeoutLister — фейковая реализация answerTimeoutLister:
// возвращает заранее заданный (изменяемый между вызовами) срез строк.
type fakeAnswerTimeoutLister struct {
	rows []db.ListWaitingUserTasksWithStaleQuestionRow
}

func (f *fakeAnswerTimeoutLister) ListWaitingUserTasksWithStaleQuestion(_ context.Context, _ pgtype.Timestamptz) ([]db.ListWaitingUserTasksWithStaleQuestionRow, error) {
	return f.rows, nil
}

// fakeAnswerNotifier — фейковая реализация answerNotifier: запоминает все
// вызовы Notify (без мьютекса — тесты этого файла однопоточные, вызывают
// tick/handleStaleQuestion напрямую из горутины теста, кроме
// TestAnswerTimeoutWorker_Run_StopsOnContextCancel, где Notify вообще не
// достигается).
type fakeAnswerNotifier struct {
	calls []notify.Notification
}

func (f *fakeAnswerNotifier) Notify(_ context.Context, n notify.Notification) error {
	f.calls = append(f.calls, n)
	return nil
}

// newTestAnswerTimeoutWorker собирает AnswerTimeoutWorker в обход
// NewAnswerTimeoutWorker (который требует настоящий *Transitioner), напрямую
// подставляя лишь необходимые для теста поля — тот же приём, что и
// newTestStaleWorker в stale_worker_test.go.
func newTestAnswerTimeoutWorker(t *testing.T, lister answerTimeoutLister, spy *spyTransitioner, notifier answerNotifier, opts ...AnswerTimeoutWorkerOption) *AnswerTimeoutWorker {
	t.Helper()
	w := &AnswerTimeoutWorker{
		queries:      lister,
		transition:   spy.transition,
		notifier:     notifier,
		logger:       slog.Default(),
		pollInterval: defaultAnswerTimeoutPollInterval,
		threshold:    defaultAnswerTimeoutThreshold,
		behavior:     AnswerTimeoutBehaviorWait,
		handled:      make(map[pgtype.UUID]pgtype.UUID),
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

func questionEventID(b byte) pgtype.UUID {
	var u pgtype.UUID
	u.Bytes[15] = b
	u.Valid = true
	return u
}

// TestAnswerTimeoutWorker_HandleStaleQuestion_SendsReminderOnce — одна и та
// же строка (тот же question_event_id) обрабатывается дважды подряд →
// уведомление отправлено РОВНО один раз (дедуп через handled).
func TestAnswerTimeoutWorker_HandleStaleQuestion_SendsReminderOnce(t *testing.T) {
	taskID := uuidFromByte(1)
	row := db.ListWaitingUserTasksWithStaleQuestionRow{
		TaskID:          taskID,
		UserID:          uuidFromByte(2),
		QuestionEventID: questionEventID(1),
	}
	notifier := &fakeAnswerNotifier{}
	spy := &spyTransitioner{}
	w := newTestAnswerTimeoutWorker(t, &fakeAnswerTimeoutLister{}, spy, notifier)

	w.handleStaleQuestion(context.Background(), row)
	w.handleStaleQuestion(context.Background(), row)

	if len(notifier.calls) != 1 {
		t.Fatalf("ожидался 1 вызов Notify, получено %d", len(notifier.calls))
	}
	if notifier.calls[0].TaskID != taskID {
		t.Fatalf("Notify: TaskID=%v, хотим %v", notifier.calls[0].TaskID, taskID)
	}
	if notifier.calls[0].Kind != notify.KindAnswerReminder {
		t.Fatalf("Notify: Kind=%s, хотим %s", notifier.calls[0].Kind, notify.KindAnswerReminder)
	}
}

// TestAnswerTimeoutWorker_HandleStaleQuestion_NewQuestionResendsReminder —
// тот же taskID, но НОВЫЙ question_event_id → уведомление отправляется снова
// (сброс дедупа по факту нового вопроса).
func TestAnswerTimeoutWorker_HandleStaleQuestion_NewQuestionResendsReminder(t *testing.T) {
	taskID := uuidFromByte(1)
	row1 := db.ListWaitingUserTasksWithStaleQuestionRow{TaskID: taskID, UserID: uuidFromByte(2), QuestionEventID: questionEventID(1)}
	row2 := db.ListWaitingUserTasksWithStaleQuestionRow{TaskID: taskID, UserID: uuidFromByte(2), QuestionEventID: questionEventID(2)}
	notifier := &fakeAnswerNotifier{}
	spy := &spyTransitioner{}
	w := newTestAnswerTimeoutWorker(t, &fakeAnswerTimeoutLister{}, spy, notifier)

	w.handleStaleQuestion(context.Background(), row1)
	w.handleStaleQuestion(context.Background(), row2)

	if len(notifier.calls) != 2 {
		t.Fatalf("ожидалось 2 вызова Notify (разные вопросы), получено %d", len(notifier.calls))
	}
}

// TestAnswerTimeoutWorker_HandleStaleQuestion_AutoCancelBehavior_TriggersCancelRequested —
// с WithAnswerTimeoutBehavior(AnswerTimeoutBehaviorAutoCancel) →
// spy-transitioner получает вызов РОВНО с TriggerCancelRequested для нужного
// taskID.
func TestAnswerTimeoutWorker_HandleStaleQuestion_AutoCancelBehavior_TriggersCancelRequested(t *testing.T) {
	taskID := uuidFromByte(1)
	row := db.ListWaitingUserTasksWithStaleQuestionRow{TaskID: taskID, UserID: uuidFromByte(2), QuestionEventID: questionEventID(1)}
	spy := &spyTransitioner{}
	w := newTestAnswerTimeoutWorker(t, &fakeAnswerTimeoutLister{}, spy, &fakeAnswerNotifier{}, WithAnswerTimeoutBehavior(AnswerTimeoutBehaviorAutoCancel))

	w.handleStaleQuestion(context.Background(), row)

	calls := spy.callsSnapshot()
	if len(calls) != 1 {
		t.Fatalf("ожидался 1 вызов перехода, получено %d", len(calls))
	}
	if calls[0].taskID != taskID {
		t.Fatalf("taskID=%v, хотим %v", calls[0].taskID, taskID)
	}
	if calls[0].trigger != TriggerCancelRequested {
		t.Fatalf("trigger=%s, хотим %s", calls[0].trigger, TriggerCancelRequested)
	}
}

// TestAnswerTimeoutWorker_HandleStaleQuestion_WaitBehavior_DoesNotTriggerTransition —
// с дефолтным (или явным Wait) поведением → spy-transitioner НЕ вызывается
// вообще.
func TestAnswerTimeoutWorker_HandleStaleQuestion_WaitBehavior_DoesNotTriggerTransition(t *testing.T) {
	row := db.ListWaitingUserTasksWithStaleQuestionRow{TaskID: uuidFromByte(1), UserID: uuidFromByte(2), QuestionEventID: questionEventID(1)}
	spy := &spyTransitioner{}
	w := newTestAnswerTimeoutWorker(t, &fakeAnswerTimeoutLister{}, spy, &fakeAnswerNotifier{}, WithAnswerTimeoutBehavior(AnswerTimeoutBehaviorWait))

	w.handleStaleQuestion(context.Background(), row)

	if calls := spy.callsSnapshot(); len(calls) != 0 {
		t.Fatalf("ожидалось 0 вызовов перехода при поведении wait, получено %d", len(calls))
	}
}

// TestAnswerTimeoutWorker_HandleStaleQuestion_NilNotifier_DoesNotPanic —
// notifier не задан (nil) → no panic, авто-поведение (если настроено) всё
// равно срабатывает.
func TestAnswerTimeoutWorker_HandleStaleQuestion_NilNotifier_DoesNotPanic(t *testing.T) {
	row := db.ListWaitingUserTasksWithStaleQuestionRow{TaskID: uuidFromByte(1), UserID: uuidFromByte(2), QuestionEventID: questionEventID(1)}
	spy := &spyTransitioner{}
	w := newTestAnswerTimeoutWorker(t, &fakeAnswerTimeoutLister{}, spy, nil, WithAnswerTimeoutBehavior(AnswerTimeoutBehaviorAutoCancel))

	w.handleStaleQuestion(context.Background(), row)

	calls := spy.callsSnapshot()
	if len(calls) != 1 || calls[0].trigger != TriggerCancelRequested {
		t.Fatalf("ожидался 1 вызов перехода TriggerCancelRequested несмотря на nil notifier, получено %+v", calls)
	}
}

// TestAnswerTimeoutWorker_Tick_PrunesHandledForTasksNoLongerListed — задача
// обработана в первом тике, во втором тике её уже нет в списке (queries
// вернули пустой срез) → следующее появление ТОЙ ЖЕ задачи с ТЕМ ЖЕ
// question_event_id снова шлёт напоминание (доказывает, что prune реально
// удалил запись, а не просто игнорируется).
func TestAnswerTimeoutWorker_Tick_PrunesHandledForTasksNoLongerListed(t *testing.T) {
	taskID := uuidFromByte(1)
	row := db.ListWaitingUserTasksWithStaleQuestionRow{TaskID: taskID, UserID: uuidFromByte(2), QuestionEventID: questionEventID(1)}
	lister := &fakeAnswerTimeoutLister{rows: []db.ListWaitingUserTasksWithStaleQuestionRow{row}}
	notifier := &fakeAnswerNotifier{}
	spy := &spyTransitioner{}
	w := newTestAnswerTimeoutWorker(t, lister, spy, notifier)

	// Тик 1: задача с устаревшим вопросом присутствует → напоминание.
	w.tick(context.Background())
	if len(notifier.calls) != 1 {
		t.Fatalf("после тика 1 ожидался 1 вызов Notify, получено %d", len(notifier.calls))
	}
	if _, ok := w.handled[taskID]; !ok {
		t.Fatal("после тика 1 задача должна быть в handled")
	}

	// Тик 2: задача больше не listed (ответили/отменили) → prune handled.
	lister.rows = nil
	w.tick(context.Background())
	if _, ok := w.handled[taskID]; ok {
		t.Fatal("после тика 2 (задача не listed) handled должен быть очищен для этой задачи")
	}

	// Тик 3: задача снова listed с ТЕМ ЖЕ question_event_id → напоминание
	// отправляется снова, а не подавляется устаревшей записью дедупа.
	lister.rows = []db.ListWaitingUserTasksWithStaleQuestionRow{row}
	w.tick(context.Background())
	if len(notifier.calls) != 2 {
		t.Fatalf("после тика 3 ожидалось 2 вызова Notify суммарно, получено %d", len(notifier.calls))
	}
}

// TestNewAnswerTimeoutWorker_NilArgs — nil transitioner или nil queries —
// ошибка конструктора, не паника; notifier nil — НЕ ошибка.
func TestNewAnswerTimeoutWorker_NilArgs(t *testing.T) {
	if _, err := NewAnswerTimeoutWorker(nil, &fakeAnswerTimeoutLister{}, &fakeAnswerNotifier{}); err == nil {
		t.Fatal("NewAnswerTimeoutWorker(nil transitioner, ...): ожидалась ошибка")
	}
	if _, err := NewAnswerTimeoutWorker(NewTransitioner(nil), nil, &fakeAnswerNotifier{}); err == nil {
		t.Fatal("NewAnswerTimeoutWorker(..., nil queries, ...): ожидалась ошибка")
	}
	w, err := NewAnswerTimeoutWorker(NewTransitioner(nil), &fakeAnswerTimeoutLister{}, nil)
	if err != nil {
		t.Fatalf("NewAnswerTimeoutWorker с nil notifier не должен возвращать ошибку: %v", err)
	}
	if w.notifier != nil {
		t.Fatal("notifier должен остаться nil")
	}
}

// TestWithAnswerTimeoutThreshold_IgnoresNonPositive — неположительное
// значение WithAnswerTimeoutThreshold игнорируется (дефолт сохраняется).
func TestWithAnswerTimeoutThreshold_IgnoresNonPositive(t *testing.T) {
	w, err := NewAnswerTimeoutWorker(NewTransitioner(nil), &fakeAnswerTimeoutLister{}, nil,
		WithAnswerTimeoutThreshold(0), WithAnswerTimeoutThreshold(-time.Second))
	if err != nil {
		t.Fatalf("NewAnswerTimeoutWorker: %v", err)
	}
	if w.threshold != defaultAnswerTimeoutThreshold {
		t.Fatalf("threshold=%v, ожидался дефолт %v", w.threshold, defaultAnswerTimeoutThreshold)
	}
}

// TestWithAnswerTimeoutPollInterval_IgnoresNonPositive — неположительное
// значение WithAnswerTimeoutPollInterval игнорируется (дефолт сохраняется).
func TestWithAnswerTimeoutPollInterval_IgnoresNonPositive(t *testing.T) {
	w, err := NewAnswerTimeoutWorker(NewTransitioner(nil), &fakeAnswerTimeoutLister{}, nil,
		WithAnswerTimeoutPollInterval(0), WithAnswerTimeoutPollInterval(-time.Second))
	if err != nil {
		t.Fatalf("NewAnswerTimeoutWorker: %v", err)
	}
	if w.pollInterval != defaultAnswerTimeoutPollInterval {
		t.Fatalf("pollInterval=%v, ожидался дефолт %v", w.pollInterval, defaultAnswerTimeoutPollInterval)
	}
}

// TestWithAnswerTimeoutBehavior_IgnoresUnknownValue — неизвестная строка
// (вроде "" или "bogus") игнорируется, дефолт Wait сохраняется.
func TestWithAnswerTimeoutBehavior_IgnoresUnknownValue(t *testing.T) {
	for _, bogus := range []AnswerTimeoutBehavior{"", "bogus"} {
		w, err := NewAnswerTimeoutWorker(NewTransitioner(nil), &fakeAnswerTimeoutLister{}, nil, WithAnswerTimeoutBehavior(bogus))
		if err != nil {
			t.Fatalf("NewAnswerTimeoutWorker: %v", err)
		}
		if w.behavior != AnswerTimeoutBehaviorWait {
			t.Fatalf("behavior для %q = %s, ожидался дефолт %s", bogus, w.behavior, AnswerTimeoutBehaviorWait)
		}
	}
}

// TestAnswerTimeoutWorker_Run_StopsOnContextCancel — Run завершается (nil)
// сразу после отмены ctx, даже если ни одного тика ещё не было.
func TestAnswerTimeoutWorker_Run_StopsOnContextCancel(t *testing.T) {
	w := newTestAnswerTimeoutWorker(t, &fakeAnswerTimeoutLister{}, &spyTransitioner{}, &fakeAnswerNotifier{}, WithAnswerTimeoutPollInterval(time.Hour))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run вернул ошибку после отмены ctx: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run не завершился после отмены ctx за отведённое время")
	}
}
