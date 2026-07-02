//go:build integration

// Integration-тест AnswerTimeoutWorker на РЕАЛЬНОМ Postgres (тикет 6.7, FR
// F5) — тот же паттерн, что и stale_worker_integration_test.go/
// transition_integration_test.go: собственный тег integration, тот же пакет
// task_test, тот же startPostgres/setupPool/seedTask helper — переиспользуются
// напрямую, без копирования (тот же тестовый бинарь пакета).
//
// Проверяемый сценарий — приёмка тикета 6.7 (FR F5: «нет ответа дольше
// порога → напоминание, затем настраиваемое поведение»):
//  1. задача доводится до waiting_user через TransitionWithEvent с событием
//     agent_question (реалистичный payload, тот же приём, что и
//     handleAgentQuestion в machine_ws.go) — вопрос заведомо старше
//     маленького threshold;
//  2. AnswerTimeoutWorker с поведением auto_cancel находит эту задачу и
//     переводит её в cancelled через TriggerCancelRequested — проверяется
//     ПРЯМЫМ SELECT из БД (tasks.status и новая запись
//     task_events(status_change, trigger=cancel_requested)), а не только
//     возвращаемым значением Transition.
package task_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/yarabey/agentify/orchestrator/internal/db"
	"github.com/yarabey/agentify/orchestrator/internal/task"
)

// TestIntegration_AnswerTimeoutWorker_AutoCancel_MarksTaskCancelled — приёмка
// тикета 6.7: agent_question устарел дольше threshold, поведение auto_cancel
// → tasks.status становится 'cancelled' с новой записью
// task_events(status_change, trigger=cancel_requested).
func TestIntegration_AnswerTimeoutWorker_AutoCancel_MarksTaskCancelled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, cleanup := setupPool(ctx, t)
	defer cleanup()

	q := db.New(pool)
	taskID := seedTask(ctx, t, pool, q, "answer-timeout-owner")

	// lastStatusChangePayload (см. stale_worker_integration_test.go) ниже
	// расшифровывает payload_enc фиксированным staleWorkerTestMasterKey —
	// Transitioner здесь должен шифровать тем же ключом, иначе GCM-тег не
	// сойдётся (FR I1, тикет 11.1).
	tr := task.NewTransitioner(pool, task.WithMasterKey(staleWorkerTestMasterKey))

	// Довести задачу до waiting_user с agent_question (реалистичный payload,
	// тот же приём, что handleAgentQuestion в machine_ws.go).
	for _, trigger := range []task.Trigger{task.TriggerEnqueued, task.TriggerTaskAccepted} {
		if _, _, err := tr.Transition(ctx, taskID, trigger); err != nil {
			t.Fatalf("подготовка (%s): %v", trigger, err)
		}
	}
	questionPayload := []byte(`{"question_id":"q-1","text":"продолжать?"}`)
	from, to, err := tr.TransitionWithEvent(ctx, taskID, task.TriggerAgentQuestion,
		"agent_question", pgtype.UUID{}, questionPayload)
	if err != nil {
		t.Fatalf("TransitionWithEvent(agent_question): %v", err)
	}
	if from != task.StatusRunning || to != task.StatusWaitingUser {
		t.Fatalf("agent_question: from=%s to=%s, хотим running→waiting_user", from, to)
	}

	row := getTaskRow(ctx, t, pool, taskID)
	if row.status != string(task.StatusWaitingUser) {
		t.Fatalf("подготовка: tasks.status = %s, хотим waiting_user", row.status)
	}
	eventsBefore := countStatusChangeEvents(ctx, t, pool, taskID)

	const (
		answerTimeoutThreshold = 50 * time.Millisecond
		pollInterval           = 20 * time.Millisecond
	)
	// Вопрос должен устареть ДО первого тика — короткая пауза, заметно
	// больше threshold, чтобы избежать гонки со временем создания события.
	time.Sleep(2 * answerTimeoutThreshold)

	w, err := task.NewAnswerTimeoutWorker(tr, db.New(pool), nil,
		task.WithAnswerTimeoutThreshold(answerTimeoutThreshold),
		task.WithAnswerTimeoutPollInterval(pollInterval),
		task.WithAnswerTimeoutBehavior(task.AnswerTimeoutBehaviorAutoCancel))
	if err != nil {
		t.Fatalf("NewAnswerTimeoutWorker: %v", err)
	}

	runCtx, runCancel := context.WithCancel(ctx)
	runDone := make(chan struct{})
	go func() {
		_ = w.Run(runCtx)
		close(runDone)
	}()

	waitForTaskStatus(ctx, t, pool, taskID, task.StatusCancelled, 10*time.Second)

	after := getTaskRow(ctx, t, pool, taskID)
	if after.status != string(task.StatusCancelled) {
		t.Fatalf("tasks.status после авто-отмены = %s, хотим cancelled", after.status)
	}
	if got := countStatusChangeEvents(ctx, t, pool, taskID); got != eventsBefore+1 {
		t.Fatalf("после авто-отмены ожидалась 1 новая запись status_change, было %d, стало %d", eventsBefore, got)
	}
	payload := lastStatusChangePayload(ctx, t, pool, taskID)
	if payload.Trigger != string(task.TriggerCancelRequested) {
		t.Fatalf("payload status_change после авто-отмены: trigger=%s, хотим %s", payload.Trigger, task.TriggerCancelRequested)
	}
	if payload.From != string(task.StatusWaitingUser) || payload.To != string(task.StatusCancelled) {
		t.Fatalf("payload status_change после авто-отмены: from=%s to=%s, хотим waiting_user/cancelled", payload.From, payload.To)
	}

	runCancel()
	select {
	case <-runDone:
	case <-time.After(10 * time.Second):
		t.Fatal("AnswerTimeoutWorker.Run не завершился после отмены ctx за отведённое время")
	}
}

// TestIntegration_AnswerTimeoutWorker_WaitBehavior_DoesNotCancel — приёмка
// тикета 6.7 (ветка "wait", дефолт): agent_question устарел дольше threshold,
// но поведение wait (дефолт) → tasks.status остаётся waiting_user, никакой
// новой записи task_events(status_change) не появляется.
func TestIntegration_AnswerTimeoutWorker_WaitBehavior_DoesNotCancel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, cleanup := setupPool(ctx, t)
	defer cleanup()

	q := db.New(pool)
	taskID := seedTask(ctx, t, pool, q, "answer-timeout-wait-owner")

	tr := task.NewTransitioner(pool)

	for _, trigger := range []task.Trigger{task.TriggerEnqueued, task.TriggerTaskAccepted} {
		if _, _, err := tr.Transition(ctx, taskID, trigger); err != nil {
			t.Fatalf("подготовка (%s): %v", trigger, err)
		}
	}
	questionPayload := []byte(`{"question_id":"q-1","text":"продолжать?"}`)
	if _, _, err := tr.TransitionWithEvent(ctx, taskID, task.TriggerAgentQuestion,
		"agent_question", pgtype.UUID{}, questionPayload); err != nil {
		t.Fatalf("TransitionWithEvent(agent_question): %v", err)
	}
	eventsBefore := countStatusChangeEvents(ctx, t, pool, taskID)

	const (
		answerTimeoutThreshold = 50 * time.Millisecond
		pollInterval           = 20 * time.Millisecond
	)
	time.Sleep(2 * answerTimeoutThreshold)

	w, err := task.NewAnswerTimeoutWorker(tr, db.New(pool), nil,
		task.WithAnswerTimeoutThreshold(answerTimeoutThreshold),
		task.WithAnswerTimeoutPollInterval(pollInterval))
	if err != nil {
		t.Fatalf("NewAnswerTimeoutWorker: %v", err)
	}

	runCtx, runCancel := context.WithCancel(ctx)
	runDone := make(chan struct{})
	go func() {
		_ = w.Run(runCtx)
		close(runDone)
	}()

	// Дать воркеру несколько тиков отработать, затем убедиться, что статус
	// не изменился (нет позитивного события, которое можно дождаться —
	// ждём фиксированное время, кратное нескольким тикам).
	time.Sleep(10 * pollInterval)

	after := getTaskRow(ctx, t, pool, taskID)
	if after.status != string(task.StatusWaitingUser) {
		t.Fatalf("tasks.status при поведении wait = %s, хотим неизменный waiting_user", after.status)
	}
	if got := countStatusChangeEvents(ctx, t, pool, taskID); got != eventsBefore {
		t.Fatalf("при поведении wait количество status_change не должно меняться: было %d, стало %d", eventsBefore, got)
	}

	runCancel()
	select {
	case <-runDone:
	case <-time.After(10 * time.Second):
		t.Fatal("AnswerTimeoutWorker.Run не завершился после отмены ctx за отведённое время")
	}
}
