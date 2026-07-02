//go:build integration

// Integration-тест приёмки тикета 8.4 (FR E6, бизнес-ТЗ §153-155 «Отмена»:
// «Отмена задачи останавливает агента на машине; статус корректно отражается
// во всех каналах») на РЕАЛЬНОМ Postgres через testcontainers-go — тот же
// пакет task_test, тот же паттерн, что и transition_integration_test.go/
// stale_worker_integration_test.go (setupPool/seedTask/getTaskRow/
// listTaskEvents/staleWorkerTestMasterKey/lastStatusChangePayload/
// countStatusChangeEvents переиспользуются напрямую — тот же тестовый
// бинарь пакета, без копирования).
//
// Пробел, который закрывает этот файл (тикет 11.3): ребро TriggerCancelRequested
// → Cancelled существует в fsm.go для ПЯТИ активных статусов (Queued, Running,
// WaitingUser, AwaitingConfirm, Stale — «отмена из любого активного статуса»,
// приёмка 8.4), и до этого файла было проверено ТОЛЬКО как чистая функция
// NextStatus (fsm_test.go, TestNextStatus_ValidTransitions — без БД) — ни один
// integration-тест не проводил Transition(cancel_requested) через РЕАЛЬНЫЙ
// Postgres ни для ОДНОГО из этих пяти статусов: transition_integration_test.go
// использует cancel_requested только один раз, и то в
// TestIntegration_Transition_InvalidRollsBack — как НЕДОПУСТИМЫЙ переход из
// completed (терминальный статус, для доказательства отката транзакции), не
// как позитивный сценарий отмены активной задачи. TestIntegration_Transition_CancelFromAllActiveStates
// ниже проверяет ВСЕ пять рёбер на реальной БД: tasks.status становится
// 'cancelled', и появляется новая запись task_events(status_change,
// trigger=cancel_requested, from=<исходный статус>, to=cancelled) — то же
// доказательство аудита, что и в StaleWorker/AnswerTimeoutWorker
// integration-тестах, но для самого Transition, вызываемого напрямую
// (как это делает PostTasksIdCancel, orchestrator/internal/api/tasks.go).
package task_test

import (
	"context"
	"testing"
	"time"

	"github.com/yarabey/agentify/orchestrator/internal/db"
	"github.com/yarabey/agentify/orchestrator/internal/task"
)

// TestIntegration_Transition_CancelFromAllActiveStates — приёмка тикета 8.4
// («Тесты: отмена из любого активного статуса → cancelled»): для каждого из
// пяти активных статусов (Queued/Running/WaitingUser/AwaitingConfirm/Stale) —
// довести задачу до него ЦЕПОЧКОЙ валидных Transition (та же техника, что и
// TestIntegration_Transition_ValidChain), затем Transition(cancel_requested)
// → to==Cancelled; и на РЕАЛЬНОМ Postgres: tasks.status=='cancelled',
// появилась РОВНО одна новая запись task_events(status_change) с
// trigger=cancel_requested и корректными from/to.
func TestIntegration_Transition_CancelFromAllActiveStates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, cleanup := setupPool(ctx, t)
	defer cleanup()

	q := db.New(pool)

	cases := []struct {
		name     string
		prep     []task.Trigger // цепочка триггеров created→...→целевой активный статус
		fromWant task.Status
	}{
		{
			name:     "queued",
			prep:     []task.Trigger{task.TriggerEnqueued},
			fromWant: task.StatusQueued,
		},
		{
			name:     "running",
			prep:     []task.Trigger{task.TriggerEnqueued, task.TriggerTaskAccepted},
			fromWant: task.StatusRunning,
		},
		{
			name:     "waiting_user",
			prep:     []task.Trigger{task.TriggerEnqueued, task.TriggerTaskAccepted, task.TriggerAgentQuestion},
			fromWant: task.StatusWaitingUser,
		},
		{
			name:     "awaiting_confirm",
			prep:     []task.Trigger{task.TriggerEnqueued, task.TriggerTaskAccepted, task.TriggerAgentCompleted},
			fromWant: task.StatusAwaitingConfirm,
		},
		{
			name:     "stale",
			prep:     []task.Trigger{task.TriggerEnqueued, task.TriggerTaskAccepted, task.TriggerTimeout},
			fromWant: task.StatusStale,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			taskID := seedTask(ctx, t, pool, q, "cancel-active-"+tc.name)
			tr := task.NewTransitioner(pool, task.WithMasterKey(staleWorkerTestMasterKey))

			for i, trigger := range tc.prep {
				if _, _, err := tr.Transition(ctx, taskID, trigger); err != nil {
					t.Fatalf("подготовка шаг %d (%s): %v", i+1, trigger, err)
				}
			}
			prepared := getTaskRow(ctx, t, pool, taskID)
			if prepared.status != string(tc.fromWant) {
				t.Fatalf("подготовка: tasks.status = %s, хотим %s (активный статус перед отменой)", prepared.status, tc.fromWant)
			}
			eventsBeforeCancel := countStatusChangeEvents(ctx, t, pool, taskID)

			from, to, err := tr.Transition(ctx, taskID, task.TriggerCancelRequested)
			if err != nil {
				t.Fatalf("Transition(%s, cancel_requested): %v", tc.fromWant, err)
			}
			if from != tc.fromWant {
				t.Fatalf("from = %s, хотим %s", from, tc.fromWant)
			}
			if to != task.StatusCancelled {
				t.Fatalf("to = %s, хотим cancelled", to)
			}

			after := getTaskRow(ctx, t, pool, taskID)
			if after.status != string(task.StatusCancelled) {
				t.Fatalf("tasks.status в БД = %s, хотим cancelled", after.status)
			}

			if got := countStatusChangeEvents(ctx, t, pool, taskID); got != eventsBeforeCancel+1 {
				t.Fatalf("после отмены ожидалась 1 новая запись status_change, было %d, стало %d", eventsBeforeCancel, got)
			}
			payload := lastStatusChangePayload(ctx, t, pool, taskID)
			if payload.Trigger != string(task.TriggerCancelRequested) {
				t.Fatalf("payload status_change после отмены: trigger=%s, хотим %s", payload.Trigger, task.TriggerCancelRequested)
			}
			if payload.From != string(tc.fromWant) || payload.To != string(task.StatusCancelled) {
				t.Fatalf("payload status_change после отмены: from=%s to=%s, хотим %s/cancelled", payload.From, payload.To, tc.fromWant)
			}
			t.Logf("OK: отмена из %s → cancelled, аудит task_events корректен", tc.fromWant)
		})
	}
}
