//go:build integration

// Integration-тест сквозной доставки команды отмены на РЕАЛЬНЫХ
// Postgres+Redpanda через testcontainers-go (тикет 8.4, FR E6, бизнес-ТЗ
// §153-154 «Отмена»: «Отмена задачи останавливает агента на машине; статус
// корректно отражается во всех каналах»). Тот же пакет api_test, тот же
// паттерн, что и TestIntegration_PostTasks_HappyPath
// (tasks_integration_test.go) — startRedpandaForTasks/
// waitRedpandaReadyForTasks/setupDB/createTestUserWithToken/
// createTestIntegration/mustEncTaskTextForTasks переиспользуются напрямую
// (тот же тестовый бинарь пакета, без копирования).
//
// Пробел, который закрывает этот файл (тикет 11.3): PostTasksIdCancel
// (tasks.go) публикует конверт type=cancel в machine.commands ПОСЛЕ
// Transition в cancelled (см. godoc обработчика) — но ни один существующий
// тест не проверяет это на РЕАЛЬНОМ брокере. tasks_test.go
// (TestPostTasksIdCancel_HappyPath и соседние) — юнит-тесты с fakePublisher:
// они проверяют ТОЛЬКО форму опубликованного конверта в памяти, не то, что
// команда физически долетает до Redpanda и видна там по правильному ключу
// партиции. Не хватало ровно того же уровня доказательства, что уже есть для
// task_assigned в TestIntegration_PostTasks_HappyPath, но для cancel —
// единственной команды, чья доставка прямо упомянута в критериях приёмки
// §149-159 («Отмена»). TestIntegration_PostTasksIdCancel_DeliversCancelCommand
// ниже доводит задачу до running, отменяет её РЕАЛЬНЫМ HTTP-запросом POST
// /tasks/{id}/cancel и проверяет: 202; tasks.status=='cancelled' и новая
// запись task_events(status_change, trigger=cancel_requested) в БД; и —
// главное для этого тикета — что в РЕАЛЬНОЙ Redpanda (machine.commands,
// партиционирован по integration_id, ADR 0001) появляется конверт
// type=cancel с правильными task_id/integration_id — то есть команда
// отмены действительно ушла в очередь, откуда её (см.
// TestIntegration_Bridge_Ack_CommitsOnce_NoRedelivery,
// orchestrator/internal/bridge/bridge_integration_test.go) доставит агенту
// мост.
package api_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/yarabey/agentify/internal/bus"
	"github.com/yarabey/agentify/internal/crypto"
	"github.com/yarabey/agentify/orchestrator/internal/api"
	"github.com/yarabey/agentify/orchestrator/internal/db"
	"github.com/yarabey/agentify/orchestrator/internal/task"
)

// TestIntegration_PostTasksIdCancel_DeliversCancelCommand — см. godoc файла:
// задача в running, POST /tasks/{id}/cancel → 202, tasks.status=='cancelled'
// в БД с аудитом task_events, и РЕАЛЬНАЯ Redpanda содержит ровно один конверт
// type=cancel в machine.commands для этой задачи/интеграции.
func TestIntegration_PostTasksIdCancel_DeliversCancelCommand(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, doneDB := setupDB(ctx, t)
	defer doneDB()
	q := db.New(pool)

	seed, doneRedpanda := startRedpandaForTasks(ctx, t)
	defer doneRedpanda()
	seeds := []string{seed}
	waitRedpandaReadyForTasks(ctx, t, seeds)
	if err := bus.EnsureMVPTopics(ctx, seeds); err != nil {
		t.Fatalf("провижининг топиков: %v", err)
	}

	producer, err := bus.NewProducer(seeds)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer producer.Close()

	user, token := createTestUserWithToken(ctx, t, q, "alice-tasks-cancel")
	integration := createTestIntegration(ctx, t, q, user.ID, "alice-machine-cancel")
	integrationUUID := uuid.UUID(integration.ID.Bytes)

	idempotencyKey := "cancel-key-1"
	taskRow, err := q.CreateTask(ctx, db.CreateTaskParams{
		UserID:         user.ID,
		IntegrationID:  integration.ID,
		TextEnc:        mustEncTaskTextForTasks(t, "долгая задача, которую отменят"),
		IdempotencyKey: &idempotencyKey,
	})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	taskID := taskRow.ID
	taskUUID := uuid.UUID(taskID.Bytes)

	tr := task.NewTransitioner(pool, task.WithMasterKey([]byte(testEncryptionKey32)))
	// Довести задачу до running (созданная задача стартует в 'created' — она
	// ещё не проходила через POST /tasks, поэтому не queued автоматически).
	for _, trigger := range []task.Trigger{task.TriggerEnqueued, task.TriggerTaskAccepted} {
		if _, _, terr := tr.Transition(ctx, taskID, trigger); terr != nil {
			t.Fatalf("подготовка (%s): %v", trigger, terr)
		}
	}
	eventsBeforeCancel := countTaskEvents(ctx, t, pool, taskUUID)

	server := api.NewServer(q, nil, []byte(testJWTSigningKey), []byte(testEncryptionKey32))
	server.SetTransitioner(tr)
	server.SetCommandPublisher(producer)
	router := api.NewRouter(server)

	rec := doIntegrationsRequest(t, router, http.MethodPost, "/tasks/"+taskUUID.String()+"/cancel", token, nil)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("POST /tasks/{id}/cancel: статус = %d (%s), ожидался 202", rec.Code, rec.Body.String())
	}

	// БД: tasks.status == 'cancelled' + новая запись task_events(status_change,
	// trigger=cancel_requested) — тот же принцип аудита, что и в
	// TestIntegration_Transition_CancelFromAllActiveStates
	// (orchestrator/internal/task), но здесь через РЕАЛЬНЫЙ HTTP-хендлер.
	var dbStatus string
	if err := pool.QueryRow(ctx, `SELECT status FROM tasks WHERE id = $1`, taskID).Scan(&dbStatus); err != nil {
		t.Fatalf("SELECT tasks.status: %v", err)
	}
	if dbStatus != string(task.StatusCancelled) {
		t.Fatalf("tasks.status в БД = %q, ожидался cancelled", dbStatus)
	}
	if got := countTaskEvents(ctx, t, pool, taskUUID); got != eventsBeforeCancel+1 {
		t.Fatalf("после отмены ожидалась 1 новая запись task_events, было %d, стало %d", eventsBeforeCancel, got)
	}

	// Расшифровываем payload_enc последнего status_change (FR I1, тикет
	// 11.1) тем же подключом, что и рабочий Transitioner, и проверяем, что
	// это ИМЕННО отмена (trigger=cancel_requested, from=running,
	// to=cancelled) — не просто "какое-то" новое событие.
	var payloadEnc []byte
	if err := pool.QueryRow(ctx, `
		SELECT payload_enc FROM task_events
		WHERE task_id = $1 AND type = 'status_change'
		ORDER BY seq DESC LIMIT 1`, taskID).Scan(&payloadEnc); err != nil {
		t.Fatalf("SELECT последнего status_change: %v", err)
	}
	plaintext, derr := crypto.Decrypt(crypto.DeriveKey([]byte(testEncryptionKey32), task.EventPayloadKeyPurpose), payloadEnc)
	if derr != nil {
		t.Fatalf("расшифровать payload_enc status_change: %v", derr)
	}
	var statusChangePayload struct {
		From    string `json:"from"`
		To      string `json:"to"`
		Trigger string `json:"trigger"`
	}
	if uerr := json.Unmarshal(plaintext, &statusChangePayload); uerr != nil {
		t.Fatalf("разобрать payload_enc status_change: %v", uerr)
	}
	if statusChangePayload.Trigger != string(task.TriggerCancelRequested) {
		t.Fatalf("payload status_change: trigger=%s, хотим %s", statusChangePayload.Trigger, task.TriggerCancelRequested)
	}
	if statusChangePayload.From != string(task.StatusRunning) || statusChangePayload.To != string(task.StatusCancelled) {
		t.Fatalf("payload status_change: from=%s to=%s, хотим running/cancelled", statusChangePayload.From, statusChangePayload.To)
	}
	t.Logf("OK: БД видит task %s в статусе cancelled с новой записью истории (trigger=cancel_requested)", taskUUID)

	// Redpanda: ровно один конверт type=cancel для этой задачи в machine.commands.
	const group = "tasks-it-cancel"
	consumer, err := bus.NewConsumer(bus.ConsumerConfig{
		Seeds:  seeds,
		Group:  group,
		Topics: []string{bus.TopicMachineCommands},
	})
	if err != nil {
		t.Fatalf("NewConsumer (проверочный): %v", err)
	}
	defer consumer.Close()

	pollCtx, pollCancel := context.WithTimeout(ctx, 30*time.Second)
	defer pollCancel()
	fetches := consumer.PollFetches(pollCtx)
	var envs []bus.Envelope
	fetches.EachRecord(func(r *kgo.Record) {
		env, uerr := bus.Unmarshal(r.Value)
		if uerr != nil {
			t.Fatalf("unmarshal конверта: %v", uerr)
		}
		envs = append(envs, env)
	})
	if len(envs) != 1 {
		t.Fatalf("получено %d конверт(ов) в machine.commands, ожидался 1: %+v", len(envs), envs)
	}
	env := envs[0]
	if env.Type != bus.MessageTypeCancel {
		t.Fatalf("env.Type = %q, ожидался %q", env.Type, bus.MessageTypeCancel)
	}
	if env.IntegrationID != integrationUUID.String() {
		t.Fatalf("env.IntegrationID = %q, ожидался %s", env.IntegrationID, integrationUUID)
	}
	if env.TaskID == nil || *env.TaskID != taskUUID.String() {
		t.Fatalf("env.TaskID = %v, ожидался %s", env.TaskID, taskUUID)
	}
	t.Logf("OK: Redpanda содержит ровно один конверт type=cancel для task_id=%s, integration_id=%s — команда отмены реально ушла в очередь", taskUUID, integrationUUID)
}
