//go:build integration

// Integration-тесты POST /tasks на РЕАЛЬНЫХ Postgres+Redpanda через
// testcontainers-go (тикет 5.3, FR E1, E4, §4 «Постановка задачи из канала»).
// Postgres поднимается общим setupDB (register_integration_test.go, тот же
// пакет api_test); Redpanda — собственный копипаст-хелпер startRedpanda (тот
// же паттерн, что и orchestrator/internal/bridge/bridge_integration_test.go —
// неэкспортированный помощник другого пакета напрямую не переиспользуется).
//
// Покрывает обязательные по тикету 5.3 сценарии («задача в очереди, видна в
// истории»):
//   - happy path: POST /tasks → 201, status=="queued"; РЕАЛЬНАЯ БД видит
//     tasks.status='queued' и task_events(type='status_change') (FR E1, F2,
//     история); РЕАЛЬНАЯ Redpanda содержит ровно один конверт type=task_assigned
//     в machine.commands с правильными task_id/integration_id/payload.text;
//   - владение: POST /tasks на чужую интеграцию → 404 (FR A4, I3);
//   - дубль Idempotency-Key одного пользователя → первый 201, второй 409
//     (временное поведение до полноценного дедупа тикета 5.5).
package api_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/yarabey/agentify/internal/bus"
	"github.com/yarabey/agentify/orchestrator/internal/api"
	"github.com/yarabey/agentify/orchestrator/internal/db"
	"github.com/yarabey/agentify/orchestrator/internal/task"
)

// startRedpandaForTasks поднимает одиночный брокер Redpanda в контейнере —
// копия помощника bridge_integration_test.go (пакеты не шарят неэкспортированные
// тестовые хелперы, см. godoc файла).
func startRedpandaForTasks(ctx context.Context, t *testing.T) (string, func()) {
	t.Helper()
	container, err := redpanda.Run(ctx, "redpandadata/redpanda:v24.2.7")
	if err != nil {
		t.Fatalf("поднять Redpanda-контейнер: %v", err)
	}
	cleanup := func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("terminate Redpanda: %v", err)
		}
	}
	seed, err := container.KafkaSeedBroker(ctx)
	if err != nil {
		cleanup()
		t.Fatalf("получить seed-брокер: %v", err)
	}
	return seed, cleanup
}

// waitRedpandaReadyForTasks ждёт, пока брокер начнёт отвечать на Ping — та же
// логика, что waitTopicReady в bridge_integration_test.go.
func waitRedpandaReadyForTasks(ctx context.Context, t *testing.T, seeds []string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		cl, err := kgo.NewClient(kgo.SeedBrokers(seeds...))
		if err == nil {
			if pingErr := cl.Ping(ctx); pingErr == nil {
				cl.Close()
				return
			}
			cl.Close()
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatal("брокер Redpanda не стал готов за отведённое время")
}

// createTestIntegration вставляет интеграцию владельца напрямую через sqlc
// (без прохождения через POST /integrations — крипто-детали uuid_hmac/uuid_enc
// не относятся к этому тикету, значения тестовые/произвольные).
func createTestIntegration(ctx context.Context, t *testing.T, q *db.Queries, userID pgtype.UUID, name string) db.Integration {
	t.Helper()
	secret := uuid.New()
	integration, err := q.CreateIntegration(ctx, db.CreateIntegrationParams{
		UserID:   userID,
		Name:     name,
		UuidHmac: hex.EncodeToString(secret[:]),
		UuidEnc:  secret[:],
	})
	if err != nil {
		t.Fatalf("CreateIntegration (%s): %v", name, err)
	}
	return integration
}

// doPostTasksRequest шлёт POST /tasks через httptest поверх router с
// Bearer-токеном и заголовком Idempotency-Key.
func doPostTasksRequest(t *testing.T, router http.Handler, bearerToken, idempotencyKey string, body any) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal тела: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", idempotencyKey)
	if bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+bearerToken)
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

// TestIntegration_PostTasks_HappyPath — приёмочный сценарий тикета 5.3:
// POST /tasks → 201, status=="queued"; БД видит tasks.status='queued' и
// task_events(status_change); Redpanda содержит ровно один конверт
// type=task_assigned в machine.commands с правильными данными (FR E1, E4).
func TestIntegration_PostTasks_HappyPath(t *testing.T) {
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

	user, token := createTestUserWithToken(ctx, t, q, "alice-tasks-happy")
	integration := createTestIntegration(ctx, t, q, user.ID, "alice-machine")

	server := api.NewServer(q, nil, []byte(testJWTSigningKey), []byte(testEncryptionKey32))
	server.SetTransitioner(task.NewTransitioner(pool))
	server.SetCommandPublisher(producer)
	router := api.NewRouter(server)

	const text = "запусти go test ./..."
	integrationID := uuid.UUID(integration.ID.Bytes)
	rec := doPostTasksRequest(t, router, token, "happy-key-1", api.TaskCreate{
		IntegrationId: integrationID,
		Text:          text,
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("статус = %d (%s), ожидался 201", rec.Code, rec.Body.String())
	}

	var created api.Task
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("unmarshal тела: %v", err)
	}
	if created.Status == nil || *created.Status != api.TaskStatus("queued") {
		t.Fatalf("status = %v, ожидался queued", created.Status)
	}
	if created.Id == nil {
		t.Fatal("id пуст")
	}
	taskID := *created.Id

	// БД: tasks.status == 'queued'.
	var dbStatus string
	if err := pool.QueryRow(ctx, `SELECT status FROM tasks WHERE id = $1`, taskID).Scan(&dbStatus); err != nil {
		t.Fatalf("SELECT tasks.status: %v", err)
	}
	if dbStatus != "queued" {
		t.Fatalf("tasks.status в БД = %q, ожидался queued", dbStatus)
	}

	// БД: task_events содержит запись status_change (история, FR H1).
	var eventCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM task_events WHERE task_id = $1 AND type = 'status_change'`, taskID).Scan(&eventCount); err != nil {
		t.Fatalf("SELECT task_events: %v", err)
	}
	if eventCount != 1 {
		t.Fatalf("task_events(status_change) для задачи = %d, ожидался 1", eventCount)
	}
	t.Logf("OK: БД видит task %s в статусе queued с записью истории status_change", taskID)

	// Redpanda: ровно один конверт task_assigned для этой задачи в machine.commands.
	const group = "tasks-it-happy"
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
	if env.Type != bus.MessageTypeTaskAssigned {
		t.Fatalf("env.Type = %q, ожидался %q", env.Type, bus.MessageTypeTaskAssigned)
	}
	if env.IntegrationID != integrationID.String() {
		t.Fatalf("env.IntegrationID = %q, ожидался %s", env.IntegrationID, integrationID)
	}
	if env.TaskID == nil || *env.TaskID != taskID.String() {
		t.Fatalf("env.TaskID = %v, ожидался %s", env.TaskID, taskID)
	}
	var payload bus.TaskAssignedPayload
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload.Text != text {
		t.Fatalf("payload.Text = %q, ожидался %q", payload.Text, text)
	}
	t.Logf("OK: Redpanda содержит ровно один task_assigned для task_id=%s", taskID)
}

// TestIntegration_PostTasks_ForeignIntegrationNotFound — POST /tasks с
// integration_id чужой интеграции → 404, не 403 (FR A4, I3, owner isolation —
// тот же принцип, что у GET/PATCH /integrations/{id}).
func TestIntegration_PostTasks_ForeignIntegrationNotFound(t *testing.T) {
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

	alice, _ := createTestUserWithToken(ctx, t, q, "alice-tasks-owner")
	aliceIntegration := createTestIntegration(ctx, t, q, alice.ID, "alice-machine-owner")
	_, bobToken := createTestUserWithToken(ctx, t, q, "bob-tasks-other")

	server := api.NewServer(q, nil, []byte(testJWTSigningKey), []byte(testEncryptionKey32))
	server.SetTransitioner(task.NewTransitioner(pool))
	server.SetCommandPublisher(producer)
	router := api.NewRouter(server)

	rec := doPostTasksRequest(t, router, bobToken, "bob-key-1", api.TaskCreate{
		IntegrationId: uuid.UUID(aliceIntegration.ID.Bytes),
		Text:          "чужая задача",
	})
	if rec.Code != http.StatusNotFound {
		t.Fatalf("статус = %d (%s), ожидался 404", rec.Code, rec.Body.String())
	}
	t.Logf("OK: POST /tasks на чужую интеграцию → 404")
}

// TestIntegration_PostTasks_DuplicateIdempotencyKeyConflicts — два POST
// /tasks подряд с одинаковым (user, Idempotency-Key) → первый 201, второй
// 409 (временное поведение до полноценного дедупа тикета 5.5).
func TestIntegration_PostTasks_DuplicateIdempotencyKeyConflicts(t *testing.T) {
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

	user, token := createTestUserWithToken(ctx, t, q, "alice-tasks-dup")
	integration := createTestIntegration(ctx, t, q, user.ID, "alice-machine-dup")

	server := api.NewServer(q, nil, []byte(testJWTSigningKey), []byte(testEncryptionKey32))
	server.SetTransitioner(task.NewTransitioner(pool))
	server.SetCommandPublisher(producer)
	router := api.NewRouter(server)

	const key = "duplicate-key-1"
	body := api.TaskCreate{IntegrationId: uuid.UUID(integration.ID.Bytes), Text: "первая задача"}

	first := doPostTasksRequest(t, router, token, key, body)
	if first.Code != http.StatusCreated {
		t.Fatalf("первый POST: статус = %d (%s), ожидался 201", first.Code, first.Body.String())
	}

	second := doPostTasksRequest(t, router, token, key, body)
	if second.Code != http.StatusConflict {
		t.Fatalf("второй POST (тот же Idempotency-Key): статус = %d (%s), ожидался 409", second.Code, second.Body.String())
	}
	t.Logf("OK: повтор Idempotency-Key → 409")
}
