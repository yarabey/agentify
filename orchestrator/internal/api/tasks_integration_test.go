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
	"github.com/jackc/pgx/v5/pgxpool"
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

// noopCommandPublisher — минимальный api.CommandPublisher для
// TestIntegration_PostTasksIdAnswer_TwoQuestionsCorrectBinding (тикет 6.2):
// этому тесту важны только HTTP-ответ и состояние реальной БД (tasks.status,
// task_events.ref_event_id), а не то, что именно попадёт в Redpanda —
// PostTasksIdAnswer публикует конверт user_answer уже ПОСЛЕ записи в БД
// (tasks.go), но при nil CommandPublisher отвечает 500 (см. godoc
// Server.commandPublisher в server.go), поэтому нужен хоть какой-то
// publisher; поднимать отдельный контейнер Redpanda ради этого не нужно —
// unit-тесты пакета api (tasks_test.go, fakePublisher) уже проверяют форму
// публикуемого конверта на fake-querier'ах, здесь это не предмет проверки.
type noopCommandPublisher struct{}

func (noopCommandPublisher) PublishKeyed(_ context.Context, _, _ string, _ bus.Envelope) error {
	return nil
}

// doPostTasksIdAnswerRequest шлёт POST /tasks/{id}/answer через httptest
// поверх router с Bearer-токеном — аналог doPostTasksRequest для эндпоинта
// ответа на вопрос агента (тикет 6.1/6.2).
func doPostTasksIdAnswerRequest(t *testing.T, router http.Handler, bearerToken string, taskID uuid.UUID, body any) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal тела: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/tasks/"+taskID.String()+"/answer", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	if bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+bearerToken)
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

// findAgentQuestionEventID ищет среди agent_question-событий задачи то,
// чей payload.question_id совпадает с искомым, и возвращает его event id —
// тот же алгоритм сопоставления, что и PostTasksIdAnswer (tasks.go), нужен
// здесь только чтобы ПОДГОТОВИТЬ (напрямую через Transitioner, в обход HTTP)
// состояние "два вопроса в истории" перед вызовом реального обработчика.
func findAgentQuestionEventID(ctx context.Context, t *testing.T, q *db.Queries, taskID pgtype.UUID, questionID uuid.UUID) pgtype.UUID {
	t.Helper()
	events, err := q.ListAgentQuestionEventsByTask(ctx, taskID)
	if err != nil {
		t.Fatalf("ListAgentQuestionEventsByTask: %v", err)
	}
	wanted := questionID.String()
	for _, e := range events {
		var payload bus.AgentQuestionPayload
		if uerr := json.Unmarshal(e.PayloadEnc, &payload); uerr != nil {
			continue
		}
		if payload.QuestionID == wanted {
			return e.ID
		}
	}
	t.Fatalf("agent_question с question_id=%s не найден среди событий задачи", wanted)
	return pgtype.UUID{}
}

// TestIntegration_PostTasksIdAnswer_TwoQuestionsCorrectBinding — приёмка
// тикета 6.2 (FR F2, §5 «Несколько вопросов сопоставляются корректно»):
// прогоняем задачу через ДВА реальных цикла вопрос/агент (первый цикл
// полностью завершён running→waiting_user→running, второй остановлен на
// waiting_user — вопрос q2 задан, ещё не отвечен) на настоящем Postgres, а
// затем отвечаем на q2 через РЕАЛЬНЫЙ HTTP-хендлер PostTasksIdAnswer (не
// fake-querier, в отличие от TestPostTasksIdAnswer_MatchesSpecificQuestionAmongMultiple
// в tasks_test.go, который проверяет тот же алгоритм сопоставления на
// моках) — проверяем 202, tasks.status в БД == running, и что ref_event_id
// новой записи task_events(user_answer) в БД указывает ИМЕННО на event id
// вопроса q2, а неq1.
func TestIntegration_PostTasksIdAnswer_TwoQuestionsCorrectBinding(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, doneDB := setupDB(ctx, t)
	defer doneDB()
	q := db.New(pool)

	user, token := createTestUserWithToken(ctx, t, q, "alice-tasks-two-questions")
	integration := createTestIntegration(ctx, t, q, user.ID, "alice-machine-two-questions")

	idempotencyKey := "two-questions-key-1"
	taskRow, err := q.CreateTask(ctx, db.CreateTaskParams{
		UserID:         user.ID,
		IntegrationID:  integration.ID,
		TextEnc:        []byte("сделай две вещи по очереди"),
		IdempotencyKey: &idempotencyKey,
	})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	taskID := taskRow.ID

	tr := task.NewTransitioner(pool)

	// Довести задачу до running обычным Transition (created→queued→running).
	for _, trigger := range []task.Trigger{task.TriggerEnqueued, task.TriggerTaskAccepted} {
		if _, _, terr := tr.Transition(ctx, taskID, trigger); terr != nil {
			t.Fatalf("подготовка (%s): %v", trigger, terr)
		}
	}

	// --- Цикл 1: вопрос q1, ПОЛНОСТЬЮ отвечен (running→waiting_user→running),
	// напрямую через Transitioner (в обход HTTP — здесь только подготовка
	// истории, не предмет проверки этого теста).
	question1ID := uuid.New()
	question1Payload, merr := json.Marshal(bus.AgentQuestionPayload{QuestionID: question1ID.String(), Text: "продолжать с первым шагом?"})
	if merr != nil {
		t.Fatalf("marshal AgentQuestionPayload (q1): %v", merr)
	}
	if _, _, terr := tr.TransitionWithEvent(ctx, taskID, task.TriggerAgentQuestion, "agent_question", pgtype.UUID{}, question1Payload); terr != nil {
		t.Fatalf("подготовка: TransitionWithEvent(agent_question q1): %v", terr)
	}
	question1EventID := findAgentQuestionEventID(ctx, t, q, taskID, question1ID)

	answer1Payload, merr := json.Marshal(bus.UserAnswerPayload{QuestionID: question1ID.String(), Text: "да, первым шагом"})
	if merr != nil {
		t.Fatalf("marshal UserAnswerPayload (q1): %v", merr)
	}
	if _, _, terr := tr.TransitionWithEvent(ctx, taskID, task.TriggerUserAnswered, "user_answer", question1EventID, answer1Payload); terr != nil {
		t.Fatalf("подготовка: TransitionWithEvent(user_answer q1): %v", terr)
	}

	// --- Цикл 2: вопрос q2 задан (running→waiting_user), НЕ отвечен —
	// задача сейчас ждёт ответа именно на q2.
	question2ID := uuid.New()
	question2Payload, merr := json.Marshal(bus.AgentQuestionPayload{QuestionID: question2ID.String(), Text: "продолжать со вторым шагом?"})
	if merr != nil {
		t.Fatalf("marshal AgentQuestionPayload (q2): %v", merr)
	}
	if _, _, terr := tr.TransitionWithEvent(ctx, taskID, task.TriggerAgentQuestion, "agent_question", pgtype.UUID{}, question2Payload); terr != nil {
		t.Fatalf("подготовка: TransitionWithEvent(agent_question q2): %v", terr)
	}
	question2EventID := findAgentQuestionEventID(ctx, t, q, taskID, question2ID)
	if question2EventID == question1EventID {
		t.Fatalf("id вопроса q2 совпал с id вопроса q1 = %v, ожидались разные события", question1EventID)
	}

	before := getTaskRowStatus(ctx, t, pool, taskID)
	if before != string(task.StatusWaitingUser) {
		t.Fatalf("подготовка: tasks.status = %s, хотим waiting_user (задача ждёт ответа на q2)", before)
	}

	// --- Теперь настоящий HTTP-запрос: отвечаем ИМЕННО на q2. ---
	server := api.NewServer(q, nil, []byte(testJWTSigningKey), []byte(testEncryptionKey32))
	server.SetTransitioner(tr)
	server.SetCommandPublisher(noopCommandPublisher{})
	router := api.NewRouter(server)

	rec := doPostTasksIdAnswerRequest(t, router, token, uuid.UUID(taskID.Bytes), api.PostTasksIdAnswerJSONBody{
		QuestionId: question2ID,
		Text:       "да, вторым шагом тоже",
	})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("статус = %d (%s), ожидался 202", rec.Code, rec.Body.String())
	}

	// БД: tasks.status снова 'running' (waiting_user → running).
	after := getTaskRowStatus(ctx, t, pool, taskID)
	if after != string(task.StatusRunning) {
		t.Fatalf("tasks.status в БД = %s, ожидался running", after)
	}

	// БД: РОВНО одна новая запись user_answer, ссылающаяся именно на q2 (не q1).
	rows, err := pool.Query(ctx, `SELECT id, ref_event_id FROM task_events WHERE task_id = $1 AND type = 'user_answer' ORDER BY seq`, taskID)
	if err != nil {
		t.Fatalf("SELECT task_events(user_answer): %v", err)
	}
	defer rows.Close()

	var refEventIDs []pgtype.UUID
	for rows.Next() {
		var id, refEventID pgtype.UUID
		if serr := rows.Scan(&id, &refEventID); serr != nil {
			t.Fatalf("сканировать task_events(user_answer): %v", serr)
		}
		refEventIDs = append(refEventIDs, refEventID)
	}
	if rerr := rows.Err(); rerr != nil {
		t.Fatalf("итерация task_events(user_answer): %v", rerr)
	}

	if len(refEventIDs) != 2 {
		t.Fatalf("ожидалось 2 записи task_events(user_answer) в БД (q1 из подготовки + q2 из HTTP-запроса), получено %d", len(refEventIDs))
	}
	if refEventIDs[0] != question1EventID {
		t.Fatalf("ref_event_id первого user_answer = %v, хотим id вопроса q1 = %v", refEventIDs[0], question1EventID)
	}
	if refEventIDs[1] != question2EventID {
		t.Fatalf("ref_event_id второго (нового, из HTTP-ответа) user_answer = %v, хотим id вопроса q2 = %v — ответ не должен привязаться к q1", refEventIDs[1], question2EventID)
	}
	t.Logf("OK: 202, tasks.status=running, ответ на q2 привязан именно к q2 (ref_event_id=%v), не к q1 (%v)", question2EventID, question1EventID)
}

// getTaskRowStatus — минимальный SELECT tasks.status по id, для проверок
// TestIntegration_PostTasksIdAnswer_TwoQuestionsCorrectBinding, где полный
// снимок taskRow (пакет task, internal/task) недоступен пакету api_test.
func getTaskRowStatus(ctx context.Context, t *testing.T, pool *pgxpool.Pool, taskID pgtype.UUID) string {
	t.Helper()
	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM tasks WHERE id = $1`, taskID).Scan(&status); err != nil {
		t.Fatalf("SELECT tasks.status: %v", err)
	}
	return status
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
