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
//   - дедуп (тикет 5.5, FR E7, §4 «Защита от двойной отправки»): повторный
//     POST /tasks с тем же (user, Idempotency-Key) → 200 с ТЕМ ЖЕ id задачи
//     (не 409, не новый 201), в БД ровно одна строка tasks на этот ключ, и
//     количество task_events не растёт от повтора;
//   - разные Idempotency-Key той же интеграции → две разные задачи (дедуп не
//     блокирует легитимные повторные постановки).
//
// Также покрывает состав истории — GET /tasks, GET /tasks/{id},
// GET /tasks/{id}/events (тикет 8.6, FR H1, Gherkin §10 «Состав записи о
// задаче»): полный цикл FSM → все поля Task/TaskEvent присутствуют; чужая и
// несуществующая задача → 404 одинаково для карточки и журнала; фильтрация
// GetTasks по integration_id/status. Эти тесты не поднимают Redpanda — сами
// хендлеры GetTasks/GetTasksId/GetTasksIdEvents читают только БД.
//
// Дополнительно покрывает бессрочное хранение (тикет 8.7, FR I2, Gherkin §10
// «Бессрочное хранение»): задача, заведомо завершённая давно (created_at/
// updated_at и все task_events.created_at принудительно перенесены в 2023
// год напрямую в БД, в обход бизнес-логики), остаётся полностью доступна
// через GET /tasks, GET /tasks/{id} и GET /tasks/{id}/events — ни карточка,
// ни журнал не урезаются и не скрываются по возрасту (авто-удаления в
// кодовой базе нет и не предполагается).
//
// Также покрывает семантику правки текста запроса (тикет 8.9, FR H3): в
// проекте НЕТ эндпоинта для редактирования text уже созданной задачи (ни
// PATCH/PUT /tasks/{id}, ни UPDATE tasks ... SET text_enc = ... в
// queries/tasks.sql — там UPDATE tasks трогает только status/updated_at, см.
// UpdateTaskStatus) — text_enc пишется единственный раз, в CreateTask (INSERT).
// Семантика FR H3 «правка не перезапускает уже выполненную задачу, повтор —
// отдельное действие» тем самым обеспечена конструктивно: единственный способ
// «исправить» уже отправленный текст — это создать НОВУЮ задачу отдельным
// POST /tasks со своим Idempotency-Key (тикет 5.5 гарантирует независимость
// таких задач). TestIntegration_PostTasks_ResubmitDoesNotRestartCompletedTask
// закрепляет это регрессионно: задача доводится до completed, затем
// «правка» моделируется как повторная постановка (второй POST /tasks с
// исправленным текстом) — оригинальная завершённая задача остаётся
// completed, её text/updated_at и число task_events не меняются.
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

// backdateTaskAndEvents — прямой UPDATE tasks.created_at/updated_at и
// task_events.created_at, минуя бизнес-логику (эмуляция «задача завершена
// давно» для теста бессрочного хранения, тикет 8.7, FR I2) — тот же приём,
// что setIntegrationLastSeenAt в stale_worker_integration_test.go (пакет
// task_test), только для задач/событий вместо last_seen_at интеграции.
func backdateTaskAndEvents(ctx context.Context, t *testing.T, pool *pgxpool.Pool, taskID pgtype.UUID, at time.Time) {
	t.Helper()
	tag, err := pool.Exec(ctx, `UPDATE tasks SET created_at = $1, updated_at = $1 WHERE id = $2`, at, taskID)
	if err != nil {
		t.Fatalf("backdate tasks: %v", err)
	}
	if tag.RowsAffected() != 1 {
		t.Fatalf("backdate tasks затронул %d строк, ожидалась 1", tag.RowsAffected())
	}
	if _, err := pool.Exec(ctx, `UPDATE task_events SET created_at = $1 WHERE task_id = $2`, at, taskID); err != nil {
		t.Fatalf("backdate task_events: %v", err)
	}
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

// countTasksByUserAndIdempotencyKey — сколько строк tasks существует для
// (user_id, idempotency_key); используется TestIntegration_PostTasks_
// DuplicateIdempotencyKeyReturnsExisting, чтобы доказать, что повтор
// постановки НЕ создаёт вторую строку (тикет 5.5, уникальный индекс
// uq_tasks_idempotency — источник истины, обработчик лишь реагирует на его
// коллизию).
func countTasksByUserAndIdempotencyKey(ctx context.Context, t *testing.T, pool *pgxpool.Pool, userID pgtype.UUID, idempotencyKey string) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM tasks WHERE user_id = $1 AND idempotency_key = $2`, userID, idempotencyKey).Scan(&count); err != nil {
		t.Fatalf("SELECT count(*) FROM tasks: %v", err)
	}
	return count
}

// countTaskEvents — сколько записей task_events существует для задачи;
// используется, чтобы доказать, что повторная постановка не пишет лишний
// status_change (тикет 5.5: повтор не вызывает Transition повторно).
func countTaskEvents(ctx context.Context, t *testing.T, pool *pgxpool.Pool, taskID uuid.UUID) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM task_events WHERE task_id = $1`, taskID).Scan(&count); err != nil {
		t.Fatalf("SELECT count(*) FROM task_events: %v", err)
	}
	return count
}

// TestIntegration_PostTasks_DuplicateIdempotencyKeyReturnsExisting — приёмка
// тикета 5.5 (FR E7, §4 «Защита от двойной отправки»): два POST /tasks
// подряд с одинаковым (user, Idempotency-Key), через РЕАЛЬНЫЙ HTTP на
// настоящем Postgres+Redpanda. Тело второго запроса намеренно ОТЛИЧАЕТСЯ
// текстом — дедуп срабатывает только по (user_id, idempotency_key), не по
// содержимому.
//
// Проверяется: первый ответ 201 (status=queued); второй ответ 200 (НЕ 409,
// НЕ 201) с ТЕМ ЖЕ id задачи, что и первый; в БД ровно одна строка tasks для
// этого (user_id, idempotency_key); количество task_events этой задачи не
// выросло между первым и вторым запросом (повтор не порождает лишний
// status_change, не вызывает Transition/публикацию повторно).
func TestIntegration_PostTasks_DuplicateIdempotencyKeyReturnsExisting(t *testing.T) {
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
	integrationID := uuid.UUID(integration.ID.Bytes)

	first := doPostTasksRequest(t, router, token, key, api.TaskCreate{IntegrationId: integrationID, Text: "первая задача"})
	if first.Code != http.StatusCreated {
		t.Fatalf("первый POST: статус = %d (%s), ожидался 201", first.Code, first.Body.String())
	}
	var firstTask api.Task
	if err := json.Unmarshal(first.Body.Bytes(), &firstTask); err != nil {
		t.Fatalf("unmarshal тела первого ответа: %v", err)
	}
	if firstTask.Id == nil {
		t.Fatal("id первого ответа пуст")
	}
	if firstTask.Status == nil || *firstTask.Status != api.TaskStatus("queued") {
		t.Fatalf("status первого ответа = %v, ожидался queued", firstTask.Status)
	}
	taskID := *firstTask.Id

	eventsAfterFirst := countTaskEvents(ctx, t, pool, taskID)

	// Тело второго запроса намеренно другое — дедуп не должен зависеть от
	// содержимого, только от (user_id, idempotency_key).
	second := doPostTasksRequest(t, router, token, key, api.TaskCreate{IntegrationId: integrationID, Text: "другой текст в повторной постановке"})
	if second.Code != http.StatusOK {
		t.Fatalf("второй POST (тот же Idempotency-Key): статус = %d (%s), ожидался 200", second.Code, second.Body.String())
	}
	var secondTask api.Task
	if err := json.Unmarshal(second.Body.Bytes(), &secondTask); err != nil {
		t.Fatalf("unmarshal тела второго ответа: %v", err)
	}
	if secondTask.Id == nil || *secondTask.Id != taskID {
		t.Fatalf("id второго ответа = %v, ожидался тот же, что у первого = %s", secondTask.Id, taskID)
	}

	// В БД ровно одна строка на этот (user_id, idempotency_key) — дубль не создан.
	if n := countTasksByUserAndIdempotencyKey(ctx, t, pool, user.ID, key); n != 1 {
		t.Fatalf("count(*) FROM tasks WHERE user_id/idempotency_key = %d, ожидался 1 (дубль не должен создаваться)", n)
	}

	// task_events не выросли — повтор не вызвал Transition/status_change заново.
	if n := countTaskEvents(ctx, t, pool, taskID); n != eventsAfterFirst {
		t.Fatalf("task_events для задачи после повтора = %d, ожидалось без изменений (%d) — повтор не должен писать новую историю", n, eventsAfterFirst)
	}

	t.Logf("OK: повтор Idempotency-Key → 200 с той же задачей %s, дубль в БД не создан, task_events не выросли", taskID)
}

// TestIntegration_PostTasks_DifferentIdempotencyKeysCreateDistinctTasks —
// разные Idempotency-Key той же интеграции/пользователя создают ДВЕ разные
// задачи (тикет 5.5): дедуп не должен блокировать легитимные повторные
// постановки, только буквальный повтор того же ключа.
func TestIntegration_PostTasks_DifferentIdempotencyKeysCreateDistinctTasks(t *testing.T) {
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

	user, token := createTestUserWithToken(ctx, t, q, "alice-tasks-distinct-keys")
	integration := createTestIntegration(ctx, t, q, user.ID, "alice-machine-distinct-keys")
	integrationID := uuid.UUID(integration.ID.Bytes)

	server := api.NewServer(q, nil, []byte(testJWTSigningKey), []byte(testEncryptionKey32))
	server.SetTransitioner(task.NewTransitioner(pool))
	server.SetCommandPublisher(producer)
	router := api.NewRouter(server)

	first := doPostTasksRequest(t, router, token, "distinct-key-1", api.TaskCreate{IntegrationId: integrationID, Text: "первая задача"})
	if first.Code != http.StatusCreated {
		t.Fatalf("первый POST: статус = %d (%s), ожидался 201", first.Code, first.Body.String())
	}
	second := doPostTasksRequest(t, router, token, "distinct-key-2", api.TaskCreate{IntegrationId: integrationID, Text: "вторая задача"})
	if second.Code != http.StatusCreated {
		t.Fatalf("второй POST (другой Idempotency-Key): статус = %d (%s), ожидался 201", second.Code, second.Body.String())
	}

	var firstTask, secondTask api.Task
	if err := json.Unmarshal(first.Body.Bytes(), &firstTask); err != nil {
		t.Fatalf("unmarshal тела первого ответа: %v", err)
	}
	if err := json.Unmarshal(second.Body.Bytes(), &secondTask); err != nil {
		t.Fatalf("unmarshal тела второго ответа: %v", err)
	}
	if firstTask.Id == nil || secondTask.Id == nil {
		t.Fatal("id одного из ответов пуст")
	}
	if *firstTask.Id == *secondTask.Id {
		t.Fatalf("разные Idempotency-Key дали одну и ту же задачу %s — дедуп не должен блокировать разные постановки", *firstTask.Id)
	}
	t.Logf("OK: разные Idempotency-Key → две разные задачи (%s, %s)", *firstTask.Id, *secondTask.Id)
}

// TestIntegration_GetTasks_FullCycleAllFieldsPresent — приёмка тикета 8.6 (FR
// H1, Gherkin §10 «Состав записи о задаче»), буквальное требование теста
// «full cycle → all fields present»: задача проводится через полный цикл FSM
// (created→queued→running→waiting_user→running→awaiting_confirm→completed) с
// одним вопросом/ответом и агентским завершением напрямую через Transitioner
// (в обход HTTP — это только подготовка истории, не предмет проверки), а
// затем ЧЕРЕЗ РЕАЛЬНЫЕ HTTP-хендлеры GetTasks/GetTasksId/GetTasksIdEvents
// проверяется, что все поля контракта Task/TaskEvent присутствуют и
// корректны (Redpanda для этого не нужна — сами хендлеры читают только БД).
func TestIntegration_GetTasks_FullCycleAllFieldsPresent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, doneDB := setupDB(ctx, t)
	defer doneDB()
	q := db.New(pool)

	user, token := createTestUserWithToken(ctx, t, q, "alice-tasks-history")
	integration := createTestIntegration(ctx, t, q, user.ID, "alice-machine-history")

	idempotencyKey := "history-key-1"
	const text = "собери отчёт"
	taskRow, err := q.CreateTask(ctx, db.CreateTaskParams{
		UserID:         user.ID,
		IntegrationID:  integration.ID,
		TextEnc:        []byte(text),
		IdempotencyKey: &idempotencyKey,
	})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	taskID := taskRow.ID

	tr := task.NewTransitioner(pool)

	// created → queued → running.
	for _, trigger := range []task.Trigger{task.TriggerEnqueued, task.TriggerTaskAccepted} {
		if _, _, terr := tr.Transition(ctx, taskID, trigger); terr != nil {
			t.Fatalf("подготовка (%s): %v", trigger, terr)
		}
	}

	// running → waiting_user: агент задаёт вопрос.
	questionID := uuid.New()
	questionPayload, merr := json.Marshal(bus.AgentQuestionPayload{QuestionID: questionID.String(), Text: "продолжать?"})
	if merr != nil {
		t.Fatalf("marshal AgentQuestionPayload: %v", merr)
	}
	if _, _, terr := tr.TransitionWithEvent(ctx, taskID, task.TriggerAgentQuestion, "agent_question", pgtype.UUID{}, questionPayload); terr != nil {
		t.Fatalf("подготовка: TransitionWithEvent(agent_question): %v", terr)
	}
	questionEventID := findAgentQuestionEventID(ctx, t, q, taskID, questionID)

	// waiting_user → running: пользователь отвечает.
	const answerText = "да, продолжай"
	answerPayload, merr := json.Marshal(bus.UserAnswerPayload{QuestionID: questionID.String(), Text: answerText})
	if merr != nil {
		t.Fatalf("marshal UserAnswerPayload: %v", merr)
	}
	if _, _, terr := tr.TransitionWithEvent(ctx, taskID, task.TriggerUserAnswered, "user_answer", questionEventID, answerPayload); terr != nil {
		t.Fatalf("подготовка: TransitionWithEvent(user_answer): %v", terr)
	}

	// running → awaiting_confirm: агент сообщает о завершении.
	const summaryText = "отчёт собран"
	completedPayload, merr := json.Marshal(bus.AgentCompletedPayload{Summary: summaryText})
	if merr != nil {
		t.Fatalf("marshal AgentCompletedPayload: %v", merr)
	}
	if _, _, terr := tr.TransitionWithEvent(ctx, taskID, task.TriggerAgentCompleted, "agent_completed", pgtype.UUID{}, completedPayload); terr != nil {
		t.Fatalf("подготовка: TransitionWithEvent(agent_completed): %v", terr)
	}

	// awaiting_confirm → completed: пользователь подтверждает.
	if _, _, terr := tr.Transition(ctx, taskID, task.TriggerUserConfirmed); terr != nil {
		t.Fatalf("подготовка: Transition(user_confirmed): %v", terr)
	}

	server := api.NewServer(q, nil, []byte(testJWTSigningKey), []byte(testEncryptionKey32))
	router := api.NewRouter(server)

	taskUUID := uuid.UUID(taskID.Bytes)
	integrationUUID := uuid.UUID(integration.ID.Bytes)

	// --- GET /tasks/{id}: карточка задачи, все поля Task присутствуют. ---
	recCard := doIntegrationsRequest(t, router, http.MethodGet, "/tasks/"+taskUUID.String(), token, nil)
	if recCard.Code != http.StatusOK {
		t.Fatalf("GET /tasks/{id}: статус = %d (%s), ожидался 200", recCard.Code, recCard.Body.String())
	}
	var card api.Task
	if uerr := json.Unmarshal(recCard.Body.Bytes(), &card); uerr != nil {
		t.Fatalf("unmarshal GET /tasks/{id}: %v", uerr)
	}
	if card.Id == nil || *card.Id != taskUUID {
		t.Fatalf("Task.Id = %v, ожидался %s", card.Id, taskUUID)
	}
	if card.IntegrationId == nil || *card.IntegrationId != integrationUUID {
		t.Fatalf("Task.IntegrationId = %v, ожидался %s", card.IntegrationId, integrationUUID)
	}
	if card.Text == nil || *card.Text != text {
		t.Fatalf("Task.Text = %v, ожидался %q", card.Text, text)
	}
	if card.Status == nil || *card.Status != api.Completed {
		t.Fatalf("Task.Status = %v, ожидался completed", card.Status)
	}
	if card.CreatedAt == nil || card.CreatedAt.IsZero() {
		t.Fatal("Task.CreatedAt пуст")
	}
	if card.UpdatedAt == nil || card.UpdatedAt.IsZero() {
		t.Fatal("Task.UpdatedAt пуст")
	}
	if card.UpdatedAt.Before(*card.CreatedAt) {
		t.Fatalf("Task.UpdatedAt (%v) раньше Task.CreatedAt (%v)", card.UpdatedAt, card.CreatedAt)
	}
	t.Logf("OK: GET /tasks/{id} — все поля Task присутствуют и корректны")

	// --- GET /tasks: список содержит эту задачу с теми же полями. ---
	recList := doIntegrationsRequest(t, router, http.MethodGet, "/tasks", token, nil)
	if recList.Code != http.StatusOK {
		t.Fatalf("GET /tasks: статус = %d (%s), ожидался 200", recList.Code, recList.Body.String())
	}
	var list []api.Task
	if uerr := json.Unmarshal(recList.Body.Bytes(), &list); uerr != nil {
		t.Fatalf("unmarshal GET /tasks: %v", uerr)
	}
	var found *api.Task
	for i := range list {
		if list[i].Id != nil && *list[i].Id == taskUUID {
			found = &list[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("задача %s не найдена в GET /tasks (получено %d задач)", taskUUID, len(list))
	}
	if found.Status == nil || *found.Status != api.Completed {
		t.Fatalf("в списке Task.Status = %v, ожидался completed", found.Status)
	}
	t.Logf("OK: GET /tasks содержит задачу с корректным статусом")

	// --- GET /tasks/{id}/events: полный журнал, по порядку seq, все поля. ---
	recEvents := doIntegrationsRequest(t, router, http.MethodGet, "/tasks/"+taskUUID.String()+"/events", token, nil)
	if recEvents.Code != http.StatusOK {
		t.Fatalf("GET /tasks/{id}/events: статус = %d (%s), ожидался 200", recEvents.Code, recEvents.Body.String())
	}
	var events []api.TaskEvent
	if uerr := json.Unmarshal(recEvents.Body.Bytes(), &events); uerr != nil {
		t.Fatalf("unmarshal GET /tasks/{id}/events: %v", uerr)
	}
	// Ожидаемая последовательность типов (см. transition.go: transition
	// пишет доп. событие ПЕРЕД status_change для каждого перехода, которому
	// оно передано):
	//   enqueued        → status_change
	//   task_accepted   → status_change
	//   agent_question  → agent_question, status_change
	//   user_answered   → user_answer, status_change
	//   agent_completed → agent_completed, status_change
	//   user_confirmed  → status_change
	wantTypes := []api.TaskEventType{
		api.TaskEventTypeStatusChange,
		api.TaskEventTypeStatusChange,
		api.TaskEventTypeAgentQuestion,
		api.TaskEventTypeStatusChange,
		api.TaskEventTypeUserAnswer,
		api.TaskEventTypeStatusChange,
		api.TaskEventTypeAgentCompleted,
		api.TaskEventTypeStatusChange,
		api.TaskEventTypeStatusChange,
	}
	if len(events) != len(wantTypes) {
		t.Fatalf("количество событий = %d, ожидалось %d: %+v", len(events), len(wantTypes), events)
	}
	var lastSeq int
	var questionEvent, answerEvent, completedEvent *api.TaskEvent
	for i, e := range events {
		if e.Id == nil {
			t.Fatalf("событие #%d: Id пуст", i)
		}
		if e.Seq == nil {
			t.Fatalf("событие #%d: Seq пуст", i)
		}
		if *e.Seq <= lastSeq {
			t.Fatalf("событие #%d: seq = %d, не возрастает относительно предыдущего %d — журнал не по порядку", i, *e.Seq, lastSeq)
		}
		lastSeq = *e.Seq
		if e.Type == nil {
			t.Fatalf("событие #%d: Type пуст", i)
		}
		if *e.Type != wantTypes[i] {
			t.Fatalf("событие #%d: Type = %q, ожидался %q", i, *e.Type, wantTypes[i])
		}
		if e.CreatedAt == nil || e.CreatedAt.IsZero() {
			t.Fatalf("событие #%d: CreatedAt пуст", i)
		}
		if e.Payload == nil {
			t.Fatalf("событие #%d (%s): Payload пуст, ожидался непустой JSON", i, *e.Type)
		}
		switch *e.Type {
		case api.TaskEventTypeAgentQuestion:
			questionEvent = &events[i]
		case api.TaskEventTypeUserAnswer:
			answerEvent = &events[i]
		case api.TaskEventTypeAgentCompleted:
			completedEvent = &events[i]
		}
	}
	if questionEvent == nil || answerEvent == nil || completedEvent == nil {
		t.Fatal("не найдены все ожидаемые бизнес-события (agent_question/user_answer/agent_completed) в журнале")
	}
	if got := (*questionEvent.Payload)["question_id"]; got != questionID.String() {
		t.Fatalf("payload agent_question.question_id = %v, ожидался %s", got, questionID)
	}
	if got := (*answerEvent.Payload)["question_id"]; got != questionID.String() {
		t.Fatalf("payload user_answer.question_id = %v, ожидался %s", got, questionID)
	}
	if got := (*answerEvent.Payload)["text"]; got != answerText {
		t.Fatalf("payload user_answer.text = %v, ожидался %q", got, answerText)
	}
	if got := (*completedEvent.Payload)["summary"]; got != summaryText {
		t.Fatalf("payload agent_completed.summary = %v, ожидался %q", got, summaryText)
	}
	t.Logf("OK: GET /tasks/{id}/events — %d событий по порядку seq, все поля присутствуют, ключевые payload корректны", len(events))
}

// TestIntegration_GetTasksId_ForeignAndNonexistentNotFound — 404 (не 403, не
// утечка существования — FR A4, I3, единый ответ «не найдено» для GetTasksId
// и GetTasksIdEvents, тикет 8.6): чужая задача и заведомо несуществующий id
// ведут себя одинаково.
func TestIntegration_GetTasksId_ForeignAndNonexistentNotFound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, doneDB := setupDB(ctx, t)
	defer doneDB()
	q := db.New(pool)

	alice, _ := createTestUserWithToken(ctx, t, q, "alice-tasks-history-owner")
	aliceIntegration := createTestIntegration(ctx, t, q, alice.ID, "alice-machine-history-owner")
	_, bobToken := createTestUserWithToken(ctx, t, q, "bob-tasks-history-other")

	idempotencyKey := "history-owner-key-1"
	aliceTask, err := q.CreateTask(ctx, db.CreateTaskParams{
		UserID:         alice.ID,
		IntegrationID:  aliceIntegration.ID,
		TextEnc:        []byte("задача алисы"),
		IdempotencyKey: &idempotencyKey,
	})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	server := api.NewServer(q, nil, []byte(testJWTSigningKey), []byte(testEncryptionKey32))
	router := api.NewRouter(server)

	aliceTaskUUID := uuid.UUID(aliceTask.ID.Bytes)
	nonexistentUUID := uuid.New()

	cases := []struct {
		name string
		id   uuid.UUID
	}{
		{"чужая задача", aliceTaskUUID},
		{"несуществующая задача", nonexistentUUID},
	}
	for _, tc := range cases {
		t.Run(tc.name+"/card", func(t *testing.T) {
			rec := doIntegrationsRequest(t, router, http.MethodGet, "/tasks/"+tc.id.String(), bobToken, nil)
			if rec.Code != http.StatusNotFound {
				t.Fatalf("GET /tasks/%s: статус = %d (%s), ожидался 404", tc.id, rec.Code, rec.Body.String())
			}
		})
		t.Run(tc.name+"/events", func(t *testing.T) {
			rec := doIntegrationsRequest(t, router, http.MethodGet, "/tasks/"+tc.id.String()+"/events", bobToken, nil)
			if rec.Code != http.StatusNotFound {
				t.Fatalf("GET /tasks/%s/events: статус = %d (%s), ожидался 404", tc.id, rec.Code, rec.Body.String())
			}
		})
	}
	t.Logf("OK: чужая задача и несуществующий id одинаково дают 404 для GetTasksId/GetTasksIdEvents")
}

// TestIntegration_GetTasks_FiltersByIntegrationAndStatus — GetTasks
// фильтрует по integration_id и по status независимо (тикет 8.6, FR H1,
// контракт GetTasksParams, api/openapi.yaml): две задачи одного пользователя,
// различающиеся и интеграцией, и статусом.
func TestIntegration_GetTasks_FiltersByIntegrationAndStatus(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, doneDB := setupDB(ctx, t)
	defer doneDB()
	q := db.New(pool)

	user, token := createTestUserWithToken(ctx, t, q, "alice-tasks-filters")
	integrationA := createTestIntegration(ctx, t, q, user.ID, "alice-machine-filters-a")
	integrationB := createTestIntegration(ctx, t, q, user.ID, "alice-machine-filters-b")

	keyA := "filters-key-a"
	taskA, err := q.CreateTask(ctx, db.CreateTaskParams{
		UserID:         user.ID,
		IntegrationID:  integrationA.ID,
		TextEnc:        []byte("задача A"),
		IdempotencyKey: &keyA,
	})
	if err != nil {
		t.Fatalf("CreateTask (A): %v", err)
	}
	keyB := "filters-key-b"
	taskB, err := q.CreateTask(ctx, db.CreateTaskParams{
		UserID:         user.ID,
		IntegrationID:  integrationB.ID,
		TextEnc:        []byte("задача B"),
		IdempotencyKey: &keyB,
	})
	if err != nil {
		t.Fatalf("CreateTask (B): %v", err)
	}

	tr := task.NewTransitioner(pool)
	if _, _, terr := tr.Transition(ctx, taskB.ID, task.TriggerEnqueued); terr != nil {
		t.Fatalf("перевести задачу B в queued: %v", terr)
	}
	// taskA остаётся в 'created' (без переходов) — статусы гарантированно разные.

	server := api.NewServer(q, nil, []byte(testJWTSigningKey), []byte(testEncryptionKey32))
	router := api.NewRouter(server)

	taskAUUID := uuid.UUID(taskA.ID.Bytes)
	taskBUUID := uuid.UUID(taskB.ID.Bytes)
	integrationAUUID := uuid.UUID(integrationA.ID.Bytes)

	// Фильтр по integration_id: только задача A.
	recByIntegration := doIntegrationsRequest(t, router, http.MethodGet, "/tasks?integration_id="+integrationAUUID.String(), token, nil)
	if recByIntegration.Code != http.StatusOK {
		t.Fatalf("GET /tasks?integration_id=...: статус = %d (%s), ожидался 200", recByIntegration.Code, recByIntegration.Body.String())
	}
	var byIntegration []api.Task
	if uerr := json.Unmarshal(recByIntegration.Body.Bytes(), &byIntegration); uerr != nil {
		t.Fatalf("unmarshal: %v", uerr)
	}
	if len(byIntegration) != 1 || byIntegration[0].Id == nil || *byIntegration[0].Id != taskAUUID {
		t.Fatalf("фильтр по integration_id вернул %+v, ожидалась ровно задача A (%s)", byIntegration, taskAUUID)
	}

	// Фильтр по status: только задача B ('queued').
	recByStatus := doIntegrationsRequest(t, router, http.MethodGet, "/tasks?status=queued", token, nil)
	if recByStatus.Code != http.StatusOK {
		t.Fatalf("GET /tasks?status=queued: статус = %d (%s), ожидался 200", recByStatus.Code, recByStatus.Body.String())
	}
	var byStatus []api.Task
	if uerr := json.Unmarshal(recByStatus.Body.Bytes(), &byStatus); uerr != nil {
		t.Fatalf("unmarshal: %v", uerr)
	}
	if len(byStatus) != 1 || byStatus[0].Id == nil || *byStatus[0].Id != taskBUUID {
		t.Fatalf("фильтр по status=queued вернул %+v, ожидалась ровно задача B (%s)", byStatus, taskBUUID)
	}

	// Без фильтров: обе задачи.
	recAll := doIntegrationsRequest(t, router, http.MethodGet, "/tasks", token, nil)
	if recAll.Code != http.StatusOK {
		t.Fatalf("GET /tasks: статус = %d (%s), ожидался 200", recAll.Code, recAll.Body.String())
	}
	var all []api.Task
	if uerr := json.Unmarshal(recAll.Body.Bytes(), &all); uerr != nil {
		t.Fatalf("unmarshal: %v", uerr)
	}
	if len(all) != 2 {
		t.Fatalf("GET /tasks без фильтров вернул %d задач, ожидалось 2", len(all))
	}
	t.Logf("OK: GetTasks фильтрует по integration_id и по status независимо")
}

// TestIntegration_GetTasks_OldTaskStillAccessible — приёмка тикета 8.7
// (FR I2, Gherkin §10 «Бессрочное хранение»): «Дано задача завершена давно /
// Когда я открываю историю / Тогда старая задача по-прежнему доступна / И
// авто-удаления не происходит». Авто-удаления/purge/retention/TTL для
// tasks/task_events в кодовой базе нет (ни в orchestrator/queries/tasks.sql,
// ни в воркерах internal/task, ни в миграциях, ни в deploy/*) — этот тест
// фиксирует свойство «доступ не ограничен возрастом» регрессионно: задача
// доводится до completed обычным Transitioner (как в
// TestIntegration_GetTasks_FullCycleAllFieldsPresent, без вопроса/ответа —
// здесь предмет проверки только возраст, не состав полей), затем её
// created_at/updated_at и created_at всех task_events принудительно
// переносятся в прошлое напрямую в БД (backdateTaskAndEvents, в обход
// бизнес-логики — эмуляция «задача завершена давно»). После этого через
// РЕАЛЬНЫЕ HTTP-хендлеры GetTasks/GetTasksId/GetTasksIdEvents проверяется,
// что задача и её полный журнал остаются полностью доступны — ни карточка,
// ни список, ни события не урезаны и не скрыты по возрасту.
func TestIntegration_GetTasks_OldTaskStillAccessible(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, doneDB := setupDB(ctx, t)
	defer doneDB()
	q := db.New(pool)

	user, token := createTestUserWithToken(ctx, t, q, "alice-tasks-old")
	integration := createTestIntegration(ctx, t, q, user.ID, "alice-machine-old")

	idempotencyKey := "old-task-key-1"
	const text = "древняя задача"
	taskRow, err := q.CreateTask(ctx, db.CreateTaskParams{
		UserID:         user.ID,
		IntegrationID:  integration.ID,
		TextEnc:        []byte(text),
		IdempotencyKey: &idempotencyKey,
	})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	taskID := taskRow.ID

	tr := task.NewTransitioner(pool)

	// Полный цикл до completed: created→queued→running→awaiting_confirm→completed.
	for _, trigger := range []task.Trigger{task.TriggerEnqueued, task.TriggerTaskAccepted} {
		if _, _, terr := tr.Transition(ctx, taskID, trigger); terr != nil {
			t.Fatalf("подготовка (%s): %v", trigger, terr)
		}
	}
	const summaryText = "готово давно"
	completedPayload, merr := json.Marshal(bus.AgentCompletedPayload{Summary: summaryText})
	if merr != nil {
		t.Fatalf("marshal AgentCompletedPayload: %v", merr)
	}
	if _, _, terr := tr.TransitionWithEvent(ctx, taskID, task.TriggerAgentCompleted, "agent_completed", pgtype.UUID{}, completedPayload); terr != nil {
		t.Fatalf("подготовка: TransitionWithEvent(agent_completed): %v", terr)
	}
	if _, _, terr := tr.Transition(ctx, taskID, task.TriggerUserConfirmed); terr != nil {
		t.Fatalf("подготовка: Transition(user_confirmed): %v", terr)
	}

	// Заведомо давняя дата — нулевые наносекунды, чтобы избежать проблем с
	// точностью postgres timestamptz при точном сравнении после round-trip
	// через JSON.
	oldTime := time.Date(2023, time.January, 15, 10, 0, 0, 0, time.UTC)
	backdateTaskAndEvents(ctx, t, pool, taskID, oldTime)

	server := api.NewServer(q, nil, []byte(testJWTSigningKey), []byte(testEncryptionKey32))
	router := api.NewRouter(server)

	taskUUID := uuid.UUID(taskID.Bytes)

	// --- GET /tasks/{id}: старая задача по-прежнему доступна как есть. ---
	recCard := doIntegrationsRequest(t, router, http.MethodGet, "/tasks/"+taskUUID.String(), token, nil)
	if recCard.Code != http.StatusOK {
		t.Fatalf("GET /tasks/{id}: статус = %d (%s), ожидался 200", recCard.Code, recCard.Body.String())
	}
	var card api.Task
	if uerr := json.Unmarshal(recCard.Body.Bytes(), &card); uerr != nil {
		t.Fatalf("unmarshal GET /tasks/{id}: %v", uerr)
	}
	if card.Id == nil || *card.Id != taskUUID {
		t.Fatalf("Task.Id = %v, ожидался %s", card.Id, taskUUID)
	}
	if card.Status == nil || *card.Status != api.Completed {
		t.Fatalf("Task.Status = %v, ожидался completed", card.Status)
	}
	if card.CreatedAt == nil || !card.CreatedAt.UTC().Equal(oldTime) {
		t.Fatalf("Task.CreatedAt = %v, ожидался %v (задача не должна выглядеть свежее, чем backdate)", card.CreatedAt, oldTime)
	}
	if card.UpdatedAt == nil || !card.UpdatedAt.UTC().Equal(oldTime) {
		t.Fatalf("Task.UpdatedAt = %v, ожидался %v", card.UpdatedAt, oldTime)
	}
	if age := time.Since(*card.CreatedAt); age < 365*24*time.Hour {
		t.Fatalf("time.Since(Task.CreatedAt) = %v, ожидалось явно «давно» (> года)", age)
	}
	t.Logf("OK: GET /tasks/{id} — задача, завершённая давно (%v), по-прежнему полностью доступна", oldTime)

	// --- GET /tasks: список НЕ скрывает и НЕ пропускает старую задачу. ---
	recList := doIntegrationsRequest(t, router, http.MethodGet, "/tasks", token, nil)
	if recList.Code != http.StatusOK {
		t.Fatalf("GET /tasks: статус = %d (%s), ожидался 200", recList.Code, recList.Body.String())
	}
	var list []api.Task
	if uerr := json.Unmarshal(recList.Body.Bytes(), &list); uerr != nil {
		t.Fatalf("unmarshal GET /tasks: %v", uerr)
	}
	var found *api.Task
	for i := range list {
		if list[i].Id != nil && *list[i].Id == taskUUID {
			found = &list[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("старая задача %s не найдена в GET /tasks (получено %d задач) — авто-удаление/сокрытие по возрасту недопустимо", taskUUID, len(list))
	}
	if found.CreatedAt == nil || !found.CreatedAt.UTC().Equal(oldTime) {
		t.Fatalf("в списке Task.CreatedAt = %v, ожидался %v", found.CreatedAt, oldTime)
	}
	t.Logf("OK: GET /tasks по-прежнему включает старую задачу")

	// --- GET /tasks/{id}/events: журнал не усечён и не скрыт по возрасту. ---
	recEvents := doIntegrationsRequest(t, router, http.MethodGet, "/tasks/"+taskUUID.String()+"/events", token, nil)
	if recEvents.Code != http.StatusOK {
		t.Fatalf("GET /tasks/{id}/events: статус = %d (%s), ожидался 200", recEvents.Code, recEvents.Body.String())
	}
	var events []api.TaskEvent
	if uerr := json.Unmarshal(recEvents.Body.Bytes(), &events); uerr != nil {
		t.Fatalf("unmarshal GET /tasks/{id}/events: %v", uerr)
	}
	// Ожидаемая последовательность типов (см. transition.go, тот же принцип,
	// что в TestIntegration_GetTasks_FullCycleAllFieldsPresent):
	//   enqueued        → status_change
	//   task_accepted   → status_change
	//   agent_completed → agent_completed, status_change
	//   user_confirmed  → status_change
	wantTypes := []api.TaskEventType{
		api.TaskEventTypeStatusChange,
		api.TaskEventTypeStatusChange,
		api.TaskEventTypeAgentCompleted,
		api.TaskEventTypeStatusChange,
		api.TaskEventTypeStatusChange,
	}
	if len(events) != len(wantTypes) {
		t.Fatalf("количество событий = %d, ожидалось %d (журнал не должен быть усечён по возрасту): %+v", len(events), len(wantTypes), events)
	}
	for i, e := range events {
		if e.Type == nil || *e.Type != wantTypes[i] {
			t.Fatalf("событие #%d: Type = %v, ожидался %q", i, e.Type, wantTypes[i])
		}
		if e.CreatedAt == nil || !e.CreatedAt.UTC().Equal(oldTime) {
			t.Fatalf("событие #%d (%s): CreatedAt = %v, ожидался %v — журнал не должен скрывать давние события", i, *e.Type, e.CreatedAt, oldTime)
		}
	}
	t.Logf("OK: GET /tasks/{id}/events — все %d событий давней задачи по-прежнему доступны, авто-удаления не произошло", len(events))
}

// TestIntegration_PostTasks_ResubmitDoesNotRestartCompletedTask — приёмка
// тикета 8.9 (FR H3, Gherkin — «Семантика правки текста запроса в истории
// определена явно: правка не перезапускает уже выполненную задачу; повтор —
// отдельное действие»).
//
// В проекте нет эндпоинта для редактирования text уже существующей задачи
// (api/openapi.yaml: у /tasks/{id} есть только GET; queries/tasks.sql: UPDATE
// tasks меняет только status/updated_at — UpdateTaskStatus, text_enc пишется
// один раз, в CreateTask). Поэтому единственный способ, которым пользователь
// может «исправить» уже отправленный текст, — отправить его снова отдельным
// POST /tasks со своим Idempotency-Key. Этот тест доводит первую задачу до
// completed (полный цикл через Transitioner, как в
// TestIntegration_GetTasks_FullCycleAllFieldsPresent, без вопроса/ответа —
// не предмет этого теста), затем через РЕАЛЬНЫЙ HTTP POST /tasks создаёт
// вторую задачу с «исправленным» текстом и другим Idempotency-Key, и
// проверяет: (1) это действительно НОВАЯ независимая задача (свой id,
// status=queued, свой text); (2) оригинальная задача НЕ перезапустилась —
// её status остаётся completed (не queued/running), text и updated_at не
// изменились, число task_events не выросло — правка/повтор не пишет новую
// историю и не трогает существующую задачу.
func TestIntegration_PostTasks_ResubmitDoesNotRestartCompletedTask(t *testing.T) {
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

	user, token := createTestUserWithToken(ctx, t, q, "alice-tasks-resubmit")
	integration := createTestIntegration(ctx, t, q, user.ID, "alice-machine-resubmit")
	integrationUUID := uuid.UUID(integration.ID.Bytes)

	server := api.NewServer(q, nil, []byte(testJWTSigningKey), []byte(testEncryptionKey32))
	tr := task.NewTransitioner(pool)
	server.SetTransitioner(tr)
	server.SetCommandPublisher(producer)
	router := api.NewRouter(server)

	// --- Оригинальная задача: POST /tasks → queued. ---
	const originalText = "собери отчёт по продажам за март"
	origRec := doPostTasksRequest(t, router, token, "orig-key", api.TaskCreate{
		IntegrationId: integrationUUID,
		Text:          originalText,
	})
	if origRec.Code != http.StatusCreated {
		t.Fatalf("исходный POST /tasks: статус = %d (%s), ожидался 201", origRec.Code, origRec.Body.String())
	}
	var origCreated api.Task
	if uerr := json.Unmarshal(origRec.Body.Bytes(), &origCreated); uerr != nil {
		t.Fatalf("unmarshal тела исходного ответа: %v", uerr)
	}
	if origCreated.Id == nil {
		t.Fatal("id исходной задачи пуст")
	}
	taskID := *origCreated.Id
	taskUUID := pgtype.UUID{Bytes: taskID, Valid: true}

	// Довести оригинальную задачу до completed: queued→running→awaiting_confirm→completed
	// (задача уже queued после POST — TriggerEnqueued уже применён обработчиком).
	if _, _, terr := tr.Transition(ctx, taskUUID, task.TriggerTaskAccepted); terr != nil {
		t.Fatalf("подготовка (task_accepted): %v", terr)
	}
	const summaryText = "отчёт готов"
	completedPayload, merr := json.Marshal(bus.AgentCompletedPayload{Summary: summaryText})
	if merr != nil {
		t.Fatalf("marshal AgentCompletedPayload: %v", merr)
	}
	if _, _, terr := tr.TransitionWithEvent(ctx, taskUUID, task.TriggerAgentCompleted, "agent_completed", pgtype.UUID{}, completedPayload); terr != nil {
		t.Fatalf("подготовка (agent_completed): %v", terr)
	}
	if _, _, terr := tr.Transition(ctx, taskUUID, task.TriggerUserConfirmed); terr != nil {
		t.Fatalf("подготовка (user_confirmed): %v", terr)
	}

	// --- Снимок «до правки»: карточка и число событий оригинальной задачи. ---
	beforeCardRec := doIntegrationsRequest(t, router, http.MethodGet, "/tasks/"+taskID.String(), token, nil)
	if beforeCardRec.Code != http.StatusOK {
		t.Fatalf("GET /tasks/{id} (до правки): статус = %d (%s), ожидался 200", beforeCardRec.Code, beforeCardRec.Body.String())
	}
	var beforeCard api.Task
	if uerr := json.Unmarshal(beforeCardRec.Body.Bytes(), &beforeCard); uerr != nil {
		t.Fatalf("unmarshal GET /tasks/{id} (до правки): %v", uerr)
	}
	if beforeCard.Status == nil || *beforeCard.Status != api.Completed {
		t.Fatalf("подготовка: Task.Status = %v, ожидался completed до имитации правки", beforeCard.Status)
	}
	if beforeCard.Text == nil || *beforeCard.Text != originalText {
		t.Fatalf("подготовка: Task.Text = %v, ожидался %q", beforeCard.Text, originalText)
	}
	if beforeCard.UpdatedAt == nil {
		t.Fatal("подготовка: Task.UpdatedAt пуст")
	}
	beforeUpdatedAt := *beforeCard.UpdatedAt
	eventsBeforeResubmit := countTaskEvents(ctx, t, pool, taskID)

	// --- «Правка» текста запроса: пользователь на самом деле создаёт НОВУЮ
	// задачу с исправленным текстом, отдельным POST /tasks со своим
	// Idempotency-Key — это и есть «повтор — отдельное действие» (FR H3). ---
	const correctedText = "собери отчёт по продажам за март (уточнение: только по региону Москва)"
	resubmitRec := doPostTasksRequest(t, router, token, "corrected-key", api.TaskCreate{
		IntegrationId: integrationUUID,
		Text:          correctedText,
	})
	if resubmitRec.Code != http.StatusCreated {
		t.Fatalf("«правка» (повторный POST /tasks): статус = %d (%s), ожидался 201", resubmitRec.Code, resubmitRec.Body.String())
	}
	var resubmitCreated api.Task
	if uerr := json.Unmarshal(resubmitRec.Body.Bytes(), &resubmitCreated); uerr != nil {
		t.Fatalf("unmarshal тела «правки»: %v", uerr)
	}
	if resubmitCreated.Id == nil {
		t.Fatal("id «правки» пуст")
	}
	secondTaskID := *resubmitCreated.Id
	if secondTaskID == taskID {
		t.Fatalf("«правка» вернула тот же id %s, что и оригинальная задача — ожидалась НОВАЯ независимая задача", taskID)
	}

	// --- Проверка: оригинальная задача НЕ перезапустилась и не изменилась. ---
	afterCardRec := doIntegrationsRequest(t, router, http.MethodGet, "/tasks/"+taskID.String(), token, nil)
	if afterCardRec.Code != http.StatusOK {
		t.Fatalf("GET /tasks/{id} (после «правки»): статус = %d (%s), ожидался 200", afterCardRec.Code, afterCardRec.Body.String())
	}
	var afterCard api.Task
	if uerr := json.Unmarshal(afterCardRec.Body.Bytes(), &afterCard); uerr != nil {
		t.Fatalf("unmarshal GET /tasks/{id} (после «правки»): %v", uerr)
	}
	if afterCard.Status == nil || *afterCard.Status != api.Completed {
		t.Fatalf("Task.Status оригинальной задачи после «правки» = %v, ожидался completed (правка не должна перезапускать выполненную задачу)", afterCard.Status)
	}
	if afterCard.Text == nil || *afterCard.Text != originalText {
		t.Fatalf("Task.Text оригинальной задачи после «правки» = %v, ожидался неизменный исходный текст %q (не текст правки)", afterCard.Text, originalText)
	}
	if afterCard.UpdatedAt == nil || !afterCard.UpdatedAt.Equal(beforeUpdatedAt) {
		t.Fatalf("Task.UpdatedAt оригинальной задачи изменился: было %v, стало %v — «правка» не должна трогать существующую задачу", beforeUpdatedAt, afterCard.UpdatedAt)
	}

	eventsAfterResubmit := countTaskEvents(ctx, t, pool, taskID)
	if eventsAfterResubmit != eventsBeforeResubmit {
		t.Fatalf("число task_events оригинальной задачи после «правки» = %d, было %d — повтор не должен писать новую историю в уже существующую задачу", eventsAfterResubmit, eventsBeforeResubmit)
	}

	afterEventsRec := doIntegrationsRequest(t, router, http.MethodGet, "/tasks/"+taskID.String()+"/events", token, nil)
	if afterEventsRec.Code != http.StatusOK {
		t.Fatalf("GET /tasks/{id}/events (после «правки»): статус = %d (%s), ожидался 200", afterEventsRec.Code, afterEventsRec.Body.String())
	}
	var afterEvents []api.TaskEvent
	if uerr := json.Unmarshal(afterEventsRec.Body.Bytes(), &afterEvents); uerr != nil {
		t.Fatalf("unmarshal GET /tasks/{id}/events (после «правки»): %v", uerr)
	}
	if len(afterEvents) != eventsBeforeResubmit {
		t.Fatalf("GET /tasks/{id}/events оригинальной задачи вернул %d событий, ожидалось без изменений (%d)", len(afterEvents), eventsBeforeResubmit)
	}

	// --- Проверка: новая задача действительно независима (свой текст, свой
	// статус queued — она никак не связана с журналом оригинальной). ---
	secondCardRec := doIntegrationsRequest(t, router, http.MethodGet, "/tasks/"+secondTaskID.String(), token, nil)
	if secondCardRec.Code != http.StatusOK {
		t.Fatalf("GET /tasks/{id} (новая задача): статус = %d (%s), ожидался 200", secondCardRec.Code, secondCardRec.Body.String())
	}
	var secondCard api.Task
	if uerr := json.Unmarshal(secondCardRec.Body.Bytes(), &secondCard); uerr != nil {
		t.Fatalf("unmarshal GET /tasks/{id} (новая задача): %v", uerr)
	}
	if secondCard.Status == nil || *secondCard.Status != api.TaskStatus("queued") {
		t.Fatalf("Task.Status новой задачи = %v, ожидался queued", secondCard.Status)
	}
	if secondCard.Text == nil || *secondCard.Text != correctedText {
		t.Fatalf("Task.Text новой задачи = %v, ожидался %q", secondCard.Text, correctedText)
	}

	t.Logf("OK: «правка» текста запроса = отдельный POST /tasks (новая задача %s), оригинальная завершённая задача %s не перезапустилась (status=completed, text и updated_at не изменились, %d событий без изменений)", secondTaskID, taskID, eventsBeforeResubmit)
}
