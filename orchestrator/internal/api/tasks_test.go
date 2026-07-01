// Unit-тесты обработчика POST /tasks без БД/Redpanda (тикет 5.3, FR E1, E4).
//
// Проверяют весь алгоритм PostTasks через httptest поверх собранного роутера
// с подменёнными слоем данных (fakeQuerier, register_test.go),
// task.Transitioner (fakeTransitioner) и CommandPublisher (fakePublisher).
// Сценарий с реальной БД и Redpanda (задача видна в очереди/истории,
// приёмка FR E1/E4) — в tasks_integration_test.go (тег integration).
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/yarabey/agentify/internal/bus"
	"github.com/yarabey/agentify/orchestrator/internal/db"
	"github.com/yarabey/agentify/orchestrator/internal/task"
)

// fakeTransitioner — подменный taskTransitioner для unit-тестов PostTasks:
// возвращает настраиваемый статус to (или ошибку) и запоминает последние
// переданные аргументы, чтобы тесты могли проверить, что PostTasks вызвал
// Transition с правильным (taskID, TriggerEnqueued).
type fakeTransitioner struct {
	to  task.Status
	err error

	lastTaskID  pgtype.UUID
	lastTrigger task.Trigger
}

func (f *fakeTransitioner) Transition(_ context.Context, taskID pgtype.UUID, trigger task.Trigger) (task.Status, task.Status, error) {
	f.lastTaskID = taskID
	f.lastTrigger = trigger
	if f.err != nil {
		return "", "", f.err
	}
	return task.StatusCreated, f.to, nil
}

// publishCall — один вызов fakePublisher.PublishKeyed, зафиксированный для
// проверки в тестах (topic/keyField/env, с которыми PostTasks опубликовал
// команду).
type publishCall struct {
	topic, keyField string
	env             bus.Envelope
}

// fakePublisher — подменный CommandPublisher для unit-тестов PostTasks.
type fakePublisher struct {
	err   error
	calls []publishCall
}

func (f *fakePublisher) PublishKeyed(_ context.Context, topic, keyField string, env bus.Envelope) error {
	f.calls = append(f.calls, publishCall{topic, keyField, env})
	return f.err
}

// taskCreatedResult — типовая успешная строка db.Task, возвращаемая
// CreateTask (fakeQuerier.createTaskResult) для happy-path сценариев.
func taskCreatedResult(id, integrationID uuid.UUID, text string) db.Task {
	return db.Task{
		ID:            pgtype.UUID{Bytes: id, Valid: true},
		IntegrationID: pgtype.UUID{Bytes: integrationID, Valid: true},
		TextEnc:       []byte(text),
		Status:        string(task.StatusCreated),
		CreatedAt:     pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true},
		UpdatedAt:     pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true},
	}
}

// postTasksServer — параметры сборки *Server для doPostTasks: позволяет
// тестам оставить transitioner/publisher нулевыми (nil), чтобы проверить
// соответствующие 500-ветки PostTasks.
type postTasksServer struct {
	q            Querier
	transitioner taskTransitioner
	publisher    CommandPublisher
}

// doPostTasks прогоняет POST /tasks через роутер, собранный из
// postTasksServer, с Bearer-токеном userID и заголовком Idempotency-Key, и
// возвращает записанный ответ.
func doPostTasks(t *testing.T, cfg postTasksServer, userID uuid.UUID, idempotencyKey string, body any) *httptest.ResponseRecorder {
	t.Helper()

	s := newTestServer(cfg.q)
	s.SetTransitioner(cfg.transitioner)
	s.SetCommandPublisher(cfg.publisher)
	router := NewRouter(s)

	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal тела: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", idempotencyKey)
	req.Header.Set("Authorization", "Bearer "+issueTestAccessToken(t, userID, time.Now()))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

// TestPostTasks_RequiresBearerToken — без Authorization-заголовка auth-middleware
// отвечает 401, не доходя до PostTasks (тикет 1.4).
func TestPostTasks_RequiresBearerToken(t *testing.T) {
	s := newTestServer(fakeQuerier{})
	s.SetTransitioner(&fakeTransitioner{to: task.StatusQueued})
	s.SetCommandPublisher(&fakePublisher{})
	router := NewRouter(s)

	body, _ := json.Marshal(TaskCreate{IntegrationId: uuid.New(), Text: "сделай x"})
	req := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "key-1")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("статус = %d (%s), ожидался 401", rec.Code, rec.Body.String())
	}
}

// TestPostTasks_MalformedJSON — невалидный JSON body → 400.
func TestPostTasks_MalformedJSON(t *testing.T) {
	s := newTestServer(fakeQuerier{})
	s.SetTransitioner(&fakeTransitioner{to: task.StatusQueued})
	s.SetCommandPublisher(&fakePublisher{})
	router := NewRouter(s)

	req := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewReader([]byte("{not json")))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "key-1")
	req.Header.Set("Authorization", "Bearer "+issueTestAccessToken(t, uuid.New(), time.Now()))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("статус = %d (%s), ожидался 400", rec.Code, rec.Body.String())
	}
}

// TestPostTasks_EmptyTextRejected — пустой (после TrimSpace) text → 400.
func TestPostTasks_EmptyTextRejected(t *testing.T) {
	userID := uuid.New()
	rec := doPostTasks(t, postTasksServer{
		q:            fakeQuerier{},
		transitioner: &fakeTransitioner{to: task.StatusQueued},
		publisher:    &fakePublisher{},
	}, userID, "key-1", TaskCreate{IntegrationId: uuid.New(), Text: "   "})

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("статус = %d (%s), ожидался 400", rec.Code, rec.Body.String())
	}
}

// TestPostTasks_IntegrationNotFound — чужая/несуществующая интеграция
// (GetIntegrationByIDAndUser → pgx.ErrNoRows) → 404 (FR A4, I3).
func TestPostTasks_IntegrationNotFound(t *testing.T) {
	userID := uuid.New()
	rec := doPostTasks(t, postTasksServer{
		q:            fakeQuerier{getIntegrationErr: pgx.ErrNoRows},
		transitioner: &fakeTransitioner{to: task.StatusQueued},
		publisher:    &fakePublisher{},
	}, userID, "key-1", TaskCreate{IntegrationId: uuid.New(), Text: "сделай x"})

	if rec.Code != http.StatusNotFound {
		t.Fatalf("статус = %d (%s), ожидался 404", rec.Code, rec.Body.String())
	}
}

// TestPostTasks_IntegrationLookupInternalError — неожиданная ошибка при
// проверке владения интеграцией → 500.
func TestPostTasks_IntegrationLookupInternalError(t *testing.T) {
	userID := uuid.New()
	rec := doPostTasks(t, postTasksServer{
		q:            fakeQuerier{getIntegrationErr: context.DeadlineExceeded},
		transitioner: &fakeTransitioner{to: task.StatusQueued},
		publisher:    &fakePublisher{},
	}, userID, "key-1", TaskCreate{IntegrationId: uuid.New(), Text: "сделай x"})

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("статус = %d (%s), ожидался 500", rec.Code, rec.Body.String())
	}
}

// TestPostTasks_IdempotencyKeyConflict — коллизия uq_tasks_idempotency
// (SQLSTATE 23505 при CreateTask) → 409 (временное поведение до дедупа
// тикета 5.5).
func TestPostTasks_IdempotencyKeyConflict(t *testing.T) {
	userID := uuid.New()
	integrationID := uuid.New()
	pgErr := &pgconn.PgError{Code: "23505", ConstraintName: "uq_tasks_idempotency"}
	rec := doPostTasks(t, postTasksServer{
		q: fakeQuerier{
			getIntegrationResult: db.Integration{ID: pgtype.UUID{Bytes: integrationID, Valid: true}},
			createTaskErr:        pgErr,
		},
		transitioner: &fakeTransitioner{to: task.StatusQueued},
		publisher:    &fakePublisher{},
	}, userID, "dup-key", TaskCreate{IntegrationId: integrationID, Text: "сделай x"})

	if rec.Code != http.StatusConflict {
		t.Fatalf("статус = %d (%s), ожидался 409", rec.Code, rec.Body.String())
	}
	assertErrorBody(t, rec)
}

// TestPostTasks_CreateTaskInternalError — прочая (не 23505) ошибка вставки →
// 500.
func TestPostTasks_CreateTaskInternalError(t *testing.T) {
	userID := uuid.New()
	integrationID := uuid.New()
	rec := doPostTasks(t, postTasksServer{
		q: fakeQuerier{
			getIntegrationResult: db.Integration{ID: pgtype.UUID{Bytes: integrationID, Valid: true}},
			createTaskErr:        context.DeadlineExceeded,
		},
		transitioner: &fakeTransitioner{to: task.StatusQueued},
		publisher:    &fakePublisher{},
	}, userID, "key-1", TaskCreate{IntegrationId: integrationID, Text: "сделай x"})

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("статус = %d (%s), ожидался 500", rec.Code, rec.Body.String())
	}
}

// TestPostTasks_NoTransitionerConfigured — transitioner не установлен (nil)
// → 500: инициализационная ошибка сервиса, не штатный случай (см. godoc поля
// Server.transitioner).
func TestPostTasks_NoTransitionerConfigured(t *testing.T) {
	userID := uuid.New()
	integrationID := uuid.New()
	taskID := uuid.New()
	rec := doPostTasks(t, postTasksServer{
		q: fakeQuerier{
			getIntegrationResult: db.Integration{ID: pgtype.UUID{Bytes: integrationID, Valid: true}},
			createTaskResult:     taskCreatedResult(taskID, integrationID, "сделай x"),
		},
		transitioner: nil,
		publisher:    &fakePublisher{},
	}, userID, "key-1", TaskCreate{IntegrationId: integrationID, Text: "сделай x"})

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("статус = %d (%s), ожидался 500", rec.Code, rec.Body.String())
	}
}

// TestPostTasks_TransitionError — Transition вернул ошибку (недопустимый
// переход/сбой БД) → 500.
func TestPostTasks_TransitionError(t *testing.T) {
	userID := uuid.New()
	integrationID := uuid.New()
	taskID := uuid.New()
	rec := doPostTasks(t, postTasksServer{
		q: fakeQuerier{
			getIntegrationResult: db.Integration{ID: pgtype.UUID{Bytes: integrationID, Valid: true}},
			createTaskResult:     taskCreatedResult(taskID, integrationID, "сделай x"),
		},
		transitioner: &fakeTransitioner{err: context.DeadlineExceeded},
		publisher:    &fakePublisher{},
	}, userID, "key-1", TaskCreate{IntegrationId: integrationID, Text: "сделай x"})

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("статус = %d (%s), ожидался 500", rec.Code, rec.Body.String())
	}
}

// TestPostTasks_NoPublisherConfigured — CommandPublisher не установлен (nil),
// хотя Transition прошёл успешно → 500 (задачу нельзя доставить без шины).
func TestPostTasks_NoPublisherConfigured(t *testing.T) {
	userID := uuid.New()
	integrationID := uuid.New()
	taskID := uuid.New()
	rec := doPostTasks(t, postTasksServer{
		q: fakeQuerier{
			getIntegrationResult: db.Integration{ID: pgtype.UUID{Bytes: integrationID, Valid: true}},
			createTaskResult:     taskCreatedResult(taskID, integrationID, "сделай x"),
		},
		transitioner: &fakeTransitioner{to: task.StatusQueued},
		publisher:    nil,
	}, userID, "key-1", TaskCreate{IntegrationId: integrationID, Text: "сделай x"})

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("статус = %d (%s), ожидался 500", rec.Code, rec.Body.String())
	}
}

// TestPostTasks_PublishError — PublishKeyed вернул ошибку → 500.
func TestPostTasks_PublishError(t *testing.T) {
	userID := uuid.New()
	integrationID := uuid.New()
	taskID := uuid.New()
	rec := doPostTasks(t, postTasksServer{
		q: fakeQuerier{
			getIntegrationResult: db.Integration{ID: pgtype.UUID{Bytes: integrationID, Valid: true}},
			createTaskResult:     taskCreatedResult(taskID, integrationID, "сделай x"),
		},
		transitioner: &fakeTransitioner{to: task.StatusQueued},
		publisher:    &fakePublisher{err: context.DeadlineExceeded},
	}, userID, "key-1", TaskCreate{IntegrationId: integrationID, Text: "сделай x"})

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("статус = %d (%s), ожидался 500", rec.Code, rec.Body.String())
	}
}

// TestPostTasks_HappyPath — успешная постановка задачи (FR E1, E4): 201,
// тело содержит status="queued" (значение, вернувшееся из Transition), и
// PublishKeyed вызван ровно один раз с правильными topic/keyField/env.
func TestPostTasks_HappyPath(t *testing.T) {
	userID := uuid.New()
	integrationID := uuid.New()
	taskID := uuid.New()
	const text = "запусти тесты"

	transitioner := &fakeTransitioner{to: task.StatusQueued}
	publisher := &fakePublisher{}
	rec := doPostTasks(t, postTasksServer{
		q: fakeQuerier{
			getIntegrationResult: db.Integration{ID: pgtype.UUID{Bytes: integrationID, Valid: true}},
			createTaskResult:     taskCreatedResult(taskID, integrationID, text),
		},
		transitioner: transitioner,
		publisher:    publisher,
	}, userID, "key-1", TaskCreate{IntegrationId: integrationID, Text: text})

	if rec.Code != http.StatusCreated {
		t.Fatalf("статус = %d (%s), ожидался 201", rec.Code, rec.Body.String())
	}

	var respTask Task
	if err := json.Unmarshal(rec.Body.Bytes(), &respTask); err != nil {
		t.Fatalf("unmarshal тела: %v", err)
	}
	if respTask.Status == nil || *respTask.Status != TaskStatus(task.StatusQueued) {
		t.Fatalf("status = %v, ожидался %q", respTask.Status, task.StatusQueued)
	}
	if respTask.Id == nil || *respTask.Id != taskID {
		t.Fatalf("id = %v, ожидался %s", respTask.Id, taskID)
	}
	if respTask.IntegrationId == nil || *respTask.IntegrationId != integrationID {
		t.Fatalf("integration_id = %v, ожидался %s", respTask.IntegrationId, integrationID)
	}
	if respTask.Text == nil || *respTask.Text != text {
		t.Fatalf("text = %v, ожидался %q", respTask.Text, text)
	}

	// Transition вызван с id новой задачи и триггером TriggerEnqueued.
	if transitioner.lastTaskID.Bytes != taskID {
		t.Fatalf("Transition вызван с taskID = %s, ожидался %s", uuid.UUID(transitioner.lastTaskID.Bytes), taskID)
	}
	if transitioner.lastTrigger != task.TriggerEnqueued {
		t.Fatalf("Transition вызван с trigger = %q, ожидался %q", transitioner.lastTrigger, task.TriggerEnqueued)
	}

	// PublishKeyed вызван ровно один раз с правильным конвертом.
	if len(publisher.calls) != 1 {
		t.Fatalf("PublishKeyed вызван %d раз(а), ожидался 1", len(publisher.calls))
	}
	call := publisher.calls[0]
	if call.topic != bus.TopicMachineCommands {
		t.Errorf("topic = %q, ожидался %q", call.topic, bus.TopicMachineCommands)
	}
	if call.keyField != bus.PartitionKeyIntegrationID {
		t.Errorf("keyField = %q, ожидался %q", call.keyField, bus.PartitionKeyIntegrationID)
	}
	if call.env.Type != bus.MessageTypeTaskAssigned {
		t.Errorf("env.Type = %q, ожидался %q", call.env.Type, bus.MessageTypeTaskAssigned)
	}
	if call.env.TaskID == nil || *call.env.TaskID != taskID.String() {
		t.Errorf("env.TaskID = %v, ожидался %s", call.env.TaskID, taskID)
	}
	if call.env.IntegrationID != integrationID.String() {
		t.Errorf("env.IntegrationID = %q, ожидался %s", call.env.IntegrationID, integrationID)
	}

	var payload bus.TaskAssignedPayload
	if err := json.Unmarshal(call.env.Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload.Text != text {
		t.Errorf("payload.Text = %q, ожидался %q", payload.Text, text)
	}
}
