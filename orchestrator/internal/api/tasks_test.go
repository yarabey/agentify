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

// fakeTransitioner — подменный taskTransitioner для unit-тестов PostTasks/
// PostTasksIdAnswer: возвращает настраиваемый статус to (или ошибку) и
// запоминает последние переданные аргументы, чтобы тесты могли проверить,
// что обработчик вызвал Transition/TransitionWithEvent с правильными
// аргументами.
//
// lastEventType/lastRefEventID/lastEventPayload — ОТДЕЛЬНЫЕ поля от
// lastTaskID/lastTrigger (которые делят Transition и TransitionWithEvent):
// тестам PostTasksIdAnswer (тикет 6.1) нужно проверить именно эти три
// аргумента TransitionWithEvent (тип события/ref_event_id/payload), не
// задевая существующие тесты PostTasks, которые проверяют только
// lastTaskID/lastTrigger через Transition.
type fakeTransitioner struct {
	to  task.Status
	err error

	lastTaskID  pgtype.UUID
	lastTrigger task.Trigger

	lastEventType    string
	lastRefEventID   pgtype.UUID
	lastEventPayload []byte
}

func (f *fakeTransitioner) Transition(_ context.Context, taskID pgtype.UUID, trigger task.Trigger) (task.Status, task.Status, error) {
	f.lastTaskID = taskID
	f.lastTrigger = trigger
	if f.err != nil {
		return "", "", f.err
	}
	return task.StatusCreated, f.to, nil
}

func (f *fakeTransitioner) TransitionWithEvent(_ context.Context, taskID pgtype.UUID, trigger task.Trigger, eventType string, refEventID pgtype.UUID, eventPayload []byte) (task.Status, task.Status, error) {
	f.lastTaskID = taskID
	f.lastTrigger = trigger
	f.lastEventType = eventType
	f.lastRefEventID = refEventID
	f.lastEventPayload = eventPayload
	if f.err != nil {
		return "", "", f.err
	}
	return task.StatusWaitingUser, f.to, nil
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
// (SQLSTATE 23505 при CreateTask, повтор постановки с тем же
// (user_id, idempotency_key)) → дедуп (тикет 5.5, FR E7): существующая
// задача перечитывается (GetTaskByUserAndIdempotencyKey) и возвращается с
// 200 (НЕ 409, НЕ 201), Transition/PublishKeyed НЕ вызываются повторно —
// задача уже прошла этот путь при первой постановке.
func TestPostTasks_IdempotencyKeyConflict(t *testing.T) {
	userID := uuid.New()
	integrationID := uuid.New()
	existingTaskID := uuid.New()
	const existingText = "исходный текст задачи"
	pgErr := &pgconn.PgError{Code: "23505", ConstraintName: "uq_tasks_idempotency"}

	transitioner := &fakeTransitioner{to: task.StatusQueued}
	publisher := &fakePublisher{}
	existing := taskCreatedResult(existingTaskID, integrationID, existingText)
	existing.Status = string(task.StatusQueued)
	rec := doPostTasks(t, postTasksServer{
		q: fakeQuerier{
			getIntegrationResult:                 db.Integration{ID: pgtype.UUID{Bytes: integrationID, Valid: true}},
			createTaskErr:                        pgErr,
			getTaskByUserAndIdempotencyKeyResult: existing,
		},
		transitioner: transitioner,
		publisher:    publisher,
	}, userID, "dup-key", TaskCreate{IntegrationId: integrationID, Text: "другой текст в повторе — не важно"})

	if rec.Code != http.StatusOK {
		t.Fatalf("статус = %d (%s), ожидался 200", rec.Code, rec.Body.String())
	}

	var respTask Task
	if err := json.Unmarshal(rec.Body.Bytes(), &respTask); err != nil {
		t.Fatalf("unmarshal тела: %v", err)
	}
	if respTask.Id == nil || *respTask.Id != existingTaskID {
		t.Fatalf("id = %v, ожидался %s (существующая задача, не новая)", respTask.Id, existingTaskID)
	}
	if respTask.Status == nil || *respTask.Status != TaskStatus(task.StatusQueued) {
		t.Fatalf("status = %v, ожидался %q", respTask.Status, task.StatusQueued)
	}

	if transitioner.lastTaskID.Valid {
		t.Fatalf("Transition не должен вызываться при повторной постановке, но вызван с taskID = %s", uuid.UUID(transitioner.lastTaskID.Bytes))
	}
	if len(publisher.calls) != 0 {
		t.Fatalf("PublishKeyed не должен вызываться при повторной постановке, но вызван %d раз(а)", len(publisher.calls))
	}
}

// TestPostTasks_IdempotencyKeyConflictRaceInternalError — коллизия
// uq_tasks_idempotency при CreateTask, но GetTaskByUserAndIdempotencyKey всё
// равно не находит строку (гипотетическая гонка) → 500, НЕ 409/404: это не
// штатный пользовательский случай.
func TestPostTasks_IdempotencyKeyConflictRaceInternalError(t *testing.T) {
	userID := uuid.New()
	integrationID := uuid.New()
	pgErr := &pgconn.PgError{Code: "23505", ConstraintName: "uq_tasks_idempotency"}
	rec := doPostTasks(t, postTasksServer{
		q: fakeQuerier{
			getIntegrationResult:              db.Integration{ID: pgtype.UUID{Bytes: integrationID, Valid: true}},
			createTaskErr:                     pgErr,
			getTaskByUserAndIdempotencyKeyErr: pgx.ErrNoRows,
		},
		transitioner: &fakeTransitioner{to: task.StatusQueued},
		publisher:    &fakePublisher{},
	}, userID, "dup-key", TaskCreate{IntegrationId: integrationID, Text: "сделай x"})

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("статус = %d (%s), ожидался 500", rec.Code, rec.Body.String())
	}
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

// agentQuestionEvent строит db.TaskEvent с type=agent_question, пригодную
// подставить в fakeQuerier.listAgentQuestionEventsByTaskResult (тикет 6.1):
// payload сериализуется как bus.AgentQuestionPayload{QuestionID, Text} —
// та же форма, что реально пишет handleAgentQuestion (machine_ws.go).
func agentQuestionEvent(t *testing.T, eventID uuid.UUID, questionID, text string) db.TaskEvent {
	t.Helper()
	payload, err := json.Marshal(bus.AgentQuestionPayload{QuestionID: questionID, Text: text})
	if err != nil {
		t.Fatalf("marshal AgentQuestionPayload: %v", err)
	}
	return db.TaskEvent{
		ID:         pgtype.UUID{Bytes: eventID, Valid: true},
		Type:       "agent_question",
		PayloadEnc: payload,
	}
}

// doPostTasksIdAnswer прогоняет POST /tasks/{id}/answer через роутер,
// собранный из postTasksServer, с Bearer-токеном userID, и возвращает
// записанный ответ.
func doPostTasksIdAnswer(t *testing.T, cfg postTasksServer, userID, taskID uuid.UUID, body any) *httptest.ResponseRecorder {
	t.Helper()

	s := newTestServer(cfg.q)
	s.SetTransitioner(cfg.transitioner)
	s.SetCommandPublisher(cfg.publisher)
	router := NewRouter(s)

	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal тела: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/tasks/"+taskID.String()+"/answer", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+issueTestAccessToken(t, userID, time.Now()))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

// TestPostTasksIdAnswer_RequiresBearerToken — без Authorization-заголовка
// auth-middleware отвечает 401, не доходя до PostTasksIdAnswer (тикет 1.4).
func TestPostTasksIdAnswer_RequiresBearerToken(t *testing.T) {
	s := newTestServer(fakeQuerier{})
	s.SetTransitioner(&fakeTransitioner{})
	s.SetCommandPublisher(&fakePublisher{})
	router := NewRouter(s)

	body, _ := json.Marshal(PostTasksIdAnswerJSONBody{QuestionId: uuid.New(), Text: "42"})
	req := httptest.NewRequest(http.MethodPost, "/tasks/"+uuid.New().String()+"/answer", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("статус = %d (%s), ожидался 401", rec.Code, rec.Body.String())
	}
}

// TestPostTasksIdAnswer_MalformedJSON — невалидный JSON body → 400.
func TestPostTasksIdAnswer_MalformedJSON(t *testing.T) {
	userID := uuid.New()
	taskID := uuid.New()
	s := newTestServer(fakeQuerier{})
	s.SetTransitioner(&fakeTransitioner{})
	s.SetCommandPublisher(&fakePublisher{})
	router := NewRouter(s)

	req := httptest.NewRequest(http.MethodPost, "/tasks/"+taskID.String()+"/answer", bytes.NewReader([]byte("{not json")))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+issueTestAccessToken(t, userID, time.Now()))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("статус = %d (%s), ожидался 400", rec.Code, rec.Body.String())
	}
}

// TestPostTasksIdAnswer_EmptyTextRejected — пустой (после TrimSpace) text →
// 400.
func TestPostTasksIdAnswer_EmptyTextRejected(t *testing.T) {
	userID := uuid.New()
	taskID := uuid.New()
	rec := doPostTasksIdAnswer(t, postTasksServer{
		q:            fakeQuerier{},
		transitioner: &fakeTransitioner{},
		publisher:    &fakePublisher{},
	}, userID, taskID, PostTasksIdAnswerJSONBody{QuestionId: uuid.New(), Text: "   "})

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("статус = %d (%s), ожидался 400", rec.Code, rec.Body.String())
	}
}

// TestPostTasksIdAnswer_TaskNotFound — чужая/несуществующая задача
// (GetTaskByIDAndUser → pgx.ErrNoRows) → 404 (FR A4, I3).
func TestPostTasksIdAnswer_TaskNotFound(t *testing.T) {
	userID := uuid.New()
	taskID := uuid.New()
	rec := doPostTasksIdAnswer(t, postTasksServer{
		q:            fakeQuerier{getTaskByIDAndUserErr: pgx.ErrNoRows},
		transitioner: &fakeTransitioner{},
		publisher:    &fakePublisher{},
	}, userID, taskID, PostTasksIdAnswerJSONBody{QuestionId: uuid.New(), Text: "42"})

	if rec.Code != http.StatusNotFound {
		t.Fatalf("статус = %d (%s), ожидался 404", rec.Code, rec.Body.String())
	}
}

// TestPostTasksIdAnswer_TaskLookupInternalError — неожиданная ошибка при
// проверке владения задачей → 500.
func TestPostTasksIdAnswer_TaskLookupInternalError(t *testing.T) {
	userID := uuid.New()
	taskID := uuid.New()
	rec := doPostTasksIdAnswer(t, postTasksServer{
		q:            fakeQuerier{getTaskByIDAndUserErr: context.DeadlineExceeded},
		transitioner: &fakeTransitioner{},
		publisher:    &fakePublisher{},
	}, userID, taskID, PostTasksIdAnswerJSONBody{QuestionId: uuid.New(), Text: "42"})

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("статус = %d (%s), ожидался 500", rec.Code, rec.Body.String())
	}
}

// TestPostTasksIdAnswer_QuestionNotFound — question_id из тела не совпадает
// ни с одним agent_question этой задачи → 404 (сопоставление по question_id,
// не «последний вопрос», см. godoc PostTasksIdAnswer).
func TestPostTasksIdAnswer_QuestionNotFound(t *testing.T) {
	userID := uuid.New()
	taskID := uuid.New()
	integrationID := uuid.New()
	rec := doPostTasksIdAnswer(t, postTasksServer{
		q: fakeQuerier{
			getTaskByIDAndUserResult: db.Task{
				ID:            pgtype.UUID{Bytes: taskID, Valid: true},
				IntegrationID: pgtype.UUID{Bytes: integrationID, Valid: true},
			},
			listAgentQuestionEventsByTaskResult: []db.TaskEvent{
				agentQuestionEvent(t, uuid.New(), uuid.New().String(), "другой вопрос"),
			},
		},
		transitioner: &fakeTransitioner{},
		publisher:    &fakePublisher{},
	}, userID, taskID, PostTasksIdAnswerJSONBody{QuestionId: uuid.New(), Text: "42"})

	if rec.Code != http.StatusNotFound {
		t.Fatalf("статус = %d (%s), ожидался 404", rec.Code, rec.Body.String())
	}
}

// TestPostTasksIdAnswer_ListQuestionsInternalError — ошибка
// ListAgentQuestionEventsByTask → 500.
func TestPostTasksIdAnswer_ListQuestionsInternalError(t *testing.T) {
	userID := uuid.New()
	taskID := uuid.New()
	rec := doPostTasksIdAnswer(t, postTasksServer{
		q: fakeQuerier{
			getTaskByIDAndUserResult:         db.Task{ID: pgtype.UUID{Bytes: taskID, Valid: true}},
			listAgentQuestionEventsByTaskErr: context.DeadlineExceeded,
		},
		transitioner: &fakeTransitioner{},
		publisher:    &fakePublisher{},
	}, userID, taskID, PostTasksIdAnswerJSONBody{QuestionId: uuid.New(), Text: "42"})

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("статус = %d (%s), ожидался 500", rec.Code, rec.Body.String())
	}
}

// TestPostTasksIdAnswer_NoTransitionerConfigured — transitioner не установлен
// (nil) → 500.
func TestPostTasksIdAnswer_NoTransitionerConfigured(t *testing.T) {
	userID := uuid.New()
	taskID := uuid.New()
	questionID := uuid.New()
	rec := doPostTasksIdAnswer(t, postTasksServer{
		q: fakeQuerier{
			getTaskByIDAndUserResult: db.Task{ID: pgtype.UUID{Bytes: taskID, Valid: true}},
			listAgentQuestionEventsByTaskResult: []db.TaskEvent{
				agentQuestionEvent(t, uuid.New(), questionID.String(), "вопрос"),
			},
		},
		transitioner: nil,
		publisher:    &fakePublisher{},
	}, userID, taskID, PostTasksIdAnswerJSONBody{QuestionId: questionID, Text: "42"})

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("статус = %d (%s), ожидался 500", rec.Code, rec.Body.String())
	}
}

// TestPostTasksIdAnswer_TransitionError — TransitionWithEvent вернул ошибку
// (недопустимый переход/сбой БД) → 500.
func TestPostTasksIdAnswer_TransitionError(t *testing.T) {
	userID := uuid.New()
	taskID := uuid.New()
	questionID := uuid.New()
	rec := doPostTasksIdAnswer(t, postTasksServer{
		q: fakeQuerier{
			getTaskByIDAndUserResult: db.Task{ID: pgtype.UUID{Bytes: taskID, Valid: true}},
			listAgentQuestionEventsByTaskResult: []db.TaskEvent{
				agentQuestionEvent(t, uuid.New(), questionID.String(), "вопрос"),
			},
		},
		transitioner: &fakeTransitioner{err: context.DeadlineExceeded},
		publisher:    &fakePublisher{},
	}, userID, taskID, PostTasksIdAnswerJSONBody{QuestionId: questionID, Text: "42"})

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("статус = %d (%s), ожидался 500", rec.Code, rec.Body.String())
	}
}

// TestPostTasksIdAnswer_NoPublisherConfigured — CommandPublisher не
// установлен (nil), хотя TransitionWithEvent прошёл успешно → 500.
func TestPostTasksIdAnswer_NoPublisherConfigured(t *testing.T) {
	userID := uuid.New()
	taskID := uuid.New()
	questionID := uuid.New()
	rec := doPostTasksIdAnswer(t, postTasksServer{
		q: fakeQuerier{
			getTaskByIDAndUserResult: db.Task{ID: pgtype.UUID{Bytes: taskID, Valid: true}},
			listAgentQuestionEventsByTaskResult: []db.TaskEvent{
				agentQuestionEvent(t, uuid.New(), questionID.String(), "вопрос"),
			},
		},
		transitioner: &fakeTransitioner{to: task.StatusRunning},
		publisher:    nil,
	}, userID, taskID, PostTasksIdAnswerJSONBody{QuestionId: questionID, Text: "42"})

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("статус = %d (%s), ожидался 500", rec.Code, rec.Body.String())
	}
}

// TestPostTasksIdAnswer_PublishError — PublishKeyed вернул ошибку → 500.
func TestPostTasksIdAnswer_PublishError(t *testing.T) {
	userID := uuid.New()
	taskID := uuid.New()
	questionID := uuid.New()
	rec := doPostTasksIdAnswer(t, postTasksServer{
		q: fakeQuerier{
			getTaskByIDAndUserResult: db.Task{ID: pgtype.UUID{Bytes: taskID, Valid: true}},
			listAgentQuestionEventsByTaskResult: []db.TaskEvent{
				agentQuestionEvent(t, uuid.New(), questionID.String(), "вопрос"),
			},
		},
		transitioner: &fakeTransitioner{to: task.StatusRunning},
		publisher:    &fakePublisher{err: context.DeadlineExceeded},
	}, userID, taskID, PostTasksIdAnswerJSONBody{QuestionId: questionID, Text: "42"})

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("статус = %d (%s), ожидался 500", rec.Code, rec.Body.String())
	}
}

// TestPostTasksIdAnswer_HappyPath — успешный ответ пользователя (FR F1, F2):
// 202, TransitionWithEvent вызван с (taskID, TriggerUserAnswered,
// "user_answer", ref_event_id == id найденной записи agent_question,
// payload с тем же question_id/text), PublishKeyed вызван ровно один раз с
// конвертом user_answer.
func TestPostTasksIdAnswer_HappyPath(t *testing.T) {
	userID := uuid.New()
	taskID := uuid.New()
	integrationID := uuid.New()
	questionID := uuid.New()
	questionEventID := uuid.New()
	const answerText = "используй Go 1.24"

	transitioner := &fakeTransitioner{to: task.StatusRunning}
	publisher := &fakePublisher{}
	rec := doPostTasksIdAnswer(t, postTasksServer{
		q: fakeQuerier{
			getTaskByIDAndUserResult: db.Task{
				ID:            pgtype.UUID{Bytes: taskID, Valid: true},
				IntegrationID: pgtype.UUID{Bytes: integrationID, Valid: true},
			},
			listAgentQuestionEventsByTaskResult: []db.TaskEvent{
				agentQuestionEvent(t, questionEventID, questionID.String(), "какую версию Go использовать?"),
			},
		},
		transitioner: transitioner,
		publisher:    publisher,
	}, userID, taskID, PostTasksIdAnswerJSONBody{QuestionId: questionID, Text: answerText})

	if rec.Code != http.StatusAccepted {
		t.Fatalf("статус = %d (%s), ожидался 202", rec.Code, rec.Body.String())
	}

	if transitioner.lastTaskID.Bytes != taskID {
		t.Fatalf("TransitionWithEvent вызван с taskID = %s, ожидался %s", uuid.UUID(transitioner.lastTaskID.Bytes), taskID)
	}
	if transitioner.lastTrigger != task.TriggerUserAnswered {
		t.Fatalf("TransitionWithEvent вызван с trigger = %q, ожидался %q", transitioner.lastTrigger, task.TriggerUserAnswered)
	}
	if transitioner.lastEventType != "user_answer" {
		t.Fatalf("TransitionWithEvent вызван с eventType = %q, ожидался %q", transitioner.lastEventType, "user_answer")
	}
	if transitioner.lastRefEventID.Bytes != questionEventID {
		t.Fatalf("TransitionWithEvent вызван с refEventID = %s, ожидался %s (id найденной agent_question)", uuid.UUID(transitioner.lastRefEventID.Bytes), questionEventID)
	}
	var eventPayload bus.UserAnswerPayload
	if err := json.Unmarshal(transitioner.lastEventPayload, &eventPayload); err != nil {
		t.Fatalf("unmarshal lastEventPayload: %v", err)
	}
	if eventPayload.QuestionID != questionID.String() {
		t.Errorf("lastEventPayload.QuestionID = %q, ожидался %q", eventPayload.QuestionID, questionID.String())
	}
	if eventPayload.Text != answerText {
		t.Errorf("lastEventPayload.Text = %q, ожидался %q", eventPayload.Text, answerText)
	}

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
	if call.env.Type != bus.MessageTypeUserAnswer {
		t.Errorf("env.Type = %q, ожидался %q", call.env.Type, bus.MessageTypeUserAnswer)
	}
	if call.env.TaskID == nil || *call.env.TaskID != taskID.String() {
		t.Errorf("env.TaskID = %v, ожидался %s", call.env.TaskID, taskID)
	}
	if call.env.IntegrationID != integrationID.String() {
		t.Errorf("env.IntegrationID = %q, ожидался %s", call.env.IntegrationID, integrationID)
	}

	var payload bus.UserAnswerPayload
	if err := json.Unmarshal(call.env.Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload.QuestionID != questionID.String() {
		t.Errorf("payload.QuestionID = %q, ожидался %q", payload.QuestionID, questionID.String())
	}
	if payload.Text != answerText {
		t.Errorf("payload.Text = %q, ожидался %q", payload.Text, answerText)
	}
}

// TestPostTasksIdAnswer_MatchesSpecificQuestionAmongMultiple — сопоставление
// СТРОГО по question_id, а не «последний вопрос задачи» (это требование уже
// этого тикета 6.1, а не только 6.2 «несколько вопросов сопоставляются
// корректно», см. godoc PostTasksIdAnswer): среди нескольких agent_question
// этой задачи выбирается именно та запись, чей question_id совпал с телом
// запроса, даже если она не самая новая (первая в порядке ListAgentQuestionEventsByTask,
// который возвращает записи по убыванию seq).
func TestPostTasksIdAnswer_MatchesSpecificQuestionAmongMultiple(t *testing.T) {
	userID := uuid.New()
	taskID := uuid.New()
	integrationID := uuid.New()

	// Три вопроса; отвечаем на СРЕДНИЙ (по времени) — не первый в списке
	// (самый новый) и не последний (самый старый).
	newestEventID := uuid.New()
	targetEventID := uuid.New()
	oldestEventID := uuid.New()
	targetQuestionID := uuid.New()

	transitioner := &fakeTransitioner{to: task.StatusRunning}
	publisher := &fakePublisher{}
	rec := doPostTasksIdAnswer(t, postTasksServer{
		q: fakeQuerier{
			getTaskByIDAndUserResult: db.Task{
				ID:            pgtype.UUID{Bytes: taskID, Valid: true},
				IntegrationID: pgtype.UUID{Bytes: integrationID, Valid: true},
			},
			// ListAgentQuestionEventsByTask возвращает самые новые первыми
			// (ORDER BY seq DESC, см. queries/tasks.sql) — targetEventID
			// намеренно НЕ первый в срезе.
			listAgentQuestionEventsByTaskResult: []db.TaskEvent{
				agentQuestionEvent(t, newestEventID, uuid.New().String(), "самый новый вопрос"),
				agentQuestionEvent(t, targetEventID, targetQuestionID.String(), "нужный вопрос"),
				agentQuestionEvent(t, oldestEventID, uuid.New().String(), "самый старый вопрос"),
			},
		},
		transitioner: transitioner,
		publisher:    publisher,
	}, userID, taskID, PostTasksIdAnswerJSONBody{QuestionId: targetQuestionID, Text: "ответ на нужный вопрос"})

	if rec.Code != http.StatusAccepted {
		t.Fatalf("статус = %d (%s), ожидался 202", rec.Code, rec.Body.String())
	}
	if transitioner.lastRefEventID.Bytes != targetEventID {
		t.Fatalf("ref_event_id = %s, ожидался %s (именно нужный вопрос, не самый новый/старый)",
			uuid.UUID(transitioner.lastRefEventID.Bytes), targetEventID)
	}
}

// commandApprovalRequestEvent строит db.TaskEvent с type=command_approval_request,
// пригодную подставить в fakeQuerier.listCommandApprovalRequestEventsByTaskResult
// (тикет 6.4): payload сериализуется как
// bus.CommandApprovalRequestPayload{RequestID, Command, Reason} — та же
// форма, что реально пишет handleCommandApprovalRequest (machine_ws.go).
func commandApprovalRequestEvent(t *testing.T, eventID uuid.UUID, requestID, command, reason string) db.TaskEvent {
	t.Helper()
	payload, err := json.Marshal(bus.CommandApprovalRequestPayload{RequestID: requestID, Command: command, Reason: reason})
	if err != nil {
		t.Fatalf("marshal CommandApprovalRequestPayload: %v", err)
	}
	return db.TaskEvent{
		ID:         pgtype.UUID{Bytes: eventID, Valid: true},
		Type:       "command_approval_request",
		PayloadEnc: payload,
	}
}

// doPostTasksIdApprove прогоняет POST /tasks/{id}/approve через роутер,
// собранный из postTasksServer, с Bearer-токеном userID, и возвращает
// записанный ответ.
func doPostTasksIdApprove(t *testing.T, cfg postTasksServer, userID, taskID uuid.UUID, body any) *httptest.ResponseRecorder {
	t.Helper()

	s := newTestServer(cfg.q)
	s.SetTransitioner(cfg.transitioner)
	s.SetCommandPublisher(cfg.publisher)
	router := NewRouter(s)

	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal тела: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/tasks/"+taskID.String()+"/approve", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+issueTestAccessToken(t, userID, time.Now()))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

// TestPostTasksIdApprove_RequiresBearerToken — без Authorization-заголовка
// auth-middleware отвечает 401, не доходя до PostTasksIdApprove (тикет 1.4).
func TestPostTasksIdApprove_RequiresBearerToken(t *testing.T) {
	s := newTestServer(fakeQuerier{})
	s.SetTransitioner(&fakeTransitioner{})
	s.SetCommandPublisher(&fakePublisher{})
	router := NewRouter(s)

	body, _ := json.Marshal(PostTasksIdApproveJSONBody{RequestId: uuid.New(), Decision: Approve})
	req := httptest.NewRequest(http.MethodPost, "/tasks/"+uuid.New().String()+"/approve", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("статус = %d (%s), ожидался 401", rec.Code, rec.Body.String())
	}
}

// TestPostTasksIdApprove_MalformedJSON — невалидный JSON body → 400.
func TestPostTasksIdApprove_MalformedJSON(t *testing.T) {
	userID := uuid.New()
	taskID := uuid.New()
	s := newTestServer(fakeQuerier{})
	s.SetTransitioner(&fakeTransitioner{})
	s.SetCommandPublisher(&fakePublisher{})
	router := NewRouter(s)

	req := httptest.NewRequest(http.MethodPost, "/tasks/"+taskID.String()+"/approve", bytes.NewReader([]byte("{not json")))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+issueTestAccessToken(t, userID, time.Now()))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("статус = %d (%s), ожидался 400", rec.Code, rec.Body.String())
	}
}

// TestPostTasksIdApprove_InvalidDecisionRejected — decision вне словаря
// {approve, reject} → 400 (тип PostTasksIdApproveJSONBodyDecision в
// контракте — просто string, JSON-декодирование само по себе не проверяет
// значение).
func TestPostTasksIdApprove_InvalidDecisionRejected(t *testing.T) {
	userID := uuid.New()
	taskID := uuid.New()
	s := newTestServer(fakeQuerier{})
	s.SetTransitioner(&fakeTransitioner{})
	s.SetCommandPublisher(&fakePublisher{})
	router := NewRouter(s)

	body, _ := json.Marshal(map[string]any{"request_id": uuid.New().String(), "decision": "maybe"})
	req := httptest.NewRequest(http.MethodPost, "/tasks/"+taskID.String()+"/approve", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+issueTestAccessToken(t, userID, time.Now()))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("статус = %d (%s), ожидался 400", rec.Code, rec.Body.String())
	}
}

// TestPostTasksIdApprove_TaskNotFound — чужая/несуществующая задача
// (GetTaskByIDAndUser → pgx.ErrNoRows) → 404 (FR A4, I3).
func TestPostTasksIdApprove_TaskNotFound(t *testing.T) {
	userID := uuid.New()
	taskID := uuid.New()
	rec := doPostTasksIdApprove(t, postTasksServer{
		q:            fakeQuerier{getTaskByIDAndUserErr: pgx.ErrNoRows},
		transitioner: &fakeTransitioner{},
		publisher:    &fakePublisher{},
	}, userID, taskID, PostTasksIdApproveJSONBody{RequestId: uuid.New(), Decision: Approve})

	if rec.Code != http.StatusNotFound {
		t.Fatalf("статус = %d (%s), ожидался 404", rec.Code, rec.Body.String())
	}
}

// TestPostTasksIdApprove_TaskLookupInternalError — неожиданная ошибка при
// проверке владения задачей → 500.
func TestPostTasksIdApprove_TaskLookupInternalError(t *testing.T) {
	userID := uuid.New()
	taskID := uuid.New()
	rec := doPostTasksIdApprove(t, postTasksServer{
		q:            fakeQuerier{getTaskByIDAndUserErr: context.DeadlineExceeded},
		transitioner: &fakeTransitioner{},
		publisher:    &fakePublisher{},
	}, userID, taskID, PostTasksIdApproveJSONBody{RequestId: uuid.New(), Decision: Approve})

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("статус = %d (%s), ожидался 500", rec.Code, rec.Body.String())
	}
}

// TestPostTasksIdApprove_RequestNotFound — request_id из тела не совпадает ни
// с одним command_approval_request этой задачи → 404 (сопоставление по
// request_id, не «последний запрос», см. godoc PostTasksIdApprove).
func TestPostTasksIdApprove_RequestNotFound(t *testing.T) {
	userID := uuid.New()
	taskID := uuid.New()
	integrationID := uuid.New()
	rec := doPostTasksIdApprove(t, postTasksServer{
		q: fakeQuerier{
			getTaskByIDAndUserResult: db.Task{
				ID:            pgtype.UUID{Bytes: taskID, Valid: true},
				IntegrationID: pgtype.UUID{Bytes: integrationID, Valid: true},
			},
			listCommandApprovalRequestEventsByTaskResult: []db.TaskEvent{
				commandApprovalRequestEvent(t, uuid.New(), uuid.New().String(), "rm -rf /", "другой запрос"),
			},
		},
		transitioner: &fakeTransitioner{},
		publisher:    &fakePublisher{},
	}, userID, taskID, PostTasksIdApproveJSONBody{RequestId: uuid.New(), Decision: Approve})

	if rec.Code != http.StatusNotFound {
		t.Fatalf("статус = %d (%s), ожидался 404", rec.Code, rec.Body.String())
	}
}

// TestPostTasksIdApprove_ListRequestsInternalError — ошибка
// ListCommandApprovalRequestEventsByTask → 500.
func TestPostTasksIdApprove_ListRequestsInternalError(t *testing.T) {
	userID := uuid.New()
	taskID := uuid.New()
	rec := doPostTasksIdApprove(t, postTasksServer{
		q: fakeQuerier{
			getTaskByIDAndUserResult:                  db.Task{ID: pgtype.UUID{Bytes: taskID, Valid: true}},
			listCommandApprovalRequestEventsByTaskErr: context.DeadlineExceeded,
		},
		transitioner: &fakeTransitioner{},
		publisher:    &fakePublisher{},
	}, userID, taskID, PostTasksIdApproveJSONBody{RequestId: uuid.New(), Decision: Approve})

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("статус = %d (%s), ожидался 500", rec.Code, rec.Body.String())
	}
}

// TestPostTasksIdApprove_NoTransitionerConfigured — transitioner не установлен
// (nil) → 500.
func TestPostTasksIdApprove_NoTransitionerConfigured(t *testing.T) {
	userID := uuid.New()
	taskID := uuid.New()
	requestID := uuid.New()
	rec := doPostTasksIdApprove(t, postTasksServer{
		q: fakeQuerier{
			getTaskByIDAndUserResult: db.Task{ID: pgtype.UUID{Bytes: taskID, Valid: true}},
			listCommandApprovalRequestEventsByTaskResult: []db.TaskEvent{
				commandApprovalRequestEvent(t, uuid.New(), requestID.String(), "rm -rf /tmp", "нужно очистить"),
			},
		},
		transitioner: nil,
		publisher:    &fakePublisher{},
	}, userID, taskID, PostTasksIdApproveJSONBody{RequestId: requestID, Decision: Approve})

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("статус = %d (%s), ожидался 500", rec.Code, rec.Body.String())
	}
}

// TestPostTasksIdApprove_TransitionError — TransitionWithEvent вернул ошибку
// (недопустимый переход/сбой БД) → 500.
func TestPostTasksIdApprove_TransitionError(t *testing.T) {
	userID := uuid.New()
	taskID := uuid.New()
	requestID := uuid.New()
	rec := doPostTasksIdApprove(t, postTasksServer{
		q: fakeQuerier{
			getTaskByIDAndUserResult: db.Task{ID: pgtype.UUID{Bytes: taskID, Valid: true}},
			listCommandApprovalRequestEventsByTaskResult: []db.TaskEvent{
				commandApprovalRequestEvent(t, uuid.New(), requestID.String(), "rm -rf /tmp", "нужно очистить"),
			},
		},
		transitioner: &fakeTransitioner{err: context.DeadlineExceeded},
		publisher:    &fakePublisher{},
	}, userID, taskID, PostTasksIdApproveJSONBody{RequestId: requestID, Decision: Approve})

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("статус = %d (%s), ожидался 500", rec.Code, rec.Body.String())
	}
}

// TestPostTasksIdApprove_NoPublisherConfigured — CommandPublisher не
// установлен (nil), хотя TransitionWithEvent прошёл успешно → 500.
func TestPostTasksIdApprove_NoPublisherConfigured(t *testing.T) {
	userID := uuid.New()
	taskID := uuid.New()
	requestID := uuid.New()
	rec := doPostTasksIdApprove(t, postTasksServer{
		q: fakeQuerier{
			getTaskByIDAndUserResult: db.Task{ID: pgtype.UUID{Bytes: taskID, Valid: true}},
			listCommandApprovalRequestEventsByTaskResult: []db.TaskEvent{
				commandApprovalRequestEvent(t, uuid.New(), requestID.String(), "rm -rf /tmp", "нужно очистить"),
			},
		},
		transitioner: &fakeTransitioner{to: task.StatusRunning},
		publisher:    nil,
	}, userID, taskID, PostTasksIdApproveJSONBody{RequestId: requestID, Decision: Approve})

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("статус = %d (%s), ожидался 500", rec.Code, rec.Body.String())
	}
}

// TestPostTasksIdApprove_PublishError — PublishKeyed вернул ошибку → 500.
func TestPostTasksIdApprove_PublishError(t *testing.T) {
	userID := uuid.New()
	taskID := uuid.New()
	requestID := uuid.New()
	rec := doPostTasksIdApprove(t, postTasksServer{
		q: fakeQuerier{
			getTaskByIDAndUserResult: db.Task{ID: pgtype.UUID{Bytes: taskID, Valid: true}},
			listCommandApprovalRequestEventsByTaskResult: []db.TaskEvent{
				commandApprovalRequestEvent(t, uuid.New(), requestID.String(), "rm -rf /tmp", "нужно очистить"),
			},
		},
		transitioner: &fakeTransitioner{to: task.StatusRunning},
		publisher:    &fakePublisher{err: context.DeadlineExceeded},
	}, userID, taskID, PostTasksIdApproveJSONBody{RequestId: requestID, Decision: Approve})

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("статус = %d (%s), ожидался 500", rec.Code, rec.Body.String())
	}
}

// TestPostTasksIdApprove_HappyPath_Approve — успешное согласование (FR F3,
// decision=approve): 202, TransitionWithEvent вызван с (taskID,
// TriggerCommandDecision, "user_decision", ref_event_id == id найденной
// записи command_approval_request, payload с тем же request_id/decision),
// PublishKeyed вызван ровно один раз с конвертом command_decision.
func TestPostTasksIdApprove_HappyPath_Approve(t *testing.T) {
	userID := uuid.New()
	taskID := uuid.New()
	integrationID := uuid.New()
	requestID := uuid.New()
	requestEventID := uuid.New()

	transitioner := &fakeTransitioner{to: task.StatusRunning}
	publisher := &fakePublisher{}
	rec := doPostTasksIdApprove(t, postTasksServer{
		q: fakeQuerier{
			getTaskByIDAndUserResult: db.Task{
				ID:            pgtype.UUID{Bytes: taskID, Valid: true},
				IntegrationID: pgtype.UUID{Bytes: integrationID, Valid: true},
			},
			listCommandApprovalRequestEventsByTaskResult: []db.TaskEvent{
				commandApprovalRequestEvent(t, requestEventID, requestID.String(), "rm -rf /tmp/build", "нужно очистить директорию сборки"),
			},
		},
		transitioner: transitioner,
		publisher:    publisher,
	}, userID, taskID, PostTasksIdApproveJSONBody{RequestId: requestID, Decision: Approve})

	if rec.Code != http.StatusAccepted {
		t.Fatalf("статус = %d (%s), ожидался 202", rec.Code, rec.Body.String())
	}

	if transitioner.lastTaskID.Bytes != taskID {
		t.Fatalf("TransitionWithEvent вызван с taskID = %s, ожидался %s", uuid.UUID(transitioner.lastTaskID.Bytes), taskID)
	}
	if transitioner.lastTrigger != task.TriggerCommandDecision {
		t.Fatalf("TransitionWithEvent вызван с trigger = %q, ожидался %q", transitioner.lastTrigger, task.TriggerCommandDecision)
	}
	if transitioner.lastEventType != "user_decision" {
		t.Fatalf("TransitionWithEvent вызван с eventType = %q, ожидался %q", transitioner.lastEventType, "user_decision")
	}
	if transitioner.lastRefEventID.Bytes != requestEventID {
		t.Fatalf("TransitionWithEvent вызван с refEventID = %s, ожидался %s (id найденного command_approval_request)", uuid.UUID(transitioner.lastRefEventID.Bytes), requestEventID)
	}
	var eventPayload bus.CommandDecisionPayload
	if err := json.Unmarshal(transitioner.lastEventPayload, &eventPayload); err != nil {
		t.Fatalf("unmarshal lastEventPayload: %v", err)
	}
	if eventPayload.RequestID != requestID.String() {
		t.Errorf("lastEventPayload.RequestID = %q, ожидался %q", eventPayload.RequestID, requestID.String())
	}
	if eventPayload.Decision != "approve" {
		t.Errorf("lastEventPayload.Decision = %q, ожидался %q", eventPayload.Decision, "approve")
	}

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
	if call.env.Type != bus.MessageTypeCommandDecision {
		t.Errorf("env.Type = %q, ожидался %q", call.env.Type, bus.MessageTypeCommandDecision)
	}
	if call.env.TaskID == nil || *call.env.TaskID != taskID.String() {
		t.Errorf("env.TaskID = %v, ожидался %s", call.env.TaskID, taskID)
	}
	if call.env.IntegrationID != integrationID.String() {
		t.Errorf("env.IntegrationID = %q, ожидался %s", call.env.IntegrationID, integrationID)
	}

	var payload bus.CommandDecisionPayload
	if err := json.Unmarshal(call.env.Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload.RequestID != requestID.String() {
		t.Errorf("payload.RequestID = %q, ожидался %q", payload.RequestID, requestID.String())
	}
	if payload.Decision != "approve" {
		t.Errorf("payload.Decision = %q, ожидался %q", payload.Decision, "approve")
	}
}

// TestPostTasksIdApprove_HappyPath_Reject — то же самое, но decision=reject:
// контракт 6.4 (в отличие от поведенческой приёмки 6.5) требует лишь, чтобы
// оба значения decision корректно доходили до event/publish payload —
// ПОВЕДЕНЧЕСКАЯ проверка «агент не выполнил отклонённую команду» — предмет
// отдельного тикета 6.5 (deps: 6.4) и здесь не проверяется.
func TestPostTasksIdApprove_HappyPath_Reject(t *testing.T) {
	userID := uuid.New()
	taskID := uuid.New()
	integrationID := uuid.New()
	requestID := uuid.New()
	requestEventID := uuid.New()

	transitioner := &fakeTransitioner{to: task.StatusRunning}
	publisher := &fakePublisher{}
	rec := doPostTasksIdApprove(t, postTasksServer{
		q: fakeQuerier{
			getTaskByIDAndUserResult: db.Task{
				ID:            pgtype.UUID{Bytes: taskID, Valid: true},
				IntegrationID: pgtype.UUID{Bytes: integrationID, Valid: true},
			},
			listCommandApprovalRequestEventsByTaskResult: []db.TaskEvent{
				commandApprovalRequestEvent(t, requestEventID, requestID.String(), "rm -rf /tmp/build", "нужно очистить директорию сборки"),
			},
		},
		transitioner: transitioner,
		publisher:    publisher,
	}, userID, taskID, PostTasksIdApproveJSONBody{RequestId: requestID, Decision: Reject})

	if rec.Code != http.StatusAccepted {
		t.Fatalf("статус = %d (%s), ожидался 202", rec.Code, rec.Body.String())
	}

	var eventPayload bus.CommandDecisionPayload
	if err := json.Unmarshal(transitioner.lastEventPayload, &eventPayload); err != nil {
		t.Fatalf("unmarshal lastEventPayload: %v", err)
	}
	if eventPayload.Decision != "reject" {
		t.Errorf("lastEventPayload.Decision = %q, ожидался %q", eventPayload.Decision, "reject")
	}

	if len(publisher.calls) != 1 {
		t.Fatalf("PublishKeyed вызван %d раз(а), ожидался 1", len(publisher.calls))
	}
	var payload bus.CommandDecisionPayload
	if err := json.Unmarshal(publisher.calls[0].env.Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload.Decision != "reject" {
		t.Errorf("payload.Decision = %q, ожидался %q", payload.Decision, "reject")
	}
}

// TestPostTasksIdApprove_CommandNotPublishedBeforeApprove — ключевая приёмка
// тикета 6.4 (Gherkin §5 «Но команда не выполняется, пока я её не одобрю»):
// до успешного вызова PostTasksIdApprove ни один command_decision не
// публикуется (publisher.calls пуст), и на каждом из путей отказа
// (задача/запрос не найдены, transitioner/publisher не настроены,
// TransitionWithEvent вернул ошибку) публикации по-прежнему не происходит —
// т.е. согласование (или отказ) не «утекает» на сторону агента раньше
// официального успешного approve.
func TestPostTasksIdApprove_CommandNotPublishedBeforeApprove(t *testing.T) {
	userID := uuid.New()
	taskID := uuid.New()
	requestID := uuid.New()

	cases := map[string]postTasksServer{
		"задача не найдена": {
			q:            fakeQuerier{getTaskByIDAndUserErr: pgx.ErrNoRows},
			transitioner: &fakeTransitioner{},
			publisher:    &fakePublisher{},
		},
		"запрос на согласование не найден": {
			q: fakeQuerier{
				getTaskByIDAndUserResult: db.Task{ID: pgtype.UUID{Bytes: taskID, Valid: true}},
			},
			transitioner: &fakeTransitioner{},
			publisher:    &fakePublisher{},
		},
		"transitioner не настроен": {
			q: fakeQuerier{
				getTaskByIDAndUserResult: db.Task{ID: pgtype.UUID{Bytes: taskID, Valid: true}},
				listCommandApprovalRequestEventsByTaskResult: []db.TaskEvent{
					commandApprovalRequestEvent(t, uuid.New(), requestID.String(), "rm -rf /tmp", "тест"),
				},
			},
			transitioner: nil,
			publisher:    &fakePublisher{},
		},
		"TransitionWithEvent вернул ошибку": {
			q: fakeQuerier{
				getTaskByIDAndUserResult: db.Task{ID: pgtype.UUID{Bytes: taskID, Valid: true}},
				listCommandApprovalRequestEventsByTaskResult: []db.TaskEvent{
					commandApprovalRequestEvent(t, uuid.New(), requestID.String(), "rm -rf /tmp", "тест"),
				},
			},
			transitioner: &fakeTransitioner{err: context.DeadlineExceeded},
			publisher:    &fakePublisher{},
		},
	}

	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			publisher := cfg.publisher.(*fakePublisher)
			if len(publisher.calls) != 0 {
				t.Fatalf("до вызова PostTasksIdApprove publisher.calls уже не пуст: %d", len(publisher.calls))
			}

			rec := doPostTasksIdApprove(t, cfg, userID, taskID, PostTasksIdApproveJSONBody{RequestId: requestID, Decision: Approve})

			if rec.Code == http.StatusAccepted {
				t.Fatalf("этот сценарий (%s) должен провалиться, а не вернуть 202", name)
			}
			if len(publisher.calls) != 0 {
				t.Fatalf("%s: команда опубликована (publisher.calls = %d), хотя approve не прошёл успешно", name, len(publisher.calls))
			}
		})
	}

	// И зеркально: happy path публикует РОВНО один раз — до успешного approve
	// вызовов не было (см. TestPostTasksIdApprove_HappyPath_Approve выше),
	// после — ровно один, не более.
	requestEventID := uuid.New()
	transitioner := &fakeTransitioner{to: task.StatusRunning}
	publisher := &fakePublisher{}
	if len(publisher.calls) != 0 {
		t.Fatal("publisher.calls не пуст до вызова approve")
	}
	rec := doPostTasksIdApprove(t, postTasksServer{
		q: fakeQuerier{
			getTaskByIDAndUserResult: db.Task{ID: pgtype.UUID{Bytes: taskID, Valid: true}},
			listCommandApprovalRequestEventsByTaskResult: []db.TaskEvent{
				commandApprovalRequestEvent(t, requestEventID, requestID.String(), "rm -rf /tmp", "тест"),
			},
		},
		transitioner: transitioner,
		publisher:    publisher,
	}, userID, taskID, PostTasksIdApproveJSONBody{RequestId: requestID, Decision: Approve})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("статус = %d (%s), ожидался 202", rec.Code, rec.Body.String())
	}
	if len(publisher.calls) != 1 {
		t.Fatalf("после успешного approve команда должна быть опубликована ровно 1 раз, а не %d", len(publisher.calls))
	}
}

// doPostTasksIdConfirm прогоняет POST /tasks/{id}/confirm через роутер,
// собранный из postTasksServer, с Bearer-токеном userID, и возвращает
// записанный ответ. В отличие от doPostTasksIdApprove/doPostTasksIdAnswer —
// без тела запроса: контракт (api/openapi.yaml) не описывает requestBody для
// confirm (тикет 8.2).
func doPostTasksIdConfirm(t *testing.T, cfg postTasksServer, userID, taskID uuid.UUID) *httptest.ResponseRecorder {
	t.Helper()

	s := newTestServer(cfg.q)
	s.SetTransitioner(cfg.transitioner)
	s.SetCommandPublisher(cfg.publisher)
	router := NewRouter(s)

	req := httptest.NewRequest(http.MethodPost, "/tasks/"+taskID.String()+"/confirm", nil)
	req.Header.Set("Authorization", "Bearer "+issueTestAccessToken(t, userID, time.Now()))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

// TestPostTasksIdConfirm_RequiresBearerToken — без Authorization-заголовка
// auth-middleware отвечает 401, не доходя до PostTasksIdConfirm (тикет 1.4).
func TestPostTasksIdConfirm_RequiresBearerToken(t *testing.T) {
	s := newTestServer(fakeQuerier{})
	s.SetTransitioner(&fakeTransitioner{})
	router := NewRouter(s)

	req := httptest.NewRequest(http.MethodPost, "/tasks/"+uuid.New().String()+"/confirm", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("статус = %d (%s), ожидался 401", rec.Code, rec.Body.String())
	}
}

// TestPostTasksIdConfirm_TaskNotFound — чужая/несуществующая задача
// (GetTaskByIDAndUser → pgx.ErrNoRows) → 404 (FR A4, I3).
func TestPostTasksIdConfirm_TaskNotFound(t *testing.T) {
	userID := uuid.New()
	taskID := uuid.New()
	rec := doPostTasksIdConfirm(t, postTasksServer{
		q:            fakeQuerier{getTaskByIDAndUserErr: pgx.ErrNoRows},
		transitioner: &fakeTransitioner{},
	}, userID, taskID)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("статус = %d (%s), ожидался 404", rec.Code, rec.Body.String())
	}
}

// TestPostTasksIdConfirm_TaskLookupInternalError — неожиданная ошибка при
// проверке владения задачей → 500.
func TestPostTasksIdConfirm_TaskLookupInternalError(t *testing.T) {
	userID := uuid.New()
	taskID := uuid.New()
	rec := doPostTasksIdConfirm(t, postTasksServer{
		q:            fakeQuerier{getTaskByIDAndUserErr: context.DeadlineExceeded},
		transitioner: &fakeTransitioner{},
	}, userID, taskID)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("статус = %d (%s), ожидался 500", rec.Code, rec.Body.String())
	}
}

// TestPostTasksIdConfirm_NoTransitionerConfigured — transitioner не установлен
// (nil) → 500.
func TestPostTasksIdConfirm_NoTransitionerConfigured(t *testing.T) {
	userID := uuid.New()
	taskID := uuid.New()
	rec := doPostTasksIdConfirm(t, postTasksServer{
		q:            fakeQuerier{getTaskByIDAndUserResult: db.Task{ID: pgtype.UUID{Bytes: taskID, Valid: true}}},
		transitioner: nil,
	}, userID, taskID)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("статус = %d (%s), ожидался 500", rec.Code, rec.Body.String())
	}
}

// TestPostTasksIdConfirm_TransitionError — Transition вернул ошибку. Это
// покрывает в т.ч. приёмку «confirm запрещён из статусов, отличных от
// awaiting_confirm»: сама проверка недопустимости перехода — на уровне
// task.NextStatus (уже протестирована в fsm_test.go тикета 5.2); здесь
// проверяется только то, что HTTP-обработчик корректно транслирует ошибку
// Transition в 500 — тот же паттерн, что и
// TestPostTasksIdApprove_TransitionError/TestPostTasksIdAnswer_TransitionError,
// отдельной ветки/статуса для недопустимого перехода в проекте не заведено.
func TestPostTasksIdConfirm_TransitionError(t *testing.T) {
	userID := uuid.New()
	taskID := uuid.New()
	rec := doPostTasksIdConfirm(t, postTasksServer{
		q:            fakeQuerier{getTaskByIDAndUserResult: db.Task{ID: pgtype.UUID{Bytes: taskID, Valid: true}}},
		transitioner: &fakeTransitioner{err: context.DeadlineExceeded},
	}, userID, taskID)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("статус = %d (%s), ожидался 500", rec.Code, rec.Body.String())
	}
}

// TestPostTasksIdConfirm_HappyPath — успешное подтверждение завершения (FR
// E2, Gherkin §7 «Пользователь подтверждает завершение»): Transition вызван с
// (taskID, task.TriggerUserConfirmed), 200, тело ответа содержит
// status="completed".
func TestPostTasksIdConfirm_HappyPath(t *testing.T) {
	userID := uuid.New()
	taskID := uuid.New()
	integrationID := uuid.New()

	transitioner := &fakeTransitioner{to: task.StatusCompleted}
	rec := doPostTasksIdConfirm(t, postTasksServer{
		q: fakeQuerier{
			getTaskByIDAndUserResult: db.Task{
				ID:            pgtype.UUID{Bytes: taskID, Valid: true},
				IntegrationID: pgtype.UUID{Bytes: integrationID, Valid: true},
			},
		},
		transitioner: transitioner,
	}, userID, taskID)

	if rec.Code != http.StatusOK {
		t.Fatalf("статус = %d (%s), ожидался 200", rec.Code, rec.Body.String())
	}

	if transitioner.lastTaskID.Bytes != taskID {
		t.Fatalf("Transition вызван с taskID = %s, ожидался %s", uuid.UUID(transitioner.lastTaskID.Bytes), taskID)
	}
	if transitioner.lastTrigger != task.TriggerUserConfirmed {
		t.Fatalf("Transition вызван с trigger = %q, ожидался %q", transitioner.lastTrigger, task.TriggerUserConfirmed)
	}

	var respTask Task
	if err := json.Unmarshal(rec.Body.Bytes(), &respTask); err != nil {
		t.Fatalf("unmarshal тела: %v", err)
	}
	if respTask.Status == nil || *respTask.Status != TaskStatus(task.StatusCompleted) {
		t.Fatalf("status = %v, ожидался %q", respTask.Status, task.StatusCompleted)
	}
}

// doPostTasksIdReject прогоняет POST /tasks/{id}/reject через роутер. body ==
// nil означает запрос вообще без тела (requestBody не required в контракте,
// api/openapi.yaml) — обычный случай "отклонение без комментария".
func doPostTasksIdReject(t *testing.T, cfg postTasksServer, userID, taskID uuid.UUID, body []byte) *httptest.ResponseRecorder {
	t.Helper()

	s := newTestServer(cfg.q)
	s.SetTransitioner(cfg.transitioner)
	s.SetCommandPublisher(cfg.publisher)
	router := NewRouter(s)

	var req *http.Request
	if body != nil {
		req = httptest.NewRequest(http.MethodPost, "/tasks/"+taskID.String()+"/reject", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(http.MethodPost, "/tasks/"+taskID.String()+"/reject", nil)
	}
	req.Header.Set("Authorization", "Bearer "+issueTestAccessToken(t, userID, time.Now()))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

// TestPostTasksIdReject_RequiresBearerToken — без Authorization-заголовка
// auth-middleware отвечает 401, не доходя до PostTasksIdReject (тикет 1.4).
func TestPostTasksIdReject_RequiresBearerToken(t *testing.T) {
	s := newTestServer(fakeQuerier{})
	s.SetTransitioner(&fakeTransitioner{})
	router := NewRouter(s)

	req := httptest.NewRequest(http.MethodPost, "/tasks/"+uuid.New().String()+"/reject", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("статус = %d (%s), ожидался 401", rec.Code, rec.Body.String())
	}
}

// TestPostTasksIdReject_TaskNotFound — чужая/несуществующая задача
// (GetTaskByIDAndUser → pgx.ErrNoRows) → 404 (FR A4, I3).
func TestPostTasksIdReject_TaskNotFound(t *testing.T) {
	userID := uuid.New()
	taskID := uuid.New()
	rec := doPostTasksIdReject(t, postTasksServer{
		q:            fakeQuerier{getTaskByIDAndUserErr: pgx.ErrNoRows},
		transitioner: &fakeTransitioner{},
	}, userID, taskID, nil)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("статус = %d (%s), ожидался 404", rec.Code, rec.Body.String())
	}
}

// TestPostTasksIdReject_TaskLookupInternalError — неожиданная ошибка при
// проверке владения задачей → 500.
func TestPostTasksIdReject_TaskLookupInternalError(t *testing.T) {
	userID := uuid.New()
	taskID := uuid.New()
	rec := doPostTasksIdReject(t, postTasksServer{
		q:            fakeQuerier{getTaskByIDAndUserErr: context.DeadlineExceeded},
		transitioner: &fakeTransitioner{},
	}, userID, taskID, nil)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("статус = %d (%s), ожидался 500", rec.Code, rec.Body.String())
	}
}

// TestPostTasksIdReject_NoTransitionerConfigured — transitioner не установлен
// (nil) → 500.
func TestPostTasksIdReject_NoTransitionerConfigured(t *testing.T) {
	userID := uuid.New()
	taskID := uuid.New()
	rec := doPostTasksIdReject(t, postTasksServer{
		q:            fakeQuerier{getTaskByIDAndUserResult: db.Task{ID: pgtype.UUID{Bytes: taskID, Valid: true}}},
		transitioner: nil,
	}, userID, taskID, nil)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("статус = %d (%s), ожидался 500", rec.Code, rec.Body.String())
	}
}

// TestPostTasksIdReject_TransitionError — Transition вернул ошибку. Это
// покрывает в т.ч. приёмку «reject запрещён из статусов, отличных от
// awaiting_confirm»: сама проверка недопустимости перехода — на уровне
// task.NextStatus (уже протестирована в fsm_test.go тикета 5.2); здесь
// проверяется только то, что HTTP-обработчик корректно транслирует ошибку
// Transition в 500 — тот же паттерн, что и
// TestPostTasksIdConfirm_TransitionError/TestPostTasksIdApprove_TransitionError,
// отдельной ветки/статуса для недопустимого перехода в проекте не заведено.
func TestPostTasksIdReject_TransitionError(t *testing.T) {
	userID := uuid.New()
	taskID := uuid.New()
	rec := doPostTasksIdReject(t, postTasksServer{
		q:            fakeQuerier{getTaskByIDAndUserResult: db.Task{ID: pgtype.UUID{Bytes: taskID, Valid: true}}},
		transitioner: &fakeTransitioner{err: context.DeadlineExceeded},
	}, userID, taskID, nil)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("статус = %d (%s), ожидался 500", rec.Code, rec.Body.String())
	}
}

// TestPostTasksIdReject_HappyPath_NoComment — успешное отклонение результата
// без тела запроса (FR E2, Gherkin §7 «Пользователь отклоняет результат»):
// Transition вызван с (taskID, task.TriggerCompletionRejected), 200, тело
// ответа содержит status="running".
func TestPostTasksIdReject_HappyPath_NoComment(t *testing.T) {
	userID := uuid.New()
	taskID := uuid.New()
	integrationID := uuid.New()

	transitioner := &fakeTransitioner{to: task.StatusRunning}
	rec := doPostTasksIdReject(t, postTasksServer{
		q: fakeQuerier{
			getTaskByIDAndUserResult: db.Task{
				ID:            pgtype.UUID{Bytes: taskID, Valid: true},
				IntegrationID: pgtype.UUID{Bytes: integrationID, Valid: true},
			},
		},
		transitioner: transitioner,
	}, userID, taskID, nil)

	if rec.Code != http.StatusOK {
		t.Fatalf("статус = %d (%s), ожидался 200", rec.Code, rec.Body.String())
	}

	if transitioner.lastTaskID.Bytes != taskID {
		t.Fatalf("Transition вызван с taskID = %s, ожидался %s", uuid.UUID(transitioner.lastTaskID.Bytes), taskID)
	}
	if transitioner.lastTrigger != task.TriggerCompletionRejected {
		t.Fatalf("Transition вызван с trigger = %q, ожидался %q", transitioner.lastTrigger, task.TriggerCompletionRejected)
	}

	var respTask Task
	if err := json.Unmarshal(rec.Body.Bytes(), &respTask); err != nil {
		t.Fatalf("unmarshal тела: %v", err)
	}
	if respTask.Status == nil || *respTask.Status != TaskStatus(task.StatusRunning) {
		t.Fatalf("status = %v, ожидался %q", respTask.Status, task.StatusRunning)
	}
}

// TestPostTasksIdReject_HappyPath_WithComment — то же самое, но с телом
// {"comment": "..."}: комментарий принимается и не ломает обработку, даже
// если он не персистится (тикет 8.3 — вне объёма хранение комментария, см.
// комментарий над PostTasksIdReject).
func TestPostTasksIdReject_HappyPath_WithComment(t *testing.T) {
	userID := uuid.New()
	taskID := uuid.New()
	integrationID := uuid.New()

	transitioner := &fakeTransitioner{to: task.StatusRunning}
	rec := doPostTasksIdReject(t, postTasksServer{
		q: fakeQuerier{
			getTaskByIDAndUserResult: db.Task{
				ID:            pgtype.UUID{Bytes: taskID, Valid: true},
				IntegrationID: pgtype.UUID{Bytes: integrationID, Valid: true},
			},
		},
		transitioner: transitioner,
	}, userID, taskID, []byte(`{"comment":"нужно доделать логирование"}`))

	if rec.Code != http.StatusOK {
		t.Fatalf("статус = %d (%s), ожидался 200", rec.Code, rec.Body.String())
	}

	if transitioner.lastTrigger != task.TriggerCompletionRejected {
		t.Fatalf("Transition вызван с trigger = %q, ожидался %q", transitioner.lastTrigger, task.TriggerCompletionRejected)
	}

	var respTask Task
	if err := json.Unmarshal(rec.Body.Bytes(), &respTask); err != nil {
		t.Fatalf("unmarshal тела: %v", err)
	}
	if respTask.Status == nil || *respTask.Status != TaskStatus(task.StatusRunning) {
		t.Fatalf("status = %v, ожидался %q", respTask.Status, task.StatusRunning)
	}
}

// TestPostTasksIdReject_InvalidJSONBody — заведомо битый JSON в теле → 400,
// не доходя до Transition.
func TestPostTasksIdReject_InvalidJSONBody(t *testing.T) {
	userID := uuid.New()
	taskID := uuid.New()
	rec := doPostTasksIdReject(t, postTasksServer{
		q:            fakeQuerier{getTaskByIDAndUserResult: db.Task{ID: pgtype.UUID{Bytes: taskID, Valid: true}}},
		transitioner: &fakeTransitioner{},
	}, userID, taskID, []byte(`{"comment":`))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("статус = %d (%s), ожидался 400", rec.Code, rec.Body.String())
	}
}
