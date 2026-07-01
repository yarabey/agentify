//go:build integration

// Integration-тесты Transition на РЕАЛЬНОМ Postgres через testcontainers-go
// (тикет 5.2, FR E1) — тот же паттерн, что и
// orchestrator/internal/db/migrate_integration_test.go: собственный
// startPostgres-хелпер (пакеты оркестратора не шарят его специально, см.
// AGENTS.md/бриф тикета), тег integration, чтобы `make test` (unit) оставался
// быстрым и не требовал docker; CI-джоба `integration` гоняет
// `go test -tags=integration ./...`.
//
// Проверяемые сценарии (приёмка 5.2):
//   - цепочка валидных переходов created→queued→running→waiting_user→
//     running→awaiting_confirm→completed: каждый шаг проверяется и по
//     возвращённым (from,to), и по реальному состоянию в БД (tasks.status,
//     tasks.updated_at, task_events.seq/type) — Transition пишет РОВНО то,
//     что заявлено (FR E1, F2, F4);
//   - недопустимый переход из терминального статуса completed откатывает
//     транзакцию целиком: ни tasks.status, ни количество строк task_events
//     не меняются.
package task_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/yarabey/agentify/orchestrator/internal/db"
	"github.com/yarabey/agentify/orchestrator/internal/task"
	"github.com/yarabey/agentify/orchestrator/migrations"
)

const (
	// pgImage тянется через настроенный daemon registry-mirror (тикет 0.3,
	// mirror.gcr.io) — прокси не обходим.
	pgImage = "postgres:16-alpine"
	pgUser  = "task_test"
	pgPass  = "task_test"
	pgDB    = "task_test"
)

// startPostgres поднимает одиночный Postgres в контейнере и возвращает строку
// подключения (pgx/libpq URL) и функцию очистки (terminate). Копия паттерна
// orchestrator/internal/db/migrate_integration_test.go — каждый пакет держит
// свой собственный копипаст (нет общего testutil-пакета в кодовой базе).
func startPostgres(ctx context.Context, t *testing.T) (string, func()) {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image:        pgImage,
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     pgUser,
			"POSTGRES_PASSWORD": pgPass,
			"POSTGRES_DB":       pgDB,
		},
		// ВАЖНО: НЕ используем wait.ForListeningPort — см. обоснование в
		// orchestrator/internal/db/migrate_integration_test.go (образ postgres
		// печатает готовность дважды: временный старт initdb + финальный).
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(2 * time.Minute),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("поднять Postgres-контейнер: %v", err)
	}
	cleanup := func() {
		if terr := testcontainers.TerminateContainer(container); terr != nil {
			t.Logf("terminate Postgres: %v", terr)
		}
	}

	host, err := container.Host(ctx)
	if err != nil {
		cleanup()
		t.Fatalf("получить host контейнера: %v", err)
	}
	port, err := container.MappedPort(ctx, "5432/tcp")
	if err != nil {
		cleanup()
		t.Fatalf("получить порт контейнера: %v", err)
	}
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		pgUser, pgPass, host, port.Port(), pgDB)
	return dsn, cleanup
}

// setupPool поднимает Postgres, применяет миграции goose и возвращает готовый
// *pgxpool.Pool + функцию очистки (закрывает пул и завершает контейнер).
func setupPool(ctx context.Context, t *testing.T) (*pgxpool.Pool, func()) {
	t.Helper()
	dsn, cleanupContainer := startPostgres(ctx, t)

	sqlDB, err := sql.Open("pgx", dsn)
	if err != nil {
		cleanupContainer()
		t.Fatalf("sql.Open: %v", err)
	}
	goose.SetBaseFS(migrations.FS)
	if derr := goose.SetDialect("postgres"); derr != nil {
		cleanupContainer()
		t.Fatalf("goose SetDialect: %v", derr)
	}
	if uperr := goose.UpContext(ctx, sqlDB, "."); uperr != nil {
		cleanupContainer()
		t.Fatalf("goose Up: %v", uperr)
	}
	_ = sqlDB.Close()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		cleanupContainer()
		t.Fatalf("pgxpool.New: %v", err)
	}
	return pool, func() {
		pool.Close()
		cleanupContainer()
	}
}

// seedTask создаёт владельца, интеграцию и задачу (стартовый статус
// 'created' — DEFAULT схемы), возвращает id задачи.
func seedTask(ctx context.Context, t *testing.T, pool *pgxpool.Pool, q *db.Queries, username string) pgtype.UUID {
	t.Helper()
	owner, err := q.CreateUser(ctx, db.CreateUserParams{
		Username:     username,
		PasswordHash: "argon2id$stub",
		IsAdmin:      false,
	})
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	var integrationID pgtype.UUID
	err = pool.QueryRow(ctx, `
		INSERT INTO integrations (user_id, name, uuid_hmac, uuid_enc)
		VALUES ($1, $2, $3, $4)
		RETURNING id`,
		owner.ID, username+"-machine", username+"-hmac", []byte("ciphertext-stub")).Scan(&integrationID)
	if err != nil {
		t.Fatalf("вставка integrations: %v", err)
	}

	var taskID pgtype.UUID
	err = pool.QueryRow(ctx, `
		INSERT INTO tasks (user_id, integration_id, text_enc)
		VALUES ($1, $2, $3)
		RETURNING id`,
		owner.ID, integrationID, []byte("text-ciphertext-stub")).Scan(&taskID)
	if err != nil {
		t.Fatalf("вставка tasks: %v", err)
	}
	return taskID
}

// taskRow — снимок tasks.status/updated_at для сравнения до/после Transition.
type taskRow struct {
	status    string
	updatedAt time.Time
}

func getTaskRow(ctx context.Context, t *testing.T, pool *pgxpool.Pool, taskID pgtype.UUID) taskRow {
	t.Helper()
	var row taskRow
	err := pool.QueryRow(ctx, `SELECT status, updated_at FROM tasks WHERE id = $1`, taskID).
		Scan(&row.status, &row.updatedAt)
	if err != nil {
		t.Fatalf("прочитать tasks: %v", err)
	}
	return row
}

// eventRow — снимок одной строки task_events (для проверки seq/type).
type eventRow struct {
	seq     int64
	evtType string
}

func listTaskEvents(ctx context.Context, t *testing.T, pool *pgxpool.Pool, taskID pgtype.UUID) []eventRow {
	t.Helper()
	rows, err := pool.Query(ctx, `SELECT seq, type FROM task_events WHERE task_id = $1 ORDER BY seq`, taskID)
	if err != nil {
		t.Fatalf("прочитать task_events: %v", err)
	}
	defer rows.Close()

	var events []eventRow
	for rows.Next() {
		var e eventRow
		if serr := rows.Scan(&e.seq, &e.evtType); serr != nil {
			t.Fatalf("сканировать task_events: %v", serr)
		}
		events = append(events, e)
	}
	if rerr := rows.Err(); rerr != nil {
		t.Fatalf("итерация task_events: %v", rerr)
	}
	return events
}

// eventFullRow — снимок одной строки task_events включая id/ref_event_id —
// нужен TestIntegration_TransitionWithEvent_* (тикет 6.1) для проверки
// сопоставления agent_question/user_answer по id/ref_event_id, которого
// eventRow (seq/type) не даёт.
type eventFullRow struct {
	id         pgtype.UUID
	seq        int64
	evtType    string
	refEventID pgtype.UUID
}

func listTaskEventsFull(ctx context.Context, t *testing.T, pool *pgxpool.Pool, taskID pgtype.UUID) []eventFullRow {
	t.Helper()
	rows, err := pool.Query(ctx, `SELECT id, seq, type, ref_event_id FROM task_events WHERE task_id = $1 ORDER BY seq`, taskID)
	if err != nil {
		t.Fatalf("прочитать task_events: %v", err)
	}
	defer rows.Close()

	var events []eventFullRow
	for rows.Next() {
		var e eventFullRow
		if serr := rows.Scan(&e.id, &e.seq, &e.evtType, &e.refEventID); serr != nil {
			t.Fatalf("сканировать task_events: %v", serr)
		}
		events = append(events, e)
	}
	if rerr := rows.Err(); rerr != nil {
		t.Fatalf("итерация task_events: %v", rerr)
	}
	return events
}

// TestIntegration_Transition_ValidChain — приёмка 5.2: цепочка валидных
// переходов created→queued→running→waiting_user→running→awaiting_confirm→
// completed. На каждом шаге проверяем и возвращённые (from,to), и реальное
// состояние в БД: tasks.status совпадает с to, tasks.updated_at не убывает,
// task_events получает новую строку type='status_change' со строго
// возрастающим seq (1..6).
func TestIntegration_Transition_ValidChain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, cleanup := setupPool(ctx, t)
	defer cleanup()

	q := db.New(pool)
	taskID := seedTask(ctx, t, pool, q, "chain-owner")

	tr := task.NewTransitioner(pool)

	steps := []struct {
		trigger task.Trigger
		want    task.Status
	}{
		{task.TriggerEnqueued, task.StatusQueued},
		{task.TriggerTaskAccepted, task.StatusRunning},
		{task.TriggerAgentQuestion, task.StatusWaitingUser},
		{task.TriggerUserAnswered, task.StatusRunning},
		{task.TriggerAgentCompleted, task.StatusAwaitingConfirm},
		{task.TriggerUserConfirmed, task.StatusCompleted},
	}

	prevUpdatedAt := getTaskRow(ctx, t, pool, taskID).updatedAt
	var prevStatus task.Status = task.StatusCreated

	for i, step := range steps {
		from, to, err := tr.Transition(ctx, taskID, step.trigger)
		if err != nil {
			t.Fatalf("шаг %d: Transition(%s, %s): %v", i+1, prevStatus, step.trigger, err)
		}
		if from != prevStatus {
			t.Fatalf("шаг %d: from = %s, хотим %s", i+1, from, prevStatus)
		}
		if to != step.want {
			t.Fatalf("шаг %d: to = %s, хотим %s", i+1, to, step.want)
		}

		row := getTaskRow(ctx, t, pool, taskID)
		if row.status != string(step.want) {
			t.Fatalf("шаг %d: tasks.status в БД = %s, хотим %s", i+1, row.status, step.want)
		}
		if row.updatedAt.Before(prevUpdatedAt) {
			t.Fatalf("шаг %d: tasks.updated_at не должен убывать: было %v, стало %v", i+1, prevUpdatedAt, row.updatedAt)
		}

		events := listTaskEvents(ctx, t, pool, taskID)
		wantSeq := int64(i + 1)
		if int64(len(events)) != wantSeq {
			t.Fatalf("шаг %d: ожидалось %d событий task_events, получено %d", i+1, wantSeq, len(events))
		}
		last := events[len(events)-1]
		if last.seq != wantSeq {
			t.Fatalf("шаг %d: seq последнего события = %d, хотим %d", i+1, last.seq, wantSeq)
		}
		if last.evtType != "status_change" {
			t.Fatalf("шаг %d: type последнего события = %s, хотим status_change", i+1, last.evtType)
		}

		prevStatus = to
		prevUpdatedAt = row.updatedAt
	}
}

// TestIntegration_Transition_InvalidRollsBack — приёмка 5.2: недопустимый
// переход из терминального статуса completed возвращает ошибку и НЕ меняет
// состояние в БД (доказывает откат транзакции целиком).
func TestIntegration_Transition_InvalidRollsBack(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, cleanup := setupPool(ctx, t)
	defer cleanup()

	q := db.New(pool)
	taskID := seedTask(ctx, t, pool, q, "terminal-owner")

	// Довести задачу до completed отдельной валидной цепочкой.
	tr := task.NewTransitioner(pool)
	for _, trigger := range []task.Trigger{
		task.TriggerEnqueued,
		task.TriggerTaskAccepted,
		task.TriggerAgentCompleted,
		task.TriggerUserConfirmed,
	} {
		if _, _, err := tr.Transition(ctx, taskID, trigger); err != nil {
			t.Fatalf("подготовка (%s): %v", trigger, err)
		}
	}

	before := getTaskRow(ctx, t, pool, taskID)
	if before.status != string(task.StatusCompleted) {
		t.Fatalf("подготовка: статус = %s, хотим completed", before.status)
	}
	eventsBefore := listTaskEvents(ctx, t, pool, taskID)

	from, to, err := tr.Transition(ctx, taskID, task.TriggerCancelRequested)
	if err == nil {
		t.Fatalf("Transition из completed с cancel_requested должен вернуть ошибку, получено to=%s", to)
	}
	if from != task.StatusCompleted {
		t.Fatalf("from при ошибке = %s, хотим completed", from)
	}
	if to != "" {
		t.Fatalf("to при ошибке должен быть пустым, получено %s", to)
	}

	after := getTaskRow(ctx, t, pool, taskID)
	if after.status != string(task.StatusCompleted) {
		t.Fatalf("после недопустимого перехода tasks.status изменился: было completed, стало %s", after.status)
	}
	eventsAfter := listTaskEvents(ctx, t, pool, taskID)
	if len(eventsAfter) != len(eventsBefore) {
		t.Fatalf("после недопустимого перехода количество task_events изменилось: было %d, стало %d",
			len(eventsBefore), len(eventsAfter))
	}
}

// TestIntegration_TransitionWithEvent_HappyPath — приёмка тикета 6.1: агент
// задал вопрос (running→waiting_user) и пользователь ответил
// (waiting_user→running), каждый переход атомарно пишет ДВЕ записи
// task_events — бизнес-событие (agent_question/user_answer) и status_change,
// со строго возрастающим seq, причём событие-ответ (user_answer) несёт
// ref_event_id, указывающий РОВНО на строку task_events исходного вопроса
// (не «последний вопрос», см. бриф тикета — сопоставление по question_id/id
// события, задел под 6.2 «несколько вопросов»).
func TestIntegration_TransitionWithEvent_HappyPath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, cleanup := setupPool(ctx, t)
	defer cleanup()

	q := db.New(pool)
	taskID := seedTask(ctx, t, pool, q, "qna-owner")

	tr := task.NewTransitioner(pool)

	// Довести задачу до running обычным Transition (без доп. события).
	for _, trigger := range []task.Trigger{task.TriggerEnqueued, task.TriggerTaskAccepted} {
		if _, _, err := tr.Transition(ctx, taskID, trigger); err != nil {
			t.Fatalf("подготовка (%s): %v", trigger, err)
		}
	}

	// Агент задаёт вопрос: running → waiting_user, ref_event_id вопроса NULL.
	questionPayload := []byte(`{"question_id":"q-1","text":"продолжать?"}`)
	from, to, err := tr.TransitionWithEvent(ctx, taskID, task.TriggerAgentQuestion,
		"agent_question", pgtype.UUID{}, questionPayload)
	if err != nil {
		t.Fatalf("TransitionWithEvent(agent_question): %v", err)
	}
	if from != task.StatusRunning || to != task.StatusWaitingUser {
		t.Fatalf("agent_question: from=%s to=%s, хотим running→waiting_user", from, to)
	}

	events := listTaskEventsFull(ctx, t, pool, taskID)
	// seq 1=enqueued(status_change) 2=task_accepted(status_change)
	// 3=agent_question 4=status_change(→waiting_user)
	if len(events) != 4 {
		t.Fatalf("после agent_question ожидалось 4 события, получено %d", len(events))
	}
	questionEvent := events[2]
	if questionEvent.seq != 3 || questionEvent.evtType != "agent_question" {
		t.Fatalf("событие 3 = (seq=%d, type=%s), хотим (3, agent_question)", questionEvent.seq, questionEvent.evtType)
	}
	if questionEvent.refEventID.Valid {
		t.Fatalf("ref_event_id вопроса должен быть NULL, получено %v", questionEvent.refEventID)
	}
	statusChangeAfterQuestion := events[3]
	if statusChangeAfterQuestion.seq != 4 || statusChangeAfterQuestion.evtType != "status_change" {
		t.Fatalf("событие 4 = (seq=%d, type=%s), хотим (4, status_change)", statusChangeAfterQuestion.seq, statusChangeAfterQuestion.evtType)
	}

	row := getTaskRow(ctx, t, pool, taskID)
	if row.status != string(task.StatusWaitingUser) {
		t.Fatalf("tasks.status в БД = %s, хотим waiting_user", row.status)
	}

	// Пользователь отвечает: waiting_user → running, ref_event_id указывает
	// ИМЕННО на event.id вопроса (questionEvent.id), а не на что-то другое.
	answerPayload := []byte(`{"question_id":"q-1","text":"да"}`)
	from, to, err = tr.TransitionWithEvent(ctx, taskID, task.TriggerUserAnswered,
		"user_answer", questionEvent.id, answerPayload)
	if err != nil {
		t.Fatalf("TransitionWithEvent(user_answer): %v", err)
	}
	if from != task.StatusWaitingUser || to != task.StatusRunning {
		t.Fatalf("user_answer: from=%s to=%s, хотим waiting_user→running", from, to)
	}

	events = listTaskEventsFull(ctx, t, pool, taskID)
	if len(events) != 6 {
		t.Fatalf("после user_answer ожидалось 6 событий, получено %d", len(events))
	}
	answerEvent := events[4]
	if answerEvent.seq != 5 || answerEvent.evtType != "user_answer" {
		t.Fatalf("событие 5 = (seq=%d, type=%s), хотим (5, user_answer)", answerEvent.seq, answerEvent.evtType)
	}
	if answerEvent.refEventID != questionEvent.id {
		t.Fatalf("ref_event_id ответа = %v, хотим id вопроса %v", answerEvent.refEventID, questionEvent.id)
	}
	statusChangeAfterAnswer := events[5]
	if statusChangeAfterAnswer.seq != 6 || statusChangeAfterAnswer.evtType != "status_change" {
		t.Fatalf("событие 6 = (seq=%d, type=%s), хотим (6, status_change)", statusChangeAfterAnswer.seq, statusChangeAfterAnswer.evtType)
	}

	row = getTaskRow(ctx, t, pool, taskID)
	if row.status != string(task.StatusRunning) {
		t.Fatalf("tasks.status в БД = %s, хотим running", row.status)
	}
}

// TestIntegration_TransitionWithEvent_TwoQuestionsCorrectBinding — приёмка
// тикета 6.2 (FR F2, §5 «Несколько вопросов сопоставляются корректно»): два
// ПОСЛЕДОВАТЕЛЬНЫХ цикла вопрос/ответ для одной задачи (running→waiting_user→
// running с question_id=q-1, затем снова running→waiting_user→running с
// question_id=q-2) на РЕАЛЬНОМ Postgres — проверяем, что ref_event_id каждого
// user_answer в БД указывает именно на "свой" agent_question, а не перепутан
// со вторым (FSM МВП допускает только последовательные вопросы — см. бриф
// тикета 6.2, {waiting_user, agent_question} не является рёбром fsm.go, это
// ожидаемое поведение и не тестируется здесь).
func TestIntegration_TransitionWithEvent_TwoQuestionsCorrectBinding(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, cleanup := setupPool(ctx, t)
	defer cleanup()

	q := db.New(pool)
	taskID := seedTask(ctx, t, pool, q, "two-questions-owner")

	tr := task.NewTransitioner(pool)

	// Довести задачу до running обычным Transition (без доп. события).
	for _, trigger := range []task.Trigger{task.TriggerEnqueued, task.TriggerTaskAccepted} {
		if _, _, err := tr.Transition(ctx, taskID, trigger); err != nil {
			t.Fatalf("подготовка (%s): %v", trigger, err)
		}
	}

	// --- Цикл 1: вопрос q-1, ответ на q-1 ---
	questionPayload1 := []byte(`{"question_id":"q-1","text":"продолжать с шагом 1?"}`)
	from, to, err := tr.TransitionWithEvent(ctx, taskID, task.TriggerAgentQuestion,
		"agent_question", pgtype.UUID{}, questionPayload1)
	if err != nil {
		t.Fatalf("цикл 1: TransitionWithEvent(agent_question q-1): %v", err)
	}
	if from != task.StatusRunning || to != task.StatusWaitingUser {
		t.Fatalf("цикл 1: agent_question: from=%s to=%s, хотим running→waiting_user", from, to)
	}

	events := listTaskEventsFull(ctx, t, pool, taskID)
	// seq 1=enqueued 2=task_accepted 3=agent_question(q-1) 4=status_change
	if len(events) != 4 {
		t.Fatalf("цикл 1: после agent_question ожидалось 4 события, получено %d", len(events))
	}
	question1Event := events[2]
	if question1Event.seq != 3 || question1Event.evtType != "agent_question" {
		t.Fatalf("цикл 1: событие 3 = (seq=%d, type=%s), хотим (3, agent_question)", question1Event.seq, question1Event.evtType)
	}

	answerPayload1 := []byte(`{"question_id":"q-1","text":"да, продолжай"}`)
	from, to, err = tr.TransitionWithEvent(ctx, taskID, task.TriggerUserAnswered,
		"user_answer", question1Event.id, answerPayload1)
	if err != nil {
		t.Fatalf("цикл 1: TransitionWithEvent(user_answer q-1): %v", err)
	}
	if from != task.StatusWaitingUser || to != task.StatusRunning {
		t.Fatalf("цикл 1: user_answer: from=%s to=%s, хотим waiting_user→running", from, to)
	}

	events = listTaskEventsFull(ctx, t, pool, taskID)
	// seq 5=user_answer(q-1) 6=status_change
	if len(events) != 6 {
		t.Fatalf("цикл 1: после user_answer ожидалось 6 событий, получено %d", len(events))
	}
	answer1Event := events[4]
	if answer1Event.seq != 5 || answer1Event.evtType != "user_answer" {
		t.Fatalf("цикл 1: событие 5 = (seq=%d, type=%s), хотим (5, user_answer)", answer1Event.seq, answer1Event.evtType)
	}
	if answer1Event.refEventID != question1Event.id {
		t.Fatalf("цикл 1: ref_event_id ответа = %v, хотим id вопроса q-1 = %v", answer1Event.refEventID, question1Event.id)
	}

	// --- Цикл 2: вопрос q-2, ответ на q-2 ---
	questionPayload2 := []byte(`{"question_id":"q-2","text":"продолжать с шагом 2?"}`)
	from, to, err = tr.TransitionWithEvent(ctx, taskID, task.TriggerAgentQuestion,
		"agent_question", pgtype.UUID{}, questionPayload2)
	if err != nil {
		t.Fatalf("цикл 2: TransitionWithEvent(agent_question q-2): %v", err)
	}
	if from != task.StatusRunning || to != task.StatusWaitingUser {
		t.Fatalf("цикл 2: agent_question: from=%s to=%s, хотим running→waiting_user", from, to)
	}

	events = listTaskEventsFull(ctx, t, pool, taskID)
	// seq 7=agent_question(q-2) 8=status_change
	if len(events) != 8 {
		t.Fatalf("цикл 2: после agent_question ожидалось 8 событий, получено %d", len(events))
	}
	question2Event := events[6]
	if question2Event.seq != 7 || question2Event.evtType != "agent_question" {
		t.Fatalf("цикл 2: событие 7 = (seq=%d, type=%s), хотим (7, agent_question)", question2Event.seq, question2Event.evtType)
	}
	if question2Event.id == question1Event.id {
		t.Fatalf("цикл 2: id вопроса q-2 совпал с id вопроса q-1 = %v, ожидались разные события", question1Event.id)
	}

	answerPayload2 := []byte(`{"question_id":"q-2","text":"да, продолжай и здесь"}`)
	from, to, err = tr.TransitionWithEvent(ctx, taskID, task.TriggerUserAnswered,
		"user_answer", question2Event.id, answerPayload2)
	if err != nil {
		t.Fatalf("цикл 2: TransitionWithEvent(user_answer q-2): %v", err)
	}
	if from != task.StatusWaitingUser || to != task.StatusRunning {
		t.Fatalf("цикл 2: user_answer: from=%s to=%s, хотим waiting_user→running", from, to)
	}

	// --- Итоговая проверка: РОВНО два user_answer, каждый указывает на "свой" вопрос ---
	events = listTaskEventsFull(ctx, t, pool, taskID)
	var answerEvents []eventFullRow
	for _, e := range events {
		if e.evtType == "user_answer" {
			answerEvents = append(answerEvents, e)
		}
	}
	if len(answerEvents) != 2 {
		t.Fatalf("ожидалось ровно 2 события user_answer, получено %d", len(answerEvents))
	}
	if answerEvents[0].refEventID != question1Event.id {
		t.Fatalf("первый user_answer: ref_event_id = %v, хотим id вопроса q-1 = %v", answerEvents[0].refEventID, question1Event.id)
	}
	if answerEvents[1].refEventID != question2Event.id {
		t.Fatalf("второй user_answer: ref_event_id = %v, хотим id вопроса q-2 = %v", answerEvents[1].refEventID, question2Event.id)
	}
	if answerEvents[0].refEventID == answerEvents[1].refEventID {
		t.Fatalf("ref_event_id первого и второго user_answer совпадают (%v) — ответы перепутаны", answerEvents[0].refEventID)
	}

	row := getTaskRow(ctx, t, pool, taskID)
	if row.status != string(task.StatusRunning) {
		t.Fatalf("итоговый tasks.status в БД = %s, хотим running", row.status)
	}
}

// TestIntegration_TransitionWithEvent_InvalidRollsBack — недопустимый переход
// (NextStatus вернул ошибку) откатывает транзакцию целиком: НИ бизнес-событие
// (agent_question), НИ status_change не должны быть записаны, а tasks.status
// не должен измениться (тикет 6.1, тот же принцип, что и
// TestIntegration_Transition_InvalidRollsBack для обычного Transition).
func TestIntegration_TransitionWithEvent_InvalidRollsBack(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, cleanup := setupPool(ctx, t)
	defer cleanup()

	q := db.New(pool)
	taskID := seedTask(ctx, t, pool, q, "qna-invalid-owner")

	tr := task.NewTransitioner(pool)

	before := getTaskRow(ctx, t, pool, taskID)
	if before.status != string(task.StatusCreated) {
		t.Fatalf("подготовка: статус = %s, хотим created", before.status)
	}
	eventsBefore := listTaskEvents(ctx, t, pool, taskID)

	// created + agent_question не является допустимым ребром FSM (см.
	// orchestrator/internal/task/fsm.go transitions).
	payload := []byte(`{"question_id":"q-1","text":"продолжать?"}`)
	from, to, err := tr.TransitionWithEvent(ctx, taskID, task.TriggerAgentQuestion,
		"agent_question", pgtype.UUID{}, payload)
	if err == nil {
		t.Fatalf("TransitionWithEvent(created, agent_question) должен вернуть ошибку, получено to=%s", to)
	}
	if from != task.StatusCreated {
		t.Fatalf("from при ошибке = %s, хотим created", from)
	}
	if to != "" {
		t.Fatalf("to при ошибке должен быть пустым, получено %s", to)
	}

	after := getTaskRow(ctx, t, pool, taskID)
	if after.status != string(task.StatusCreated) {
		t.Fatalf("после недопустимого перехода tasks.status изменился: было created, стало %s", after.status)
	}
	eventsAfter := listTaskEvents(ctx, t, pool, taskID)
	if len(eventsAfter) != len(eventsBefore) {
		t.Fatalf("после недопустимого перехода количество task_events изменилось: было %d, стало %d",
			len(eventsBefore), len(eventsAfter))
	}
}

// TestIntegration_TransitionWithEvent_CommandApprovalHappyPath — приёмка
// тикетов 6.3/6.4/6.5 (FR F3): агент запросил согласование команды вне
// allowlist (running→waiting_user) и пользователь вынес решение
// (waiting_user→running), зеркально TestIntegration_TransitionWithEvent_HappyPath
// (тикет 6.1), но для пары command_approval_request/user_decision — каждый
// переход атомарно пишет ДВЕ записи task_events (бизнес-событие и
// status_change) со строго возрастающим seq, причём user_decision несёт
// ref_event_id, указывающий РОВНО на строку task_events исходного
// command_approval_request.
func TestIntegration_TransitionWithEvent_CommandApprovalHappyPath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, cleanup := setupPool(ctx, t)
	defer cleanup()

	q := db.New(pool)
	taskID := seedTask(ctx, t, pool, q, "approval-owner")

	tr := task.NewTransitioner(pool)

	// Довести задачу до running обычным Transition (без доп. события).
	for _, trigger := range []task.Trigger{task.TriggerEnqueued, task.TriggerTaskAccepted} {
		if _, _, err := tr.Transition(ctx, taskID, trigger); err != nil {
			t.Fatalf("подготовка (%s): %v", trigger, err)
		}
	}

	// Агент запрашивает согласование команды вне allowlist: running →
	// waiting_user, ref_event_id запроса NULL.
	approvalPayload := []byte(`{"request_id":"r-1","command":"rm -rf /tmp/x","reason":"вне allowlist"}`)
	from, to, err := tr.TransitionWithEvent(ctx, taskID, task.TriggerApprovalRequested,
		"command_approval_request", pgtype.UUID{}, approvalPayload)
	if err != nil {
		t.Fatalf("TransitionWithEvent(command_approval_request): %v", err)
	}
	if from != task.StatusRunning || to != task.StatusWaitingUser {
		t.Fatalf("command_approval_request: from=%s to=%s, хотим running→waiting_user", from, to)
	}

	events := listTaskEventsFull(ctx, t, pool, taskID)
	// seq 1=enqueued(status_change) 2=task_accepted(status_change)
	// 3=command_approval_request 4=status_change(→waiting_user)
	if len(events) != 4 {
		t.Fatalf("после command_approval_request ожидалось 4 события, получено %d", len(events))
	}
	approvalEvent := events[2]
	if approvalEvent.seq != 3 || approvalEvent.evtType != "command_approval_request" {
		t.Fatalf("событие 3 = (seq=%d, type=%s), хотим (3, command_approval_request)", approvalEvent.seq, approvalEvent.evtType)
	}
	if approvalEvent.refEventID.Valid {
		t.Fatalf("ref_event_id запроса согласования должен быть NULL, получено %v", approvalEvent.refEventID)
	}
	statusChangeAfterApproval := events[3]
	if statusChangeAfterApproval.seq != 4 || statusChangeAfterApproval.evtType != "status_change" {
		t.Fatalf("событие 4 = (seq=%d, type=%s), хотим (4, status_change)", statusChangeAfterApproval.seq, statusChangeAfterApproval.evtType)
	}

	row := getTaskRow(ctx, t, pool, taskID)
	if row.status != string(task.StatusWaitingUser) {
		t.Fatalf("tasks.status в БД = %s, хотим waiting_user", row.status)
	}

	// Пользователь выносит решение: waiting_user → running, ref_event_id
	// указывает ИМЕННО на event.id запроса согласования (approvalEvent.id), а
	// не на что-то другое.
	decisionPayload := []byte(`{"request_id":"r-1","decision":"reject"}`)
	from, to, err = tr.TransitionWithEvent(ctx, taskID, task.TriggerCommandDecision,
		"user_decision", approvalEvent.id, decisionPayload)
	if err != nil {
		t.Fatalf("TransitionWithEvent(user_decision): %v", err)
	}
	if from != task.StatusWaitingUser || to != task.StatusRunning {
		t.Fatalf("user_decision: from=%s to=%s, хотим waiting_user→running", from, to)
	}

	events = listTaskEventsFull(ctx, t, pool, taskID)
	if len(events) != 6 {
		t.Fatalf("после user_decision ожидалось 6 событий, получено %d", len(events))
	}
	decisionEvent := events[4]
	if decisionEvent.seq != 5 || decisionEvent.evtType != "user_decision" {
		t.Fatalf("событие 5 = (seq=%d, type=%s), хотим (5, user_decision)", decisionEvent.seq, decisionEvent.evtType)
	}
	if decisionEvent.refEventID != approvalEvent.id {
		t.Fatalf("ref_event_id решения = %v, хотим id запроса согласования %v", decisionEvent.refEventID, approvalEvent.id)
	}
	statusChangeAfterDecision := events[5]
	if statusChangeAfterDecision.seq != 6 || statusChangeAfterDecision.evtType != "status_change" {
		t.Fatalf("событие 6 = (seq=%d, type=%s), хотим (6, status_change)", statusChangeAfterDecision.seq, statusChangeAfterDecision.evtType)
	}

	row = getTaskRow(ctx, t, pool, taskID)
	if row.status != string(task.StatusRunning) {
		t.Fatalf("tasks.status в БД = %s, хотим running", row.status)
	}
}

// TestIntegration_TransitionWithEvent_FullLifecycleAuditTrail — приёмка
// тикета 6.6 (FR F4: «Все действия агента и решения пользователя фиксируются
// для аудита»). В отличие от пофичевых тестов выше — HappyPath/
// TwoQuestionsCorrectBinding (тикеты 6.1/6.2, agent_question/user_answer) и
// CommandApprovalHappyPath (тикеты 6.3/6.4/6.5, command_approval_request/
// user_decision, добавлен этим же тикетом) — этот тест ХОЛИСТИЧЕСКИЙ: он
// прогоняет ОБЕ ветки подряд для ОДНОЙ задачи и проверяет ПОЛНЫЙ,
// упорядоченный по seq список событий task_events без единого пропуска — то
// есть буквально доказывает «каждое решение оставляет запись» для всего
// смёрженного на момент 6.6 функционала (deps 6.1). agent_completed/
// agent_progress сознательно НЕ используются — они вне scope 6.6 (см. тикеты
// 8.1 и Phase 2+ соответственно), тест намеренно завершается в состоянии
// running после второго цикла.
func TestIntegration_TransitionWithEvent_FullLifecycleAuditTrail(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, cleanup := setupPool(ctx, t)
	defer cleanup()

	q := db.New(pool)
	taskID := seedTask(ctx, t, pool, q, "full-lifecycle-owner")

	tr := task.NewTransitioner(pool)

	// --- created → queued → running (seq 1, 2: status_change) ---
	for _, trigger := range []task.Trigger{task.TriggerEnqueued, task.TriggerTaskAccepted} {
		if _, _, err := tr.Transition(ctx, taskID, trigger); err != nil {
			t.Fatalf("подготовка (%s): %v", trigger, err)
		}
	}

	// --- Цикл вопрос/ответ (тикеты 6.1/6.2, FR F1/F2): seq 3=agent_question
	// 4=status_change 5=user_answer 6=status_change ---
	questionPayload := []byte(`{"question_id":"q-1","text":"продолжать?"}`)
	from, to, err := tr.TransitionWithEvent(ctx, taskID, task.TriggerAgentQuestion,
		"agent_question", pgtype.UUID{}, questionPayload)
	if err != nil {
		t.Fatalf("TransitionWithEvent(agent_question): %v", err)
	}
	if from != task.StatusRunning || to != task.StatusWaitingUser {
		t.Fatalf("agent_question: from=%s to=%s, хотим running→waiting_user", from, to)
	}
	questionEvent := listTaskEventsFull(ctx, t, pool, taskID)[2]

	answerPayload := []byte(`{"question_id":"q-1","text":"да"}`)
	from, to, err = tr.TransitionWithEvent(ctx, taskID, task.TriggerUserAnswered,
		"user_answer", questionEvent.id, answerPayload)
	if err != nil {
		t.Fatalf("TransitionWithEvent(user_answer): %v", err)
	}
	if from != task.StatusWaitingUser || to != task.StatusRunning {
		t.Fatalf("user_answer: from=%s to=%s, хотим waiting_user→running", from, to)
	}

	// --- Цикл согласования команды (тикеты 6.3/6.4/6.5, FR F3): seq
	// 7=command_approval_request 8=status_change 9=user_decision
	// 10=status_change; decision=="approve" для симметрии с
	// CommandApprovalHappyPath (там reject) ---
	approvalPayload := []byte(`{"request_id":"r-1","command":"git push --force","reason":"вне allowlist"}`)
	from, to, err = tr.TransitionWithEvent(ctx, taskID, task.TriggerApprovalRequested,
		"command_approval_request", pgtype.UUID{}, approvalPayload)
	if err != nil {
		t.Fatalf("TransitionWithEvent(command_approval_request): %v", err)
	}
	if from != task.StatusRunning || to != task.StatusWaitingUser {
		t.Fatalf("command_approval_request: from=%s to=%s, хотим running→waiting_user", from, to)
	}
	approvalEvent := listTaskEventsFull(ctx, t, pool, taskID)[6]

	decisionPayload := []byte(`{"request_id":"r-1","decision":"approve"}`)
	from, to, err = tr.TransitionWithEvent(ctx, taskID, task.TriggerCommandDecision,
		"user_decision", approvalEvent.id, decisionPayload)
	if err != nil {
		t.Fatalf("TransitionWithEvent(user_decision): %v", err)
	}
	if from != task.StatusWaitingUser || to != task.StatusRunning {
		t.Fatalf("user_decision: from=%s to=%s, хотим waiting_user→running", from, to)
	}

	// --- Холистическая проверка: ПОЛНЫЙ, упорядоченный по seq список
	// task_events без единого пропуска (буквальная приёмка FR F4) ---
	wantTrail := []struct {
		seq     int64
		evtType string
	}{
		{1, "status_change"},
		{2, "status_change"},
		{3, "agent_question"},
		{4, "status_change"},
		{5, "user_answer"},
		{6, "status_change"},
		{7, "command_approval_request"},
		{8, "status_change"},
		{9, "user_decision"},
		{10, "status_change"},
	}

	events := listTaskEventsFull(ctx, t, pool, taskID)
	if len(events) != len(wantTrail) {
		t.Fatalf("аудиторский след: ожидалось %d событий task_events, получено %d", len(wantTrail), len(events))
	}
	for i, want := range wantTrail {
		got := events[i]
		if got.seq != want.seq || got.evtType != want.evtType {
			t.Fatalf("аудиторский след: событие %d = (seq=%d, type=%s), хотим (seq=%d, type=%s)",
				i+1, got.seq, got.evtType, want.seq, want.evtType)
		}
	}

	// Дополнительно: ref_event_id ответа/решения указывают именно на "свои"
	// запросы (agent_question/command_approval_request), а не перепутаны.
	if events[4].refEventID != questionEvent.id {
		t.Fatalf("аудиторский след: ref_event_id user_answer = %v, хотим id agent_question = %v",
			events[4].refEventID, questionEvent.id)
	}
	if events[8].refEventID != approvalEvent.id {
		t.Fatalf("аудиторский след: ref_event_id user_decision = %v, хотим id command_approval_request = %v",
			events[8].refEventID, approvalEvent.id)
	}

	row := getTaskRow(ctx, t, pool, taskID)
	if row.status != string(task.StatusRunning) {
		t.Fatalf("итоговый tasks.status в БД = %s, хотим running", row.status)
	}
}

// TestIntegration_RecordEvent_HappyPath — приёмка тикета 8.5 (FR E6):
// RecordEvent атомарно добавляет запись task_events БЕЗ смены статуса задачи
// — в отличие от Transition/TransitionWithEvent. Проверяем: возвращённый seq
// продолжает уже существующую последовательность событий, tasks.status в БД
// НЕ меняется, а сама запись появляется в task_events с ожидаемыми
// type/ref_event_id/payload.
func TestIntegration_RecordEvent_HappyPath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, cleanup := setupPool(ctx, t)
	defer cleanup()

	q := db.New(pool)
	taskID := seedTask(ctx, t, pool, q, "record-event-owner")

	tr := task.NewTransitioner(pool)

	// Довести задачу до running обычным Transition (seq 1, 2: status_change).
	for _, trigger := range []task.Trigger{task.TriggerEnqueued, task.TriggerTaskAccepted} {
		if _, _, err := tr.Transition(ctx, taskID, trigger); err != nil {
			t.Fatalf("подготовка (%s): %v", trigger, err)
		}
	}

	before := getTaskRow(ctx, t, pool, taskID)
	if before.status != string(task.StatusRunning) {
		t.Fatalf("подготовка: статус = %s, хотим running", before.status)
	}

	progressPayload := []byte(`{"text":"критическая операция доводится до безопасного завершения"}`)
	seq, err := tr.RecordEvent(ctx, taskID, "agent_progress", pgtype.UUID{}, progressPayload)
	if err != nil {
		t.Fatalf("RecordEvent: %v", err)
	}
	if seq != 3 {
		t.Fatalf("RecordEvent: seq = %d, хотим 3 (продолжение seq 1,2 от status_change)", seq)
	}

	after := getTaskRow(ctx, t, pool, taskID)
	if after.status != string(task.StatusRunning) {
		t.Fatalf("RecordEvent не должен менять tasks.status: было running, стало %s", after.status)
	}

	events := listTaskEventsFull(ctx, t, pool, taskID)
	if len(events) != 3 {
		t.Fatalf("ожидалось 3 события task_events после RecordEvent, получено %d", len(events))
	}
	last := events[2]
	if last.seq != 3 || last.evtType != "agent_progress" {
		t.Fatalf("событие 3 = (seq=%d, type=%s), хотим (3, agent_progress)", last.seq, last.evtType)
	}
	if last.refEventID.Valid {
		t.Fatalf("ref_event_id agent_progress должен быть NULL, получено %v", last.refEventID)
	}

	// Второй вызов RecordEvent должен продолжить seq дальше (4), доказывая,
	// что он корректно взаимодействует с FOR UPDATE-локом при повторных
	// вызовах.
	seq2, err := tr.RecordEvent(ctx, taskID, "agent_progress", pgtype.UUID{}, progressPayload)
	if err != nil {
		t.Fatalf("RecordEvent (второй вызов): %v", err)
	}
	if seq2 != 4 {
		t.Fatalf("RecordEvent (второй вызов): seq = %d, хотим 4", seq2)
	}
}

// TestIntegration_RecordEvent_TaskNotFound — RecordEvent с несуществующим
// taskID должен вернуть ошибку (не панику) — тот же принцип, что и
// GetTaskStatusForUpdate в Transition (см. TestIntegration_Transition_*).
func TestIntegration_RecordEvent_TaskNotFound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, cleanup := setupPool(ctx, t)
	defer cleanup()

	tr := task.NewTransitioner(pool)

	var unknownTaskID pgtype.UUID
	if err := unknownTaskID.Scan("00000000-0000-0000-0000-000000000000"); err != nil {
		t.Fatalf("Scan unknownTaskID: %v", err)
	}

	if _, err := tr.RecordEvent(ctx, unknownTaskID, "agent_progress", pgtype.UUID{}, []byte(`{"text":"..."}`)); err == nil {
		t.Fatal("RecordEvent с несуществующим taskID должен вернуть ошибку")
	}
}
