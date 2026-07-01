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
