//go:build integration

// Integration-тест StaleWorker на РЕАЛЬНОМ Postgres (тикет 5.7, FR E5,
// protocol.md §6) — тот же паттерн, что и transition_integration_test.go
// (собственный тег integration, тот же пакет task_test, тот же
// startPostgres/setupPool/seedTask helper — переиспользуются напрямую, без
// копирования, т.к. это тот же пакет того же тестового бинаря).
//
// Проверяемый сценарий — приёмка тикета 5.7 (Gherkin §9 «Машина пропала
// надолго»):
//  1. задача в running, integrations.last_seen_at заведомо старше
//     staleThreshold → StaleWorker переводит её в stale и пишет
//     task_events(status_change) с trigger=timeout (проверяется прямым SELECT
//     в БД, а не только возвращаемым значением Transition);
//  2. integrations.last_seen_at обновляется на now() (эмуляция возврата
//     heartbeat, эквивалент MarkIntegrationOnline) → StaleWorker возвращает
//     задачу в running и пишет task_events(status_change) с
//     trigger=machine_recovered.
package task_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/yarabey/agentify/internal/crypto"
	"github.com/yarabey/agentify/orchestrator/internal/db"
	"github.com/yarabey/agentify/orchestrator/internal/task"
)

// staleWorkerTestMasterKey — тестовый мастер-ключ шифрования (ровно 32 байта),
// которым StaleWorker.Transitioner шифрует payload_enc событий status_change
// (FR I1, тикет 11.1); lastStatusChangePayload расшифровывает тем же подключом.
var staleWorkerTestMasterKey = []byte("0123456789abcdef0123456789abcdef")

// setIntegrationLastSeenAt — прямой UPDATE integrations.last_seen_at, минуя
// heartbeat-consumer/MarkIntegrationOnline (эмуляция ухода/возврата машины,
// та же колонка, что и в проде).
func setIntegrationLastSeenAt(ctx context.Context, t *testing.T, pool *pgxpool.Pool, taskID pgtype.UUID, lastSeenAt time.Time) {
	t.Helper()
	tag, err := pool.Exec(ctx, `
		UPDATE integrations
		SET last_seen_at = $1
		WHERE id = (SELECT integration_id FROM tasks WHERE id = $2)`,
		lastSeenAt, taskID)
	if err != nil {
		t.Fatalf("обновить integrations.last_seen_at: %v", err)
	}
	if tag.RowsAffected() != 1 {
		t.Fatalf("обновление integrations.last_seen_at затронуло %d строк, ожидалась 1", tag.RowsAffected())
	}
}

// statusChangeEventPayload — минимальное содержимое payload_enc события
// status_change, нужное этому тесту (см. statusChangePayload в transition.go —
// неэкспортируемый тип, поэтому здесь свой минимальный дубликат полей).
type statusChangeEventPayload struct {
	From    string `json:"from"`
	To      string `json:"to"`
	Trigger string `json:"trigger"`
}

// lastStatusChangePayload возвращает payload_enc САМОГО ПОЗДНЕГО (по seq)
// события status_change задачи, распарсенный в statusChangeEventPayload.
func lastStatusChangePayload(ctx context.Context, t *testing.T, pool *pgxpool.Pool, taskID pgtype.UUID) statusChangeEventPayload {
	t.Helper()
	var raw []byte
	err := pool.QueryRow(ctx, `
		SELECT payload_enc FROM task_events
		WHERE task_id = $1 AND type = 'status_change'
		ORDER BY seq DESC LIMIT 1`, taskID).Scan(&raw)
	if err != nil {
		t.Fatalf("прочитать последний status_change: %v", err)
	}
	// payload_enc зашифрован at-rest (FR I1, тикет 11.1) — расшифровываем тем же
	// подключом, что и Transitioner при записи.
	plaintext, derr := crypto.Decrypt(crypto.DeriveKey(staleWorkerTestMasterKey, task.EventPayloadKeyPurpose), raw)
	if derr != nil {
		t.Fatalf("расшифровать payload_enc status_change: %v", derr)
	}
	var payload statusChangeEventPayload
	if uerr := json.Unmarshal(plaintext, &payload); uerr != nil {
		t.Fatalf("разобрать payload_enc status_change: %v", uerr)
	}
	return payload
}

// countStatusChangeEvents — сколько записей task_events(status_change) есть
// у задачи (используется, чтобы дождаться появления НОВОЙ записи после
// действия воркера, а не проверить содержимое старой).
func countStatusChangeEvents(ctx context.Context, t *testing.T, pool *pgxpool.Pool, taskID pgtype.UUID) int {
	t.Helper()
	var n int
	err := pool.QueryRow(ctx, `
		SELECT count(*) FROM task_events WHERE task_id = $1 AND type = 'status_change'`, taskID).Scan(&n)
	if err != nil {
		t.Fatalf("посчитать status_change: %v", err)
	}
	return n
}

// waitForTaskStatus опрашивает tasks.status до совпадения с want или до
// истечения deadline — StaleWorker работает по тикеру, а не синхронно.
func waitForTaskStatus(ctx context.Context, t *testing.T, pool *pgxpool.Pool, taskID pgtype.UUID, want task.Status, deadline time.Duration) {
	t.Helper()
	end := time.Now().Add(deadline)
	for {
		row := getTaskRow(ctx, t, pool, taskID)
		if row.status == string(want) {
			return
		}
		if time.Now().After(end) {
			t.Fatalf("tasks.status не стал %s за %v (последнее наблюдение: %s)", want, deadline, row.status)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("ctx отменён во время ожидания tasks.status=%s: %v", want, ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// TestIntegration_StaleWorker_StaleThenRecovered — приёмка тикета 5.7:
// пропажа heartbeat дольше порога → stale + task_events(status_change,
// trigger=timeout); возврат heartbeat → running + task_events(status_change,
// trigger=machine_recovered).
func TestIntegration_StaleWorker_StaleThenRecovered(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, cleanup := setupPool(ctx, t)
	defer cleanup()

	q := db.New(pool)
	taskID := seedTask(ctx, t, pool, q, "stale-worker-owner")

	tr := task.NewTransitioner(pool, task.WithMasterKey(staleWorkerTestMasterKey))
	// Довести задачу до running (стартовый статус created).
	for _, trigger := range []task.Trigger{task.TriggerEnqueued, task.TriggerTaskAccepted} {
		if _, _, err := tr.Transition(ctx, taskID, trigger); err != nil {
			t.Fatalf("подготовка (%s): %v", trigger, err)
		}
	}
	row := getTaskRow(ctx, t, pool, taskID)
	if row.status != string(task.StatusRunning) {
		t.Fatalf("подготовка: tasks.status = %s, хотим running", row.status)
	}
	eventsBeforeStale := countStatusChangeEvents(ctx, t, pool, taskID)

	// Машина "пропала" задолго до порога — last_seen_at в далёком прошлом.
	setIntegrationLastSeenAt(ctx, t, pool, taskID, time.Now().UTC().Add(-5*time.Minute))

	const (
		staleThreshold = 50 * time.Millisecond
		pollInterval   = 20 * time.Millisecond
	)
	w, err := task.NewStaleWorker(tr, db.New(pool),
		task.WithStaleThreshold(staleThreshold), task.WithStalePollInterval(pollInterval))
	if err != nil {
		t.Fatalf("NewStaleWorker: %v", err)
	}

	runCtx, runCancel := context.WithCancel(ctx)
	runDone := make(chan struct{})
	go func() {
		_ = w.Run(runCtx)
		close(runDone)
	}()

	// --- Часть (а): зависание ---
	waitForTaskStatus(ctx, t, pool, taskID, task.StatusStale, 10*time.Second)

	afterStale := getTaskRow(ctx, t, pool, taskID)
	if afterStale.status != string(task.StatusStale) {
		t.Fatalf("tasks.status после зависания = %s, хотим stale", afterStale.status)
	}
	if got := countStatusChangeEvents(ctx, t, pool, taskID); got != eventsBeforeStale+1 {
		t.Fatalf("после зависания ожидалась 1 новая запись status_change, было %d, стало %d", eventsBeforeStale, got)
	}
	stalePayload := lastStatusChangePayload(ctx, t, pool, taskID)
	if stalePayload.Trigger != string(task.TriggerTimeout) {
		t.Fatalf("payload status_change после зависания: trigger=%s, хотим %s", stalePayload.Trigger, task.TriggerTimeout)
	}
	if stalePayload.From != string(task.StatusRunning) || stalePayload.To != string(task.StatusStale) {
		t.Fatalf("payload status_change после зависания: from=%s to=%s, хотим running/stale", stalePayload.From, stalePayload.To)
	}
	eventsAfterStale := countStatusChangeEvents(ctx, t, pool, taskID)

	// --- Часть (б): возврат машины ---
	setIntegrationLastSeenAt(ctx, t, pool, taskID, time.Now().UTC())

	waitForTaskStatus(ctx, t, pool, taskID, task.StatusRunning, 10*time.Second)

	afterRecovered := getTaskRow(ctx, t, pool, taskID)
	if afterRecovered.status != string(task.StatusRunning) {
		t.Fatalf("tasks.status после возврата = %s, хотим running", afterRecovered.status)
	}
	if got := countStatusChangeEvents(ctx, t, pool, taskID); got != eventsAfterStale+1 {
		t.Fatalf("после возврата ожидалась 1 новая запись status_change, было %d, стало %d", eventsAfterStale, got)
	}
	recoveredPayload := lastStatusChangePayload(ctx, t, pool, taskID)
	if recoveredPayload.Trigger != string(task.TriggerMachineRecovered) {
		t.Fatalf("payload status_change после возврата: trigger=%s, хотим %s", recoveredPayload.Trigger, task.TriggerMachineRecovered)
	}
	if recoveredPayload.From != string(task.StatusStale) || recoveredPayload.To != string(task.StatusRunning) {
		t.Fatalf("payload status_change после возврата: from=%s to=%s, хотим stale/running", recoveredPayload.From, recoveredPayload.To)
	}

	runCancel()
	select {
	case <-runDone:
	case <-time.After(10 * time.Second):
		t.Fatal("StaleWorker.Run не завершился после отмены ctx за отведённое время")
	}
}
