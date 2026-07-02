//go:build bdd

// steps_offline_test.go — степы orchestrator/features/09_async_offline.feature
// (Gherkin §9 «Асинхронность и оффлайн», FR E5, тикет 11.2). Только
// «Машина пропала надолго» реализована здесь — остальные два сценария
// помечены @redpanda (см. .feature-файл и orchestrator/features/README.md).
package bddsteps

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/cucumber/godog"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/yarabey/agentify/orchestrator/internal/api"
	"github.com/yarabey/agentify/orchestrator/internal/task"
)

// bddStaleThreshold/bddStalePollInterval — короткие интервалы StaleWorker
// для этого харнесса (в проде — минуты, см. orchestrator/main.go
// ORCH_STALE_THRESHOLD): тест не должен реально ждать порог эксплуатации,
// достаточно детерминированно управлять integrations.last_seen_at (тот же
// приём, что и в orchestrator/internal/task/stale_worker_integration_test.go).
const (
	bddStaleThreshold    = 100 * time.Millisecond
	bddStalePollInterval = 20 * time.Millisecond
)

// expectStaleNotificationEvent проверяет, что в журнале задачи taskAlias
// есть запись task_events(status_change, trigger=timeout) — сигнал «машина
// пропала» в этом харнессе (task.StaleWorker НЕ шлёт живой push, см. godoc
// в orchestrator/features/09_async_offline.feature и шаг «И я получаю
// уведомление» в steps_qa_test.go).
func (w *World) expectStaleNotificationEvent(ctx context.Context, taskAlias string) error {
	bt := w.tasks[taskAlias]
	owner := w.users[w.integrations[integrationAliasForTask(taskAlias)].Owner]

	// Идёт через РЕАЛЬНЫЙ GET /tasks/{id}/events (не напрямую db.Queries): с
	// тикета 11.1 task_events.payload_enc зашифрован at-rest (FR I1) — только
	// HTTP-хендлер умеет расшифровать его обратно в JSON (см. тот же приём в
	// findAgentQuestionEventID, steps_qa_test.go).
	if err := w.doRequest(ctx, http.MethodGet, "/tasks/"+bt.ID.String()+"/events", owner.AccessToken, nil, nil); err != nil {
		return err
	}
	if err := w.expectStatus(http.StatusOK); err != nil {
		return err
	}
	var events []api.TaskEvent
	if err := w.decodeLastBody(&events); err != nil {
		return err
	}
	for _, e := range events {
		if e.Type == nil || *e.Type != api.TaskEventTypeStatusChange || e.Payload == nil {
			continue
		}
		if trigger, _ := (*e.Payload)["trigger"].(string); trigger == "timeout" {
			return nil
		}
	}
	return fmt.Errorf("не найдена запись task_events(status_change, trigger=timeout) для задачи %s — сигнал «зависла» отсутствует", bt.ID)
}

func registerOfflineSteps(sc *godog.ScenarioContext, w *World) {
	sc.Given(`^задача выполнялась$`, func(ctx context.Context) error {
		bt, err := w.ensureRunningTask(ctx, defaultTaskAlias)
		if err != nil {
			return err
		}
		integ := w.integrations[integrationAliasForTask(defaultTaskAlias)]
		if err := w.queries.MarkIntegrationOnline(ctx, pgtype.UUID{Bytes: integ.ID, Valid: true}); err != nil {
			return fmt.Errorf("MarkIntegrationOnline: %w", err)
		}
		_ = bt
		return nil
	})
	sc.When(`^машина не выходит на связь дольше порога$`, func(ctx context.Context) error {
		integ := w.integrations[integrationAliasForTask(defaultTaskAlias)]
		// Heartbeat "перестал приходить" — last_seen_at заведомо старше
		// bddStaleThreshold (тот же приём, что setIntegrationLastSeenAt в
		// orchestrator/internal/task/stale_worker_integration_test.go).
		if _, err := w.pool.Exec(ctx, `UPDATE integrations SET last_seen_at = $1 WHERE id = $2`,
			time.Now().UTC().Add(-time.Hour), pgtype.UUID{Bytes: integ.ID, Valid: true}); err != nil {
			return fmt.Errorf("backdate integrations.last_seen_at: %w", err)
		}

		sw, err := task.NewStaleWorker(w.transitioner, w.queries,
			task.WithStaleThreshold(bddStaleThreshold),
			task.WithStalePollInterval(bddStalePollInterval))
		if err != nil {
			return fmt.Errorf("NewStaleWorker: %w", err)
		}
		workerCtx, cancel := context.WithCancel(context.Background())
		w.staleWorkerCancel = cancel
		go func() { _ = sw.Run(workerCtx) }()
		return nil
	})
	sc.Then(`^задача помечается как "([^"]+)"$`, func(ctx context.Context, ru string) error {
		want, err := resolveRuTaskStatus(ru)
		if err != nil {
			return err
		}
		bt := w.tasks[defaultTaskAlias]
		return w.waitTaskStatus(ctx, bt.ID, want, defaultWaitTimeout)
	})
	sc.When(`^машина возвращается$`, func(ctx context.Context) error {
		integ := w.integrations[integrationAliasForTask(defaultTaskAlias)]
		if err := w.queries.MarkIntegrationOnline(ctx, pgtype.UUID{Bytes: integ.ID, Valid: true}); err != nil {
			return fmt.Errorf("MarkIntegrationOnline (heartbeat возобновлён): %w", err)
		}
		return nil
	})
	sc.Then(`^выполнение может быть продолжено$`, func(ctx context.Context) error {
		bt := w.tasks[defaultTaskAlias]
		if err := w.waitTaskStatus(ctx, bt.ID, task.StatusRunning, defaultWaitTimeout); err != nil {
			return err
		}
		if w.staleWorkerCancel != nil {
			w.staleWorkerCancel()
			w.staleWorkerCancel = nil
		}
		return nil
	})
}
