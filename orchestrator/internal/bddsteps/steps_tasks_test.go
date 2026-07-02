//go:build bdd

// steps_tasks_test.go — степы orchestrator/features/04_task_creation.feature
// (Gherkin §4 «Постановка задачи», FR E1, E4, E7, тикет 11.2).
package bddsteps

import (
	"context"
	"fmt"
	"net/http"

	"github.com/cucumber/godog"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/yarabey/agentify/internal/bus"
	"github.com/yarabey/agentify/orchestrator/internal/api"
)

func registerTaskSteps(sc *godog.ScenarioContext, w *World) {
	sc.Given(`^я авторизован в канале "([^"]+)"$`, func(ctx context.Context, _channel string) error {
		// "канал" (web/telegram) не влияет на HTTP-контракт оркестратора —
		// см. обоснование в orchestrator/features/04_task_creation.feature.
		_, err := w.ensureUser(ctx, defaultUserAlias)
		return err
	})
	sc.Given(`^у меня есть подключённая интеграция$`, func(ctx context.Context) error {
		integ, err := w.createIntegration(ctx, defaultUserAlias, defaultIntegrationAlias, "bdd-connected-machine", nil)
		if err != nil {
			return err
		}
		if err := w.queries.MarkIntegrationOnline(ctx, pgtype.UUID{Bytes: integ.ID, Valid: true}); err != nil {
			return fmt.Errorf("MarkIntegrationOnline: %w", err)
		}
		return nil
	})
	sc.When(`^я создаю задачу с текстом "([^"]+)" для этой интеграции$`, func(ctx context.Context, text string) error {
		_, err := w.createTask(ctx, defaultTaskAlias, defaultIntegrationAlias, text)
		return err
	})
	sc.Then(`^задача попадает в очередь к нужной машине$`, func() error {
		if err := w.expectStatus(http.StatusCreated); err != nil {
			return err
		}
		bt := w.tasks[defaultTaskAlias]
		env, ok := w.publisher.Last(bus.MessageTypeTaskAssigned)
		if !ok {
			return fmt.Errorf("в machine.commands не опубликован task_assigned (fakePublisher пуст)")
		}
		if env.IntegrationID != bt.IntegrationID.String() {
			return fmt.Errorf("task_assigned.integration_id = %q, ожидался %s (нужная машина)", env.IntegrationID, bt.IntegrationID)
		}
		if env.TaskID == nil || *env.TaskID != bt.ID.String() {
			return fmt.Errorf("task_assigned.task_id = %v, ожидался %s", env.TaskID, bt.ID)
		}
		return nil
	})
	sc.Then(`^появляется в моей истории со статусом "([^"]+)"$`, func(ctx context.Context, ru string) error {
		want, err := resolveRuTaskStatus(ru)
		if err != nil {
			return err
		}
		bt := w.tasks[defaultTaskAlias]
		return w.waitTaskStatus(ctx, bt.ID, want, defaultWaitTimeout)
	})

	sc.Given(`^я только что отправил задачу$`, func(ctx context.Context) error {
		integ, err := w.createIntegration(ctx, defaultUserAlias, defaultIntegrationAlias, "bdd-dedup-machine", nil)
		if err != nil {
			return err
		}
		if err := w.queries.MarkIntegrationOnline(ctx, pgtype.UUID{Bytes: integ.ID, Valid: true}); err != nil {
			return fmt.Errorf("MarkIntegrationOnline: %w", err)
		}
		_, err = w.createTask(ctx, defaultTaskAlias, defaultIntegrationAlias, "исходная задача")
		return err
	})
	sc.When(`^я случайно отправляю ту же задачу повторно$`, func(ctx context.Context) error {
		bt := w.tasks[defaultTaskAlias]
		integ := w.integrations[defaultIntegrationAlias]
		owner := w.users[integ.Owner]
		// Тот же Idempotency-Key, что и у первой отправки (тикет 5.5, FR E7):
		// дедуп срабатывает по (user_id, Idempotency-Key), не по содержимому
		// тела — текст намеренно чуть другой ("случайно отправляю" тот же
		// клик повторно, а не идентичный байт-в-байт повтор).
		return w.doRequest(ctx, http.MethodPost, "/tasks", owner.AccessToken, api.TaskCreate{
			IntegrationId: integ.ID,
			Text:          "исходная задача (повтор двойного клика)",
		}, map[string]string{"Idempotency-Key": bt.IdempotencyKey})
	})
	sc.Then(`^дубликат задачи не создаётся$`, func(ctx context.Context) error {
		if err := w.expectStatus(http.StatusOK); err != nil {
			return fmt.Errorf("повторная постановка: %w (ожидался 200, не 201/409 — см. тикет 5.5)", err)
		}
		bt := w.tasks[defaultTaskAlias]
		var second api.Task
		if err := w.decodeLastBody(&second); err != nil {
			return err
		}
		if second.Id == nil || *second.Id != bt.ID {
			return fmt.Errorf("повторный POST /tasks вернул id=%v, ожидался тот же id %s", second.Id, bt.ID)
		}
		var count int
		err := w.pool.QueryRow(ctx, `SELECT count(*) FROM tasks WHERE user_id = $1 AND idempotency_key = $2`,
			pgtype.UUID{Bytes: w.users[defaultUserAlias].ID, Valid: true}, bt.IdempotencyKey).Scan(&count)
		if err != nil {
			return fmt.Errorf("count(*) FROM tasks: %w", err)
		}
		if count != 1 {
			return fmt.Errorf("в БД %d строк tasks на этот Idempotency-Key, ожидалась 1 (дубль не должен создаваться)", count)
		}
		return nil
	})
}
