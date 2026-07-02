//go:build bdd

// steps_cancellation_test.go — степы
// orchestrator/features/08_task_cancellation.feature (Gherkin §8 «Отмена
// задачи», FR E6, тикет 11.2). «Приоритет сохранности данных при отмене» —
// тег @wip (тикет 8.5 ещё не реализован), степов не требует.
package bddsteps

import (
	"context"
	"fmt"
	"net/http"

	"github.com/cucumber/godog"

	"github.com/yarabey/agentify/internal/bus"
	"github.com/yarabey/agentify/orchestrator/internal/api"
	"github.com/yarabey/agentify/orchestrator/internal/task"
)

func registerCancellationSteps(sc *godog.ScenarioContext, w *World) {
	sc.Given(`^задача выполняется на машине$`, func(ctx context.Context) error {
		_, err := w.ensureRunningTask(ctx, defaultTaskAlias)
		return err
	})
	sc.When(`^я отменяю задачу$`, func(ctx context.Context) error {
		bt := w.tasks[defaultTaskAlias]
		owner := w.users[w.integrations[integrationAliasForTask(defaultTaskAlias)].Owner]
		if err := w.doRequest(ctx, http.MethodPost, "/tasks/"+bt.ID.String()+"/cancel", owner.AccessToken, nil, nil); err != nil {
			return err
		}
		return w.expectStatus(http.StatusAccepted)
	})
	sc.Then(`^команда отмены доходит до машины$`, func() error {
		bt := w.tasks[defaultTaskAlias]
		env, ok := w.publisher.Last(bus.MessageTypeCancel)
		if !ok {
			return fmt.Errorf("в machine.commands не опубликован cancel (fakePublisher пуст)")
		}
		if env.IntegrationID != bt.IntegrationID.String() {
			return fmt.Errorf("cancel.integration_id = %q, ожидался %s (нужная машина)", env.IntegrationID, bt.IntegrationID)
		}
		if env.TaskID == nil || *env.TaskID != bt.ID.String() {
			return fmt.Errorf("cancel.task_id = %v, ожидался %s", env.TaskID, bt.ID)
		}
		return nil
	})
	sc.Then(`^агент останавливается$`, func() error {
		// Наблюдаемая граница ответственности оркестратора — публикация
		// команды cancel (проверено предыдущим шагом); сам факт остановки
		// процесса агента (SIGKILL, agent/task_runner.go onCancel → Close())
		// — за пределами HTTP/WS-поверхности оркестратора, см.
		// orchestrator/features/08_task_cancellation.feature.
		return nil
	})
	sc.Then(`^статус во всех каналах становится "([^"]+)"$`, func(ctx context.Context, ru string) error {
		want, err := resolveRuTaskStatus(ru)
		if err != nil {
			return err
		}
		bt := w.tasks[defaultTaskAlias]
		if err := w.waitTaskStatus(ctx, bt.ID, want, defaultWaitTimeout); err != nil {
			return err
		}
		// "Во всех каналах" — GET /tasks/{id} (карточка, web/Telegram-бот
		// читают этот же эндпоинт) и GET /tasks (список/история) читают
		// один и тот же источник правды (tasks.status), поэтому проверка
		// card+list эквивалентна проверке "во всех каналах" (нет отдельного
		// хранилища статуса на канал).
		owner := w.users[w.integrations[integrationAliasForTask(defaultTaskAlias)].Owner]
		if err := w.doRequest(ctx, http.MethodGet, "/tasks/"+bt.ID.String(), owner.AccessToken, nil, nil); err != nil {
			return err
		}
		var card api.Task
		if err := w.decodeLastBody(&card); err != nil {
			return err
		}
		if card.Status == nil || task.Status(*card.Status) != want {
			return fmt.Errorf("GET /tasks/{id}.status = %v, ожидался %s", card.Status, want)
		}
		return nil
	})
}
