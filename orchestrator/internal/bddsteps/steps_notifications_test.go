//go:build bdd

// steps_notifications_test.go — степы
// orchestrator/features/06_notifications.feature (Gherkin §6 «Уведомления»,
// FR G1, тикет 11.2). «Уведомление в Telegram» — тег @wip, степов не
// требует (см. .feature-файл).
package bddsteps

import (
	"context"
	"fmt"

	"github.com/cucumber/godog"
)

func registerNotificationSteps(sc *godog.ScenarioContext, w *World) {
	sc.Given(`^у меня открыт веб-интерфейс$`, func(ctx context.Context) error {
		if _, err := w.ensureRunningTask(ctx, defaultTaskAlias); err != nil {
			return err
		}
		_, err := w.connectClient(ctx, defaultUserAlias)
		return err
	})
	sc.When(`^агент задаёт вопрос по моей задаче$`, func(ctx context.Context) error {
		_, err := w.sendAgentQuestion(ctx, defaultTaskAlias, "текущий", "нужно ли продолжать?")
		return err
	})
	sc.Then(`^я вижу уведомление в web без перезагрузки страницы$`, func(ctx context.Context) error {
		cs, err := w.connectClient(ctx, defaultUserAlias)
		if err != nil {
			return err
		}
		n, err := cs.waitNotification("agent_question", defaultWaitTimeout)
		if err != nil {
			return err
		}
		bt := w.tasks[defaultTaskAlias]
		if n.TaskID != bt.ID.String() {
			return fmt.Errorf("уведомление пришло по задаче %s, ожидалась %s", n.TaskID, bt.ID)
		}
		return nil
	})
}
