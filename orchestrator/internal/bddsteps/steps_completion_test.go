//go:build bdd

// steps_completion_test.go — степы
// orchestrator/features/07_task_completion.feature (Gherkin §7 «Завершение
// задачи», FR E2, тикет 11.2).
package bddsteps

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/cucumber/godog"

	"github.com/yarabey/agentify/internal/bus"
	"github.com/yarabey/agentify/orchestrator/internal/api"
	"github.com/yarabey/agentify/orchestrator/internal/task"
)

// sendAgentCompleted отправляет agent_completed-кадр "агента" по WS-
// соединению задачи taskAlias (running→awaiting_confirm, тикет 8.1).
func (w *World) sendAgentCompleted(ctx context.Context, taskAlias, summary string) error {
	bt := w.tasks[taskAlias]
	payload, err := json.Marshal(bus.AgentCompletedPayload{Summary: summary})
	if err != nil {
		return fmt.Errorf("marshal AgentCompletedPayload: %w", err)
	}
	taskIDStr := bt.ID.String()
	env := bus.Envelope{
		MessageID:       bus.NewMessageID(),
		TaskID:          &taskIDStr,
		IntegrationID:   bt.IntegrationID.String(),
		Type:            bus.MessageTypeAgentCompleted,
		Seq:             1,
		Ts:              time.Now().UTC().Format(time.RFC3339),
		ProtocolVersion: bus.ProtocolVersion,
		Payload:         payload,
	}
	return w.sendMachineFrame(ctx, integrationAliasForTask(taskAlias), env)
}

// ensureAwaitingConfirmTask доводит задачу taskAlias до awaiting_confirm
// (агент запущен и сообщил о завершении) — общая точка входа для сценариев
// «Пользователь подтверждает завершение»/«Пользователь отклоняет результат»,
// которые в Gherkin начинаются сразу с «Дано задача в статусе "ожидает
// подтверждения"», без предшествующего явного «агент сообщает о
// завершении».
func (w *World) ensureAwaitingConfirmTask(ctx context.Context, taskAlias string) error {
	bt, err := w.ensureRunningTask(ctx, taskAlias)
	if err != nil {
		return err
	}
	if err := w.sendAgentCompleted(ctx, taskAlias, "работа выполнена"); err != nil {
		return err
	}
	return w.waitTaskStatus(ctx, bt.ID, task.StatusAwaitingConfirm, defaultWaitTimeout)
}

func registerCompletionSteps(sc *godog.ScenarioContext, w *World) {
	sc.Given(`^агент выполнил работу$`, func(ctx context.Context) error {
		_, err := w.ensureRunningTask(ctx, defaultTaskAlias)
		return err
	})
	sc.When(`^агент сообщает о завершении$`, func(ctx context.Context) error {
		return w.sendAgentCompleted(ctx, defaultTaskAlias, "работа выполнена")
	})
	// «Тогда задача переходит в статус "ожидает подтверждения"» переиспользует
	// генерик-степ, зарегистрированный registerQASteps (тот же паттерн
	// `^задача переходит в статус "([^"]+)"$`, тот же defaultTaskAlias) —
	// повторная регистрация идентичного паттерна не нужна (godog применяет
	// первый совпавший обработчик, а он делает ровно то же самое).
	sc.Then(`^не считается завершённой$`, func(ctx context.Context) error {
		bt := w.tasks[defaultTaskAlias]
		var status string
		if err := w.pool.QueryRow(ctx, `SELECT status FROM tasks WHERE id = $1`, bt.ID).Scan(&status); err != nil {
			return fmt.Errorf("SELECT tasks.status: %w", err)
		}
		if status == string(task.StatusCompleted) {
			return fmt.Errorf("tasks.status = completed сразу после agent_completed — должно требовать явного подтверждения (FR E2)")
		}
		return nil
	})

	sc.Given(`^задача в статусе "ожидает подтверждения"$`, func(ctx context.Context) error {
		return w.ensureAwaitingConfirmTask(ctx, defaultTaskAlias)
	})
	sc.When(`^я явно подтверждаю завершение$`, func(ctx context.Context) error {
		bt := w.tasks[defaultTaskAlias]
		owner := w.users[w.integrations[integrationAliasForTask(defaultTaskAlias)].Owner]
		if err := w.doRequest(ctx, http.MethodPost, "/tasks/"+bt.ID.String()+"/confirm", owner.AccessToken, nil, nil); err != nil {
			return err
		}
		if err := w.expectStatus(http.StatusOK); err != nil {
			return err
		}
		var got api.Task
		if err := w.decodeLastBody(&got); err != nil {
			return err
		}
		if got.Status == nil || *got.Status != api.Completed {
			return fmt.Errorf("POST /tasks/{id}/confirm вернул status=%v, ожидался completed", got.Status)
		}
		return nil
	})

	sc.When(`^я отклоняю результат и прошу доработку$`, func(ctx context.Context) error {
		bt := w.tasks[defaultTaskAlias]
		owner := w.users[w.integrations[integrationAliasForTask(defaultTaskAlias)].Owner]
		comment := "нужно доработать"
		if err := w.doRequest(ctx, http.MethodPost, "/tasks/"+bt.ID.String()+"/reject", owner.AccessToken,
			api.PostTasksIdRejectJSONBody{Comment: &comment}, nil); err != nil {
			return err
		}
		if err := w.expectStatus(http.StatusOK); err != nil {
			return err
		}
		var got api.Task
		if err := w.decodeLastBody(&got); err != nil {
			return err
		}
		if got.Status == nil || *got.Status != api.Running {
			return fmt.Errorf("POST /tasks/{id}/reject вернул status=%v, ожидался running", got.Status)
		}
		return nil
	})
	sc.Then(`^задача возвращается в статус "([^"]+)"$`, func(ctx context.Context, ru string) error {
		want, err := resolveRuTaskStatus(ru)
		if err != nil {
			return err
		}
		bt := w.tasks[defaultTaskAlias]
		return w.waitTaskStatus(ctx, bt.ID, want, defaultWaitTimeout)
	})
}
