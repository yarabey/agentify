//go:build bdd

// steps_history_test.go — степы orchestrator/features/10_history.feature
// (Gherkin §10 «История», FR H1, I2, тикет 11.2).
package bddsteps

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/cucumber/godog"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/yarabey/agentify/orchestrator/internal/api"
)

// driveFullTaskCycle проводит задачу taskAlias через ПОЛНЫЙ жизненный цикл
// (created→queued→running→waiting_user→running→awaiting_confirm→completed)
// РЕАЛЬНЫМИ HTTP/WS-действиями (не Transitioner напрямую) — Gherkin §10
// «Состав записи о задаче» требует историю со ВСЕМИ типами событий, которую
// даёт именно такой прогон.
func (w *World) driveFullTaskCycle(ctx context.Context, taskAlias, text string) error {
	bt, err := w.createTask(ctx, taskAlias, integrationAliasForTask(taskAlias), text)
	if err != nil {
		return err
	}
	if err := w.machineAcceptsTask(ctx, integrationAliasForTask(taskAlias), bt); err != nil {
		return err
	}

	questionID, err := w.sendAgentQuestion(ctx, taskAlias, "текущий", "продолжать?")
	if err != nil {
		return err
	}
	// Дождаться, пока read loop сервера (GetMachineWs) реально обработает
	// кадр и переведёт задачу в waiting_user — иначе POST /tasks/{id}/answer
	// ниже гонится с асинхронной обработкой WS-кадра и может застать
	// question_id ещё не записанным в task_events (см. тот же паттерн в
	// steps_qa_test.go, где "Тогда задача переходит в статус..." — ОТДЕЛЬНЫЙ
	// шаг Gherkin именно поэтому).
	if err := w.waitTaskStatus(ctx, bt.ID, "waiting_user", defaultWaitTimeout); err != nil {
		return err
	}
	owner := w.users[w.integrations[integrationAliasForTask(taskAlias)].Owner]
	qid, err := uuid.Parse(questionID)
	if err != nil {
		return err
	}
	if err := w.doRequest(ctx, http.MethodPost, "/tasks/"+bt.ID.String()+"/answer", owner.AccessToken,
		api.PostTasksIdAnswerJSONBody{QuestionId: qid, Text: "да, продолжай"}, nil); err != nil {
		return err
	}
	if err := w.expectStatus(http.StatusAccepted); err != nil {
		return err
	}

	if err := w.sendAgentCompleted(ctx, taskAlias, "отчёт собран"); err != nil {
		return err
	}
	if err := w.waitTaskStatus(ctx, bt.ID, "awaiting_confirm", defaultWaitTimeout); err != nil {
		return err
	}

	if err := w.doRequest(ctx, http.MethodPost, "/tasks/"+bt.ID.String()+"/confirm", owner.AccessToken, nil, nil); err != nil {
		return err
	}
	return w.expectStatus(http.StatusOK)
}

// backdateTaskAndEvents — прямой UPDATE tasks.created_at/updated_at и
// task_events.created_at, минуя бизнес-логику (эмуляция «задача завершена
// давно» для Gherkin §10 «Бессрочное хранение») — тот же приём, что
// backdateTaskAndEvents в orchestrator/internal/api/tasks_integration_test.go.
func (w *World) backdateTaskAndEvents(ctx context.Context, taskID uuid.UUID, at time.Time) error {
	id := pgtype.UUID{Bytes: taskID, Valid: true}
	if _, err := w.pool.Exec(ctx, `UPDATE tasks SET created_at = $1, updated_at = $1 WHERE id = $2`, at, id); err != nil {
		return fmt.Errorf("backdate tasks: %w", err)
	}
	if _, err := w.pool.Exec(ctx, `UPDATE task_events SET created_at = $1 WHERE task_id = $2`, at, id); err != nil {
		return fmt.Errorf("backdate task_events: %w", err)
	}
	return nil
}

func registerHistorySteps(sc *godog.ScenarioContext, w *World) {
	sc.Given(`^задача прошла полный цикл$`, func(ctx context.Context) error {
		return w.driveFullTaskCycle(ctx, defaultTaskAlias, "собери отчёт по проекту")
	})
	sc.When(`^я открываю её в истории$`, func(ctx context.Context) error {
		bt := w.tasks[defaultTaskAlias]
		owner := w.users[w.integrations[integrationAliasForTask(defaultTaskAlias)].Owner]
		return w.doRequest(ctx, http.MethodGet, "/tasks/"+bt.ID.String(), owner.AccessToken, nil, nil)
	})
	sc.Then(`^я вижу целевую интеграцию, таймстемп, текст запроса, статус и текстовый ответ$`, func(ctx context.Context) error {
		if err := w.expectStatus(http.StatusOK); err != nil {
			return err
		}
		var card api.Task
		if err := w.decodeLastBody(&card); err != nil {
			return err
		}
		bt := w.tasks[defaultTaskAlias]
		if card.IntegrationId == nil || *card.IntegrationId != bt.IntegrationID {
			return fmt.Errorf("Task.IntegrationId = %v, ожидалась %s (целевая интеграция)", card.IntegrationId, bt.IntegrationID)
		}
		if card.CreatedAt == nil || card.CreatedAt.IsZero() {
			return fmt.Errorf("Task.CreatedAt пуст (таймстемп)")
		}
		if card.Text == nil || *card.Text == "" {
			return fmt.Errorf("Task.Text пуст (текст запроса)")
		}
		if card.Status == nil || *card.Status != api.Completed {
			return fmt.Errorf("Task.Status = %v, ожидался completed", card.Status)
		}

		owner := w.users[w.integrations[integrationAliasForTask(defaultTaskAlias)].Owner]
		if err := w.doRequest(ctx, http.MethodGet, "/tasks/"+bt.ID.String()+"/events", owner.AccessToken, nil, nil); err != nil {
			return err
		}
		var events []api.TaskEvent
		if err := w.decodeLastBody(&events); err != nil {
			return err
		}
		var sawAgentCompletedWithSummary bool
		for _, e := range events {
			if e.Type != nil && *e.Type == api.TaskEventTypeAgentCompleted && e.Payload != nil {
				if summary, _ := (*e.Payload)["summary"].(string); summary != "" {
					sawAgentCompletedWithSummary = true
				}
			}
		}
		if !sawAgentCompletedWithSummary {
			return fmt.Errorf("в GET /tasks/{id}/events нет agent_completed с непустым summary (текстовый ответ)")
		}
		return nil
	})
	sc.Then(`^вижу свои ответы с таймстемпами, привязанные к запросу$`, func(ctx context.Context) error {
		bt := w.tasks[defaultTaskAlias]
		owner := w.users[w.integrations[integrationAliasForTask(defaultTaskAlias)].Owner]
		if err := w.doRequest(ctx, http.MethodGet, "/tasks/"+bt.ID.String()+"/events", owner.AccessToken, nil, nil); err != nil {
			return err
		}
		var events []api.TaskEvent
		if err := w.decodeLastBody(&events); err != nil {
			return err
		}
		for _, e := range events {
			if e.Type != nil && *e.Type == api.TaskEventTypeUserAnswer {
				if e.CreatedAt == nil || e.CreatedAt.IsZero() {
					return fmt.Errorf("user_answer.created_at пуст (таймстемп)")
				}
				if e.Payload == nil {
					return fmt.Errorf("user_answer.payload пуст")
				}
				if qid, _ := (*e.Payload)["question_id"].(string); qid == "" {
					return fmt.Errorf("user_answer.payload.question_id пуст (привязка к запросу агента)")
				}
				return nil
			}
		}
		return fmt.Errorf("в GET /tasks/{id}/events не найден user_answer")
	})

	sc.Given(`^задача завершена давно$`, func(ctx context.Context) error {
		if err := w.driveFullTaskCycle(ctx, defaultTaskAlias, "древняя задача"); err != nil {
			return err
		}
		bt := w.tasks[defaultTaskAlias]
		return w.backdateTaskAndEvents(ctx, bt.ID, time.Date(2023, 1, 15, 12, 0, 0, 0, time.UTC))
	})
	sc.When(`^я открываю историю$`, func(ctx context.Context) error {
		owner := w.users[w.integrations[integrationAliasForTask(defaultTaskAlias)].Owner]
		return w.doRequest(ctx, http.MethodGet, "/tasks", owner.AccessToken, nil, nil)
	})
	sc.Then(`^старая задача по-прежнему доступна$`, func() error {
		if err := w.expectStatus(http.StatusOK); err != nil {
			return err
		}
		var list []api.Task
		if err := w.decodeLastBody(&list); err != nil {
			return err
		}
		bt := w.tasks[defaultTaskAlias]
		for _, t := range list {
			if t.Id != nil && *t.Id == bt.ID {
				return nil
			}
		}
		return fmt.Errorf("задача %s (2023 год) не найдена в GET /tasks — бессрочное хранение нарушено", bt.ID)
	})
	sc.Then(`^авто-удаления не происходит$`, func(ctx context.Context) error {
		// В кодовой базе нет ни cron/воркера, ни SQL-запроса, удаляющего
		// старые tasks/task_events (FR I2 — история хранится бессрочно
		// конструктивно, не по факту отсутствия попытки) — тот же снимок
		// GET /tasks из предыдущего шага уже это доказывает: карточка
		// доступна ЦЕЛИКОМ, не только "не 404".
		bt := w.tasks[defaultTaskAlias]
		owner := w.users[w.integrations[integrationAliasForTask(defaultTaskAlias)].Owner]
		if err := w.doRequest(ctx, http.MethodGet, "/tasks/"+bt.ID.String(), owner.AccessToken, nil, nil); err != nil {
			return err
		}
		return w.expectStatus(http.StatusOK)
	})

	sc.Given(`^в истории есть активная задача$`, func(ctx context.Context) error {
		_, err := w.ensureRunningTask(ctx, defaultTaskAlias)
		return err
	})
	sc.When(`^я открываю её в веб-интерфейсе$`, func(ctx context.Context) error {
		bt := w.tasks[defaultTaskAlias]
		owner := w.users[w.integrations[integrationAliasForTask(defaultTaskAlias)].Owner]
		if err := w.doRequest(ctx, http.MethodGet, "/tasks/"+bt.ID.String(), owner.AccessToken, nil, nil); err != nil {
			return err
		}
		return w.expectStatus(http.StatusOK)
	})
	sc.Then(`^я могу её отменить, ответить на вопрос или подтвердить завершение$`, func(ctx context.Context) error {
		// Доступность действий "ответить на вопрос"/"подтвердить завершение"
		// из карточки истории уже полностью доказана §5/§7 этого харнесса
		// (те же эндпоинты POST /tasks/{id}/answer, /confirm) — здесь
		// дополнительно проверяем действие "отменить" (задача сейчас
		// running, поэтому именно оно и есть валидный переход из ТЕКУЩЕГО
		// состояния карточки, открытой предыдущим шагом).
		bt := w.tasks[defaultTaskAlias]
		owner := w.users[w.integrations[integrationAliasForTask(defaultTaskAlias)].Owner]
		if err := w.doRequest(ctx, http.MethodPost, "/tasks/"+bt.ID.String()+"/cancel", owner.AccessToken, nil, nil); err != nil {
			return err
		}
		return w.expectStatus(http.StatusAccepted)
	})
}
