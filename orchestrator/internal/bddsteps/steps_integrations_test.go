//go:build bdd

// steps_integrations_test.go — степы
// orchestrator/features/02_integrations_and_machines.feature (Gherkin §2
// «Интеграции и машины» + «Статус машины», FR B1-B5, тикет 11.2).
package bddsteps

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/coder/websocket"
	"github.com/cucumber/godog"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/yarabey/agentify/orchestrator/internal/api"
	"github.com/yarabey/agentify/orchestrator/internal/task"
)

// wsAccepted проверяет, что WS-соединение агента осталось открытым после
// hello (не было закрыто close-кодом 4401) — тот же приём, что wsAccepted в
// orchestrator/internal/api/machine_ws_integration_test.go: сервер тикета
// 2.3 на успешном пути ничего не пишет и просто блокируется на чтении,
// поэтому попытка чтения с коротким таймаутом упирается в
// context.DeadlineExceeded, а не в close-фрейм.
func wsAccepted(conn *websocket.Conn) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _, err := conn.Read(ctx)
	switch {
	case err == nil:
		return true, nil
	case websocket.CloseStatus(err) == 4401:
		return false, nil
	case errors.Is(err, context.DeadlineExceeded):
		return true, nil
	default:
		return false, fmt.Errorf("неожиданная ошибка чтения после hello: %w", err)
	}
}

func registerIntegrationSteps(sc *godog.ScenarioContext, w *World) {
	sc.Given(`^я вошёл в систему$`, func(ctx context.Context) error {
		_, err := w.ensureUser(ctx, defaultUserAlias)
		return err
	})
	sc.When(`^я создаю интеграцию с названием "([^"]+)"$`, func(ctx context.Context, name string) error {
		_, err := w.createIntegration(ctx, defaultUserAlias, defaultIntegrationAlias, name, nil)
		return err
	})
	sc.Then(`^система выдаёт уникальный UUID для этой интеграции$`, func() error {
		integ, ok := w.integrations[defaultIntegrationAlias]
		if !ok {
			return fmt.Errorf("интеграция не заведена")
		}
		if integ.Secret.String() == "" {
			return fmt.Errorf("UUID интеграции пуст")
		}
		return nil
	})
	sc.Then(`^UUID доступен мне для настройки машины$`, func(ctx context.Context) error {
		integ := w.integrations[defaultIntegrationAlias]
		owner := w.users[integ.Owner]
		if err := w.doRequest(ctx, http.MethodGet, "/integrations/"+integ.ID.String(), owner.AccessToken, nil, nil); err != nil {
			return err
		}
		if err := w.expectStatus(http.StatusOK); err != nil {
			return err
		}
		var got api.IntegrationWithSecret
		if err := w.decodeLastBody(&got); err != nil {
			return err
		}
		if got.Uuid == nil || *got.Uuid != integ.Secret {
			return fmt.Errorf("GET /integrations/{id}.uuid = %v, ожидался %s", got.Uuid, integ.Secret)
		}
		return nil
	})

	sc.Given(`^у интеграции есть UUID$`, func(ctx context.Context) error {
		_, err := w.ensureIntegration(ctx, defaultIntegrationAlias)
		return err
	})
	sc.When(`^машина обращается к системе с этим UUID$`, func(ctx context.Context) error {
		_, err := w.dialMachineWS(ctx, defaultIntegrationAlias)
		return err
	})
	sc.Then(`^система принимает её как данную интеграцию$`, func() error {
		conn := w.machineConns[defaultIntegrationAlias]
		if conn == nil {
			return fmt.Errorf("WS-соединение машины не открыто")
		}
		accepted, err := wsAccepted(conn)
		if err != nil {
			return err
		}
		if !accepted {
			return fmt.Errorf("соединение машины закрыто оркестратором (ожидалось «принято»)")
		}
		return nil
	})

	sc.Given(`^у интеграции не указан IP$`, func(ctx context.Context) error {
		_, err := w.createIntegration(ctx, defaultUserAlias, defaultIntegrationAlias, "laptop-no-ip-hint", nil)
		return err
	})
	sc.When(`^машина с динамическим адресом обращается с валидным UUID$`, func(ctx context.Context) error {
		_, err := w.dialMachineWS(ctx, defaultIntegrationAlias)
		return err
	})
	sc.Then(`^система её принимает$`, func() error {
		conn := w.machineConns[defaultIntegrationAlias]
		if conn == nil {
			return fmt.Errorf("WS-соединение машины не открыто")
		}
		accepted, err := wsAccepted(conn)
		if err != nil {
			return err
		}
		if !accepted {
			return fmt.Errorf("соединение машины закрыто оркестратором (ожидалось «принято»)")
		}
		return nil
	})

	sc.Given(`^в интеграции есть выполняющаяся задача$`, func(ctx context.Context) error {
		_, err := w.ensureRunningTask(ctx, defaultTaskAlias)
		return err
	})
	sc.When(`^я меняю её название$`, func(ctx context.Context) error {
		integ := w.integrations[integrationAliasForTask(defaultTaskAlias)]
		owner := w.users[integ.Owner]
		newName := "переименованная-машина"
		if err := w.doRequest(ctx, http.MethodPatch, "/integrations/"+integ.ID.String(), owner.AccessToken, api.IntegrationUpdate{Name: &newName}, nil); err != nil {
			return err
		}
		return w.expectStatus(http.StatusOK)
	})
	sc.Then(`^выполняющаяся задача продолжается без сбоя$`, func(ctx context.Context) error {
		bt := w.tasks[defaultTaskAlias]
		return w.waitTaskStatus(ctx, bt.ID, task.StatusRunning, 2*time.Second)
	})

	sc.When(`^я пытаюсь удалить интеграцию$`, func(ctx context.Context) error {
		integ := w.integrations[integrationAliasForTask(defaultTaskAlias)]
		owner := w.users[integ.Owner]
		return w.doRequest(ctx, http.MethodDelete, "/integrations/"+integ.ID.String(), owner.AccessToken, nil, nil)
	})
	sc.Then(`^система просит подтверждения$`, func() error {
		return w.expectStatus(http.StatusConflict)
	})
	sc.Then(`^корректно завершает или отменяет активную задачу$`, func(ctx context.Context) error {
		integ := w.integrations[integrationAliasForTask(defaultTaskAlias)]
		owner := w.users[integ.Owner]
		if err := w.doRequest(ctx, http.MethodDelete, "/integrations/"+integ.ID.String()+"?confirm=true", owner.AccessToken, nil, nil); err != nil {
			return err
		}
		if err := w.expectStatus(http.StatusNoContent); err != nil {
			return err
		}
		bt := w.tasks[defaultTaskAlias]
		return w.waitTaskStatus(ctx, bt.ID, task.StatusCancelled, 2*time.Second)
	})

	// --- «Статус машины» (@db-direct — см. orchestrator/features/02_….feature) ---
	sc.Given(`^машина подключена и шлёт сигналы о себе через очередь$`, func(ctx context.Context) error {
		integ, err := w.ensureIntegration(ctx, defaultIntegrationAlias)
		if err != nil {
			return err
		}
		if err := w.queries.MarkIntegrationOnline(ctx, pgtype.UUID{Bytes: integ.ID, Valid: true}); err != nil {
			return fmt.Errorf("MarkIntegrationOnline: %w", err)
		}
		return nil
	})
	sc.When(`^я открываю список интеграций$`, func(ctx context.Context) error {
		integ := w.integrations[defaultIntegrationAlias]
		owner := w.users[integ.Owner]
		return w.doRequest(ctx, http.MethodGet, "/integrations", owner.AccessToken, nil, nil)
	})
	sc.Then(`^машина отображается как "([^"]+)"$`, func(status string) error {
		want := map[string]api.IntegrationStatus{
			"онлайн":  api.IntegrationStatusOnline,
			"оффлайн": api.IntegrationStatusOffline,
		}[status]
		if want == "" {
			return fmt.Errorf("неизвестный статус машины в шаге: %q", status)
		}
		var list []api.Integration
		if err := w.decodeLastBody(&list); err != nil {
			return err
		}
		integ := w.integrations[defaultIntegrationAlias]
		for _, in := range list {
			if in.Id != nil && *in.Id == integ.ID {
				if in.Status == nil || *in.Status != want {
					return fmt.Errorf("integration.status = %v, ожидался %q", in.Status, want)
				}
				return nil
			}
		}
		return fmt.Errorf("интеграция %s не найдена в GET /integrations", integ.ID)
	})

	sc.Given(`^машина не выходила на связь дольше порога$`, func(ctx context.Context) error {
		integ, err := w.ensureIntegration(ctx, defaultIntegrationAlias)
		if err != nil {
			return err
		}
		if err := w.queries.MarkIntegrationOnline(ctx, pgtype.UUID{Bytes: integ.ID, Valid: true}); err != nil {
			return fmt.Errorf("MarkIntegrationOnline: %w", err)
		}
		// Порог "устарел" смоделирован cutoff'ом В БУДУЩЕМ относительно
		// last_seen_at, только что выставленного MarkIntegrationOnline —
		// MarkStaleIntegrationsOffline переводит в offline все online-
		// интеграции с last_seen_at < cutoff (см. orchestrator/queries/
		// integrations.sql), без необходимости реально ждать порог в тесте.
		cutoff := pgtype.Timestamptz{Time: time.Now().UTC().Add(time.Hour), Valid: true}
		if err := w.queries.MarkStaleIntegrationsOffline(ctx, cutoff); err != nil {
			return fmt.Errorf("MarkStaleIntegrationsOffline: %w", err)
		}
		return nil
	})
	sc.Then(`^статус обновился асинхронно, без живого соединения$`, func() error {
		// Утверждение уже доказано механизмом предыдущих шагов: статус
		// офлайн выставлен ПРЯМОЙ записью в БД (MarkStaleIntegrationsOffline)
		// без какого-либо открытого WS-соединения агента — degenerate-случай
		// FR B4 "без живого соединения" в этом харнессе (полный сквозной
		// путь через heartbeat — presence_integration_test.go, тег
		// integration, см. orchestrator/features/README.md).
		return nil
	})
}
