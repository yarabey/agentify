//go:build bdd

// steps_auth_test.go — степы orchestrator/features/01_registration_access.feature
// (Gherkin §1 «Регистрация и доступ», FR A1, A2, A4, I1, I3, тикет 11.2).
package bddsteps

import (
	"context"
	"fmt"
	"net/http"

	"github.com/cucumber/godog"

	"github.com/yarabey/agentify/orchestrator/internal/api"
	"github.com/yarabey/agentify/orchestrator/internal/bootstrap"
)

// bddRegistrationToken — значение токена регистрации, заводимого степами
// этого файла напрямую в БД (тот же приём, что и активный токен в
// orchestrator/internal/api/register_integration_test.go).
const bddRegistrationToken = "bdd-registration-token"

// bddAdminUsername/bddAdminPassword — креды администратора, заводимого
// через bootstrap.Run (тот же путь, что и в проде, orchestrator/main.go) для
// сценария «Администратор видит токен регистрации».
const (
	bddAdminUsername = "bdd-admin"
	bddAdminPassword = "bdd-admin-password"
)

func registerAuthSteps(sc *godog.ScenarioContext, w *World) {
	sc.Given(`^у меня есть валидный токен регистрации$`, func(ctx context.Context) error {
		return w.seedRegistrationToken(ctx, bddRegistrationToken)
	})
	sc.When(`^я регистрируюсь, указав этот токен и учётные данные$`, func(ctx context.Context) error {
		return w.registerViaAPI(ctx, "alice-registered", "correct horse battery staple", w.regToken)
	})
	sc.Then(`^аккаунт создаётся$`, func() error {
		return w.expectStatus(http.StatusCreated)
	})
	sc.Then(`^я могу войти в систему$`, func(ctx context.Context) error {
		if err := w.doRequest(ctx, http.MethodPost, "/auth/login", "", api.LoginRequest{
			Username: "alice-registered",
			Password: "correct horse battery staple",
		}, nil); err != nil {
			return err
		}
		if err := w.expectStatus(http.StatusOK); err != nil {
			return err
		}
		var pair api.TokenPair
		return w.decodeLastBody(&pair)
	})

	sc.Given(`^у меня нет токена регистрации$`, func() error {
		// Токен намеренно НЕ заводится в БД — GetActiveRegistrationToken в
		// PostAuthRegister вернёт pgx.ErrNoRows → 403 (см. server.go).
		return nil
	})
	sc.When(`^я пытаюсь зарегистрироваться$`, func(ctx context.Context) error {
		return w.registerViaAPI(ctx, "bob-no-token", "correct horse battery staple", "любой-неверный-токен")
	})
	sc.Then(`^система отклоняет регистрацию$`, func() error {
		if w.lastStatus < http.StatusBadRequest {
			return fmt.Errorf("статус ответа = %d, ожидался отказ (>=400): тело = %s", w.lastStatus, string(w.lastBody))
		}
		return nil
	})

	sc.Given(`^я вошёл как администратор$`, func(ctx context.Context) error {
		// bootstrap.Run — ТОТ ЖЕ путь заведения администратора, что и в
		// проде (orchestrator/main.go, тикет 1.7): создаёт и администратора,
		// и активный токен регистрации атомарно.
		if err := bootstrap.Run(ctx, w.queries, nil, bootstrap.Config{
			AdminUsername:            bddAdminUsername,
			AdminPassword:            bddAdminPassword,
			InitialRegistrationToken: bddRegistrationToken,
		}); err != nil {
			return fmt.Errorf("bootstrap.Run: %w", err)
		}
		w.regToken = bddRegistrationToken
		admin, err := w.queries.GetUserByUsername(ctx, bddAdminUsername)
		if err != nil {
			return fmt.Errorf("GetUserByUsername(%s): %w", bddAdminUsername, err)
		}
		_, err = w.bindExistingUser(defaultUserAlias, admin, bddAdminPassword)
		return err
	})
	sc.When(`^я открываю настройки в веб-интерфейсе$`, func(ctx context.Context) error {
		u, err := w.ensureUser(ctx, defaultUserAlias)
		if err != nil {
			return err
		}
		return w.doRequest(ctx, http.MethodGet, "/admin/registration-token", u.AccessToken, nil, nil)
	})
	sc.Then(`^я вижу действующий секретный токен регистрации$`, func() error {
		if w.lastStatus != http.StatusOK {
			return fmt.Errorf("GET /admin/registration-token: статус = %d, тело = %s", w.lastStatus, string(w.lastBody))
		}
		var body struct {
			Token string `json:"token"`
		}
		if err := w.decodeLastBody(&body); err != nil {
			return err
		}
		if body.Token != bddRegistrationToken {
			return fmt.Errorf("token в ответе = %q, ожидался %q", body.Token, bddRegistrationToken)
		}
		return nil
	})

	sc.Given(`^существуют пользователи "([^"]+)" и "([^"]+)" со своими задачами$`, func(ctx context.Context, aliceAlias, bobAlias string) error {
		if _, err := w.createUser(ctx, aliceAlias, "bdd-"+aliceAlias, "correct horse battery staple", false); err != nil {
			return err
		}
		if _, err := w.createUser(ctx, bobAlias, "bdd-"+bobAlias, "correct horse battery staple", false); err != nil {
			return err
		}
		if _, err := w.createIntegration(ctx, aliceAlias, aliceAlias+"/интеграция", aliceAlias+"-машина", nil); err != nil {
			return err
		}
		if _, err := w.createIntegration(ctx, bobAlias, bobAlias+"/интеграция", bobAlias+"-машина", nil); err != nil {
			return err
		}
		if _, err := w.createTask(ctx, aliceAlias+"/задача", aliceAlias+"/интеграция", "задача "+aliceAlias); err != nil {
			return err
		}
		if _, err := w.createTask(ctx, bobAlias+"/задача", bobAlias+"/интеграция", "задача "+bobAlias); err != nil {
			return err
		}
		return nil
	})
	sc.When(`^"([^"]+)" просматривает свои задачи и интеграции$`, func(ctx context.Context, alias string) error {
		u, ok := w.users[alias]
		if !ok {
			return fmt.Errorf("пользователь %q не заведён", alias)
		}
		if err := w.doRequest(ctx, http.MethodGet, "/tasks", u.AccessToken, nil, nil); err != nil {
			return err
		}
		if err := w.decodeLastBody(&w.lastVisibleTasks); err != nil {
			return err
		}
		if err := w.doRequest(ctx, http.MethodGet, "/integrations", u.AccessToken, nil, nil); err != nil {
			return err
		}
		w.lastActorAlias = alias
		return w.decodeLastBody(&w.lastVisibleIntegrations)
	})
	sc.Then(`^она видит только свои данные$`, func(ctx context.Context) error {
		if w.lastActorAlias == "" {
			return fmt.Errorf("нет предыдущего шага «просматривает свои задачи и интеграции»")
		}
		return w.assertVisibleOnly(ctx, w.lastActorAlias)
	})
	sc.Then(`^не видит ничего, принадлежащего "([^"]+)"$`, func(ctx context.Context, other string) error {
		return w.assertVisibleOnlyExcludes(other)
	})

	sc.Given(`^я вошёл и получил токен доступа$`, func(ctx context.Context) error {
		_, err := w.ensureUser(ctx, defaultUserAlias)
		return err
	})
	sc.When(`^я вызываю метод API с этим токеном$`, func(ctx context.Context) error {
		u, err := w.ensureUser(ctx, defaultUserAlias)
		if err != nil {
			return err
		}
		return w.doRequest(ctx, http.MethodPost, "/integrations", u.AccessToken, api.IntegrationCreate{Name: "api-token-check"}, nil)
	})
	sc.Then(`^система определяет меня как владельца токена$`, func() error {
		return w.expectStatus(http.StatusCreated)
	})
	sc.Then(`^выполняет операцию от моего имени$`, func(ctx context.Context) error {
		u, err := w.ensureUser(ctx, defaultUserAlias)
		if err != nil {
			return err
		}
		var created api.IntegrationWithSecret
		if err := w.decodeLastBody(&created); err != nil {
			return err
		}
		if err := w.doRequest(ctx, http.MethodGet, "/integrations/"+created.Id.String(), u.AccessToken, nil, nil); err != nil {
			return err
		}
		return w.expectStatus(http.StatusOK)
	})
}
