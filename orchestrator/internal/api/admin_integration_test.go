//go:build integration

// Integration-тесты GET /admin/registration-token на РЕАЛЬНОМ Postgres через
// testcontainers-go (тикет 1.6, FR A2). Тег integration — как у
// register_integration_test.go (setupDB/testJWTSigningKey переиспользуются
// оттуда, тот же файл пакета api_test).
//
// Покрываем приёмочные сценарии тикета 1.6:
//   - администратор (is_admin=true) видит действующий токен регистрации
//     (FR A2, Gherkin §1 «Администратор видит токен регистрации») → 200;
//   - обычный (не-админ) пользователь → 403;
//   - без access-токена → 401 (auth-middleware, тикет 1.4).
//
// Access-токены здесь выпускаются напрямую через auth.IssueAccessToken, не
// через /auth/login — обработчику и middleware важен только валидный JWT с
// верным userID, а не способ его получения; так тест не зависит от тикета
// 1.3 сверх необходимого.
package api_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/yarabey/agentify/internal/auth"
	"github.com/yarabey/agentify/orchestrator/internal/api"
	"github.com/yarabey/agentify/orchestrator/internal/db"
)

// getAdminRegistrationToken шлёт GET /admin/registration-token через httptest
// поверх router с заданным значением заголовка Authorization (пустая строка
// — заголовок не выставляется вовсе).
func getAdminRegistrationToken(t *testing.T, router http.Handler, authorizationHeader string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/admin/registration-token", nil)
	if authorizationHeader != "" {
		req.Header.Set("Authorization", authorizationHeader)
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

// TestIntegration_AdminRegistrationToken_AdminSeesToken — приёмочный сценарий
// «администратор видит токен» (FR A2, Gherkin §1): аккаунт с is_admin=true,
// предъявивший валидный access-токен, получает 200 с действующим значением
// токена регистрации, реально лежащим в БД.
func TestIntegration_AdminRegistrationToken_AdminSeesToken(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupDB(ctx, t)
	defer done()
	q := db.New(pool)

	admin, err := q.CreateUser(ctx, db.CreateUserParams{
		Username:     "admin-sees-token",
		PasswordHash: "$argon2id$v=19$m=1,t=1,p=1$c2FsdA$aGFzaA",
		IsAdmin:      true,
	})
	if err != nil {
		t.Fatalf("CreateUser (admin): %v", err)
	}
	if _, err := q.CreateRegistrationToken(ctx, activeToken); err != nil {
		t.Fatalf("CreateRegistrationToken: %v", err)
	}

	accessToken, err := auth.IssueAccessToken(admin.ID.String(), []byte(testJWTSigningKey), time.Now())
	if err != nil {
		t.Fatalf("IssueAccessToken: %v", err)
	}

	router := api.NewRouter(api.NewServer(q, nil, []byte(testJWTSigningKey)))
	rec := getAdminRegistrationToken(t, router, "Bearer "+accessToken)
	if rec.Code != http.StatusOK {
		t.Fatalf("статус = %d (%s), ожидался 200", rec.Code, rec.Body.String())
	}
	var body struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal тела: %v", err)
	}
	if body.Token != activeToken {
		t.Fatalf("token = %q, ожидался %q", body.Token, activeToken)
	}
	t.Logf("OK: администратор видит действующий токен регистрации (200)")
}

// TestIntegration_AdminRegistrationToken_NonAdminForbidden — приёмочный
// сценарий «не-админ → 403» (FR A2): аутентифицированный, но не-админ
// аккаунт не видит токен, несмотря на валидный access-токен и существующий в
// БД активный токен регистрации.
func TestIntegration_AdminRegistrationToken_NonAdminForbidden(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupDB(ctx, t)
	defer done()
	q := db.New(pool)

	regular, err := q.CreateUser(ctx, db.CreateUserParams{
		Username:     "regular-user",
		PasswordHash: "$argon2id$v=19$m=1,t=1,p=1$c2FsdA$aGFzaA",
		IsAdmin:      false,
	})
	if err != nil {
		t.Fatalf("CreateUser (regular): %v", err)
	}
	if _, err := q.CreateRegistrationToken(ctx, activeToken); err != nil {
		t.Fatalf("CreateRegistrationToken: %v", err)
	}

	accessToken, err := auth.IssueAccessToken(regular.ID.String(), []byte(testJWTSigningKey), time.Now())
	if err != nil {
		t.Fatalf("IssueAccessToken: %v", err)
	}

	router := api.NewRouter(api.NewServer(q, nil, []byte(testJWTSigningKey)))
	rec := getAdminRegistrationToken(t, router, "Bearer "+accessToken)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("статус = %d (%s), ожидался 403", rec.Code, rec.Body.String())
	}
	t.Logf("OK: не-администратор получает 403, токен не раскрыт")
}

// TestIntegration_AdminRegistrationToken_RequiresBearerToken — приёмочный
// сценарий тикета 1.4 применительно к этому маршруту: без
// Authorization-заголовка — 401 от auth-middleware, не доходя до обработчика
// (и, соответственно, без обращения к БД за is_admin).
func TestIntegration_AdminRegistrationToken_RequiresBearerToken(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupDB(ctx, t)
	defer done()
	q := db.New(pool)

	if _, err := q.CreateRegistrationToken(ctx, activeToken); err != nil {
		t.Fatalf("CreateRegistrationToken: %v", err)
	}

	router := api.NewRouter(api.NewServer(q, nil, []byte(testJWTSigningKey)))
	rec := getAdminRegistrationToken(t, router, "")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("статус = %d (%s), ожидался 401", rec.Code, rec.Body.String())
	}
	t.Logf("OK: без Authorization-заголовка — 401, без доступа к токену")
}
