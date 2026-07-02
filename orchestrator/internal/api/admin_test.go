// Unit-тесты обработчика GET /admin/registration-token без БД (тикет 1.6,
// FR A2).
//
// Проверяют форму ответов через httptest поверх собранного роутера с
// подменённым слоем данных (fake Querier): админ видит токен (200), не-админ
// получает 403, отсутствие аккаунта по userID из токена доступа — 401.
// Сценарий с реальной БД (через bootstrap + sqlc) — в
// admin_integration_test.go (тег integration).
package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/yarabey/agentify/orchestrator/internal/db"
)

// doAdminRegistrationToken прогоняет GET /admin/registration-token через
// роутер с заданным Querier и Bearer-токеном userID (валидным access-JWT,
// подписанным testJWTSigningKey) и возвращает записанный ответ.
func doAdminRegistrationToken(t *testing.T, q Querier, userID uuid.UUID) *httptest.ResponseRecorder {
	t.Helper()
	router := NewRouter(newTestServer(q))
	req := httptest.NewRequest(http.MethodGet, "/admin/registration-token", nil)
	req.Header.Set("Authorization", "Bearer "+issueTestAccessToken(t, userID, time.Now()))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

// TestAdminRegistrationToken_AdminSeesToken — приёмочный сценарий «админ
// видит токен» (FR A2, Gherkin §1 «Администратор видит токен регистрации»):
// аккаунт с is_admin=true получает 200 с действующим значением токена.
func TestAdminRegistrationToken_AdminSeesToken(t *testing.T) {
	userID := uuid.New()
	q := fakeQuerier{
		getUserByIDUser: db.User{
			ID:      pgtype.UUID{Bytes: userID, Valid: true},
			IsAdmin: true,
		},
		activeToken: "current-registration-secret",
	}

	rec := doAdminRegistrationToken(t, q, userID)
	if rec.Code != http.StatusOK {
		t.Fatalf("статус = %d (%s), ожидался 200", rec.Code, rec.Body.String())
	}

	var body struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal тела: %v", err)
	}
	if body.Token != "current-registration-secret" {
		t.Fatalf("token = %q, ожидался %q", body.Token, "current-registration-secret")
	}
}

// TestAdminRegistrationToken_NonAdminForbidden — приёмочный сценарий
// «не-админ → 403» (FR A2): аутентифицированный, но не-административный
// аккаунт не видит токен.
func TestAdminRegistrationToken_NonAdminForbidden(t *testing.T) {
	userID := uuid.New()
	q := fakeQuerier{
		getUserByIDUser: db.User{
			ID:      pgtype.UUID{Bytes: userID, Valid: true},
			IsAdmin: false,
		},
		activeToken: "current-registration-secret",
	}

	rec := doAdminRegistrationToken(t, q, userID)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("статус = %d (%s), ожидался 403", rec.Code, rec.Body.String())
	}
	assertErrorBody(t, rec)
}

// TestAdminRegistrationToken_RequiresBearerToken — приёмочный сценарий
// тикета 1.4 применительно к этому маршруту: без Authorization-заголовка —
// 401 от auth-middleware, не доходя до обработчика.
func TestAdminRegistrationToken_RequiresBearerToken(t *testing.T) {
	router := NewRouter(newTestServer(fakeQuerier{}))
	req := httptest.NewRequest(http.MethodGet, "/admin/registration-token", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("статус = %d (%s), ожидался 401", rec.Code, rec.Body.String())
	}
	assertErrorBody(t, rec)
}

// TestAdminRegistrationToken_UnknownUserUnauthorized — валидный access-токен,
// но аккаунт, на который он выдан, не найден в БД (удалён) — единый 401, как
// и у остальных ошибок проверки токена доступа.
func TestAdminRegistrationToken_UnknownUserUnauthorized(t *testing.T) {
	q := fakeQuerier{getUserByIDErr: pgx.ErrNoRows}

	rec := doAdminRegistrationToken(t, q, uuid.New())
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("статус = %d (%s), ожидался 401", rec.Code, rec.Body.String())
	}
	assertErrorBody(t, rec)
}

// TestAdminRegistrationToken_NoActiveTokenIsServerError — админ-аккаунт есть,
// но активного токена регистрации в БД нет (инвариант-сбой — bootstrap,
// тикет 1.7, обязан был его создать) — 500, не штатный пользовательский код
// ответа.
func TestAdminRegistrationToken_NoActiveTokenIsServerError(t *testing.T) {
	userID := uuid.New()
	q := fakeQuerier{
		getUserByIDUser: db.User{
			ID:      pgtype.UUID{Bytes: userID, Valid: true},
			IsAdmin: true,
		},
		activeErr: pgx.ErrNoRows,
	}

	rec := doAdminRegistrationToken(t, q, userID)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("статус = %d (%s), ожидался 500", rec.Code, rec.Body.String())
	}
}
