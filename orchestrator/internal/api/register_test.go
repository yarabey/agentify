// Unit-тесты обработчика POST /auth/register без БД (тикет 1.2, FR A1).
//
// Проверяют валидацию тела и форму ответов через httptest поверх собранного
// роутера с подменённым слоем данных (fake Querier). Сценарии с реальной БД
// (успех/дубль) — в register_integration_test.go (тег integration).
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/yarabey/agentify/orchestrator/internal/db"
)

// fakeQuerier — подменный слой данных для unit-тестов обработчика.
type fakeQuerier struct {
	activeToken    string
	activeErr      error
	createUserUser db.User
	createUserErr  error
}

func (f fakeQuerier) GetActiveRegistrationToken(context.Context) (db.RegistrationToken, error) {
	if f.activeErr != nil {
		return db.RegistrationToken{}, f.activeErr
	}
	return db.RegistrationToken{Token: f.activeToken, IsActive: true}, nil
}

func (f fakeQuerier) CreateUser(context.Context, db.CreateUserParams) (db.User, error) {
	if f.createUserErr != nil {
		return db.User{}, f.createUserErr
	}
	return f.createUserUser, nil
}

// doRegister прогоняет тело req через роутер с заданным fakeQuerier и возвращает
// записанный ответ.
func doRegister(t *testing.T, q Querier, body any) *httptest.ResponseRecorder {
	t.Helper()
	router := NewRouter(NewServer(q, nil))
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal тела: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/auth/register", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

// TestRegisterValidationRejectsEmptyFields — пустые обязательные поля → 400.
func TestRegisterValidationRejectsEmptyFields(t *testing.T) {
	q := fakeQuerier{activeToken: "secret"}
	cases := map[string]RegisterRequest{
		"пустой username": {Username: "", Password: "p", RegistrationToken: "secret"},
		"пустой password": {Username: "u", Password: "  ", RegistrationToken: "secret"},
		"пустой токен":     {Username: "u", Password: "p", RegistrationToken: ""},
	}
	for name, body := range cases {
		rec := doRegister(t, q, body)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: статус = %d, ожидался 400", name, rec.Code)
		}
	}
}

// TestRegisterRejectsMalformedJSON — невалидный JSON → 400.
func TestRegisterRejectsMalformedJSON(t *testing.T) {
	router := NewRouter(NewServer(fakeQuerier{activeToken: "secret"}, nil))
	req := httptest.NewRequest(http.MethodPost, "/auth/register", bytes.NewReader([]byte("{not json")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("статус = %d, ожидался 400", rec.Code)
	}
}

// TestRegisterForbiddenWhenNoActiveToken — нет активного токена → 403 (Gherkin §1
// «Регистрация без токена запрещена»).
func TestRegisterForbiddenWhenNoActiveToken(t *testing.T) {
	q := fakeQuerier{activeErr: pgx.ErrNoRows}
	rec := doRegister(t, q, RegisterRequest{Username: "u", Password: "p", RegistrationToken: "anything"})
	if rec.Code != http.StatusForbidden {
		t.Fatalf("статус = %d, ожидался 403", rec.Code)
	}
}

// TestRegisterForbiddenWhenTokenMismatch — предъявлен неверный токен → 403.
func TestRegisterForbiddenWhenTokenMismatch(t *testing.T) {
	q := fakeQuerier{activeToken: "real-secret"}
	rec := doRegister(t, q, RegisterRequest{Username: "u", Password: "p", RegistrationToken: "wrong"})
	if rec.Code != http.StatusForbidden {
		t.Fatalf("статус = %d, ожидался 403", rec.Code)
	}
}

// TestHealthzServedByRouter — собранный API-роутер отвечает 200 на /healthz.
func TestHealthzServedByRouter(t *testing.T) {
	router := NewRouter(NewServer(fakeQuerier{}, nil))
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("статус /healthz = %d, ожидался 200", rec.Code)
	}
}

// TestUnimplementedReturns501 — нереализованная операция (логин, тикет 1.3)
// отвечает 501 через встроенную заглушку Unimplemented.
func TestUnimplementedReturns501(t *testing.T) {
	router := NewRouter(NewServer(fakeQuerier{}, nil))
	req := httptest.NewRequest(http.MethodPost, "/auth/login", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("статус /auth/login = %d, ожидался 501", rec.Code)
	}
}
