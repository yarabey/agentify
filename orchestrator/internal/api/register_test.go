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
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/google/uuid"

	"github.com/yarabey/agentify/orchestrator/internal/db"
)

// fakeQuerier — подменный слой данных для unit-тестов обработчиков (тикеты
// 1.2/1.3). Реализует весь Querier через настраиваемые поля-результаты; тесты
// заполняют только то, что им нужно для сценария.
type fakeQuerier struct {
	activeToken    string
	activeErr      error
	createUserUser db.User
	createUserErr  error

	getUserByUsernameUser db.User
	getUserByUsernameErr  error

	getUserByIDUser db.User
	getUserByIDErr  error

	createRefreshTokenErr error

	getRefreshTokenByHashResult db.RefreshToken
	getRefreshTokenByHashErr    error

	revokeRefreshTokenByHashErr error
	// revokedHashes собирает хэши, переданные в RevokeRefreshTokenByHash —
	// позволяет тестам проверить, что отозван именно ожидаемый (старый) токен.
	revokedHashes *[]string

	createIntegrationResult db.Integration
	createIntegrationErr    error

	listIntegrationsResult []db.Integration
	listIntegrationsErr    error

	getIntegrationResult db.Integration
	getIntegrationErr    error

	updateIntegrationResult db.Integration
	updateIntegrationErr    error

	getIntegrationByUUIDHMACResult db.Integration
	getIntegrationByUUIDHMACErr    error

	createTaskResult db.Task
	createTaskErr    error
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

func (f fakeQuerier) GetUserByUsername(context.Context, string) (db.User, error) {
	if f.getUserByUsernameErr != nil {
		return db.User{}, f.getUserByUsernameErr
	}
	return f.getUserByUsernameUser, nil
}

func (f fakeQuerier) GetUserByID(context.Context, pgtype.UUID) (db.User, error) {
	if f.getUserByIDErr != nil {
		return db.User{}, f.getUserByIDErr
	}
	return f.getUserByIDUser, nil
}

func (f fakeQuerier) CreateRefreshToken(_ context.Context, arg db.CreateRefreshTokenParams) (db.RefreshToken, error) {
	if f.createRefreshTokenErr != nil {
		return db.RefreshToken{}, f.createRefreshTokenErr
	}
	return db.RefreshToken{
		UserID:    arg.UserID,
		TokenHash: arg.TokenHash,
		ExpiresAt: arg.ExpiresAt,
	}, nil
}

func (f fakeQuerier) GetRefreshTokenByHash(context.Context, string) (db.RefreshToken, error) {
	if f.getRefreshTokenByHashErr != nil {
		return db.RefreshToken{}, f.getRefreshTokenByHashErr
	}
	return f.getRefreshTokenByHashResult, nil
}

func (f fakeQuerier) RevokeRefreshTokenByHash(_ context.Context, tokenHash string) error {
	if f.revokedHashes != nil {
		*f.revokedHashes = append(*f.revokedHashes, tokenHash)
	}
	return f.revokeRefreshTokenByHashErr
}

func (f fakeQuerier) CreateIntegration(context.Context, db.CreateIntegrationParams) (db.Integration, error) {
	if f.createIntegrationErr != nil {
		return db.Integration{}, f.createIntegrationErr
	}
	return f.createIntegrationResult, nil
}

func (f fakeQuerier) ListIntegrationsByUser(context.Context, pgtype.UUID) ([]db.Integration, error) {
	if f.listIntegrationsErr != nil {
		return nil, f.listIntegrationsErr
	}
	return f.listIntegrationsResult, nil
}

func (f fakeQuerier) GetIntegrationByIDAndUser(context.Context, db.GetIntegrationByIDAndUserParams) (db.Integration, error) {
	if f.getIntegrationErr != nil {
		return db.Integration{}, f.getIntegrationErr
	}
	return f.getIntegrationResult, nil
}

func (f fakeQuerier) UpdateIntegration(context.Context, db.UpdateIntegrationParams) (db.Integration, error) {
	if f.updateIntegrationErr != nil {
		return db.Integration{}, f.updateIntegrationErr
	}
	return f.updateIntegrationResult, nil
}

func (f fakeQuerier) GetIntegrationByUUIDHMAC(context.Context, string) (db.Integration, error) {
	if f.getIntegrationByUUIDHMACErr != nil {
		return db.Integration{}, f.getIntegrationByUUIDHMACErr
	}
	return f.getIntegrationByUUIDHMACResult, nil
}

func (f fakeQuerier) CreateTask(context.Context, db.CreateTaskParams) (db.Task, error) {
	if f.createTaskErr != nil {
		return db.Task{}, f.createTaskErr
	}
	return f.createTaskResult, nil
}

// testJWTSigningKey — ключ подписи access-JWT для unit-тестов пакета api
// (тикеты 1.2/1.3). Не секрет — используется только в тестовом процессе.
const testJWTSigningKey = "unit-test-jwt-signing-key"

// testEncryptionKey32 — тестовый мастер-ключ шифрования РОВНО 32 байта для
// unit-тестов пакета api (тикет 2.2, internal/crypto). Не секрет —
// используется только в тестовом процессе, как и testJWTSigningKey.
const testEncryptionKey32 = "0123456789abcdef0123456789abcdef"

// newTestServer собирает *Server с тестовым ключом подписи JWT и тестовым
// мастер-ключом шифрования поверх переданного fakeQuerier/Querier — общий
// конструктор для всех unit-тестов пакета api (регистрация, логин, refresh,
// logout, интеграции).
func newTestServer(q Querier) *Server {
	return NewServer(q, nil, []byte(testJWTSigningKey), []byte(testEncryptionKey32))
}

// doRegister прогоняет тело req через роутер с заданным fakeQuerier и возвращает
// записанный ответ.
func doRegister(t *testing.T, q Querier, body any) *httptest.ResponseRecorder {
	t.Helper()
	router := NewRouter(newTestServer(q))
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
	router := NewRouter(newTestServer(fakeQuerier{activeToken: "secret"}))
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
	router := NewRouter(newTestServer(fakeQuerier{}))
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("статус /healthz = %d, ожидался 200", rec.Code)
	}
}

// TestUnimplementedReturns501 — нереализованная операция (привязка Telegram,
// будущий тикет) отвечает 501 через встроенную заглушку Unimplemented.
// /auth/login, /auth/refresh, /auth/logout реализованы тикетом 1.3 — их
// сценарии теперь покрыты login_test.go/refresh_test.go/logout_test.go.
//
// /channels/telegram/link-code защищён auth-middleware (тикет 1.4, нет
// `security: []` в openapi.yaml) — даже нереализованные операции проходят
// через него, поэтому запрос несёт валидный Bearer-токен: тест проверяет
// именно заглушку Unimplemented (501), а не auth-middleware (его 401-поведение
// для этого же маршрута — TestUnimplementedRequiresBearerToken ниже).
func TestUnimplementedReturns501(t *testing.T) {
	router := NewRouter(newTestServer(fakeQuerier{}))
	req := httptest.NewRequest(http.MethodPost, "/channels/telegram/link-code", nil)
	req.Header.Set("Authorization", "Bearer "+issueTestAccessToken(t, uuid.New(), time.Now()))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("статус /channels/telegram/link-code = %d, ожидался 501", rec.Code)
	}
}

// TestUnimplementedRequiresBearerToken — та же нереализованная операция без
// Authorization-заголовка отдаёт 401 от auth-middleware, не доходя до
// заглушки Unimplemented (FR A3, D2, тикет 1.4): «защищённый, но ещё не
// реализованный» — всё равно защищённый.
func TestUnimplementedRequiresBearerToken(t *testing.T) {
	router := NewRouter(newTestServer(fakeQuerier{}))
	req := httptest.NewRequest(http.MethodPost, "/channels/telegram/link-code", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("статус /channels/telegram/link-code без токена = %d, ожидался 401", rec.Code)
	}
}
