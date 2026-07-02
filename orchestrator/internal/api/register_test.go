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
	"github.com/jackc/pgx/v5/pgtype"

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

	getTaskByUserAndIdempotencyKeyResult db.Task
	getTaskByUserAndIdempotencyKeyErr    error

	getTaskByIDAndUserResult db.Task
	getTaskByIDAndUserErr    error

	getTaskByIDAndIntegrationResult db.Task
	getTaskByIDAndIntegrationErr    error

	listAgentQuestionEventsByTaskResult []db.TaskEvent
	listAgentQuestionEventsByTaskErr    error

	listCommandApprovalRequestEventsByTaskResult []db.TaskEvent
	listCommandApprovalRequestEventsByTaskErr    error

	listTasksByUserResult []db.Task
	listTasksByUserErr    error

	listTaskEventsByTaskResult []db.TaskEvent
	listTaskEventsByTaskErr    error

	listActiveTaskIDsByIntegrationResult []pgtype.UUID
	listActiveTaskIDsByIntegrationErr    error

	softDeleteIntegrationResult pgtype.UUID
	softDeleteIntegrationErr    error

	getChannelLinkByChannelAndExternalIDResult db.ChannelLink
	getChannelLinkByChannelAndExternalIDErr    error
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

func (f fakeQuerier) CreateTask(_ context.Context, arg db.CreateTaskParams) (db.Task, error) {
	if f.createTaskErr != nil {
		return db.Task{}, f.createTaskErr
	}
	// Эхо-семантика RETURNING: реальный INSERT возвращает вставленный
	// (уже зашифрованный, FR I1, тикет 11.1) text_enc, а не пресет фикстуры —
	// так toTask на выходе PostTasks расшифровывает ровно то, что зашифровал
	// обработчик, и тест получает исходный текст обратно.
	res := f.createTaskResult
	res.TextEnc = arg.TextEnc
	return res, nil
}

func (f fakeQuerier) GetTaskByUserAndIdempotencyKey(context.Context, db.GetTaskByUserAndIdempotencyKeyParams) (db.Task, error) {
	if f.getTaskByUserAndIdempotencyKeyErr != nil {
		return db.Task{}, f.getTaskByUserAndIdempotencyKeyErr
	}
	return f.getTaskByUserAndIdempotencyKeyResult, nil
}

func (f fakeQuerier) GetTaskByIDAndUser(context.Context, db.GetTaskByIDAndUserParams) (db.Task, error) {
	if f.getTaskByIDAndUserErr != nil {
		return db.Task{}, f.getTaskByIDAndUserErr
	}
	return f.getTaskByIDAndUserResult, nil
}

func (f fakeQuerier) GetTaskByIDAndIntegration(context.Context, db.GetTaskByIDAndIntegrationParams) (db.Task, error) {
	if f.getTaskByIDAndIntegrationErr != nil {
		return db.Task{}, f.getTaskByIDAndIntegrationErr
	}
	return f.getTaskByIDAndIntegrationResult, nil
}

func (f fakeQuerier) ListAgentQuestionEventsByTask(context.Context, pgtype.UUID) ([]db.TaskEvent, error) {
	if f.listAgentQuestionEventsByTaskErr != nil {
		return nil, f.listAgentQuestionEventsByTaskErr
	}
	return f.listAgentQuestionEventsByTaskResult, nil
}

func (f fakeQuerier) ListCommandApprovalRequestEventsByTask(context.Context, pgtype.UUID) ([]db.TaskEvent, error) {
	if f.listCommandApprovalRequestEventsByTaskErr != nil {
		return nil, f.listCommandApprovalRequestEventsByTaskErr
	}
	return f.listCommandApprovalRequestEventsByTaskResult, nil
}

func (f fakeQuerier) ListTasksByUser(context.Context, db.ListTasksByUserParams) ([]db.Task, error) {
	if f.listTasksByUserErr != nil {
		return nil, f.listTasksByUserErr
	}
	return f.listTasksByUserResult, nil
}

func (f fakeQuerier) ListTaskEventsByTask(context.Context, pgtype.UUID) ([]db.TaskEvent, error) {
	if f.listTaskEventsByTaskErr != nil {
		return nil, f.listTaskEventsByTaskErr
	}
	return f.listTaskEventsByTaskResult, nil
}

func (f fakeQuerier) ListActiveTaskIDsByIntegration(context.Context, pgtype.UUID) ([]pgtype.UUID, error) {
	if f.listActiveTaskIDsByIntegrationErr != nil {
		return nil, f.listActiveTaskIDsByIntegrationErr
	}
	return f.listActiveTaskIDsByIntegrationResult, nil
}

func (f fakeQuerier) SoftDeleteIntegration(context.Context, db.SoftDeleteIntegrationParams) (pgtype.UUID, error) {
	if f.softDeleteIntegrationErr != nil {
		return pgtype.UUID{}, f.softDeleteIntegrationErr
	}
	return f.softDeleteIntegrationResult, nil
}

// GetChannelLinkByChannelAndExternalID — фейк для юнит-тестов
// PostChannelsTelegramToken (тикет 10.3, см. channels_token_test.go).
func (f fakeQuerier) GetChannelLinkByChannelAndExternalID(context.Context, db.GetChannelLinkByChannelAndExternalIDParams) (db.ChannelLink, error) {
	if f.getChannelLinkByChannelAndExternalIDErr != nil {
		return db.ChannelLink{}, f.getChannelLinkByChannelAndExternalIDErr
	}
	return f.getChannelLinkByChannelAndExternalIDResult, nil
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
		"пустой токен":    {Username: "u", Password: "p", RegistrationToken: ""},
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

// TestUnimplementedStubReturns501 — сама встроенная заглушка api.Unimplemented
// (server.gen.go, отвечает 501 на любую операцию ServerInterface) работает,
// даже когда её никто в проде не вызывает.
//
// До тикета 9.6 это проверялось на реальном маршруте
// /channels/telegram/link-code — последней операции контракта, у которой не
// было своего обработчика (см. TestUnimplementedRequiresBearerToken до этого
// коммита). С 9.6 Server переопределяет ВСЕ операции ServerInterface (см.
// package-godoc server.go) — Unimplemented больше не задействован ни одним
// реальным маршрутом, поэтому тест бьёт по самой заглушке напрямую, а не
// через собранный роутер: она остаётся смонтированной как страховка на
// случай будущего расширения контракта операцией без обработчика, и эта
// страховка не должна незаметно сломаться.
func TestUnimplementedStubReturns501(t *testing.T) {
	var u Unimplemented
	req := httptest.NewRequest(http.MethodPost, "/any-not-yet-implemented-operation", nil)
	rec := httptest.NewRecorder()
	u.PostChannelsTelegramLinkCode(rec, req)
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("статус = %d, ожидался 501", rec.Code)
	}
}
