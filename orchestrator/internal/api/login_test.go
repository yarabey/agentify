// Unit-тесты обработчика POST /auth/login без БД (тикет 1.3, FR A3).
//
// Проверяют форму ответов через httptest поверх собранного роутера с
// подменённым слоем данных (fake Querier): валидация тела, единый 401 на
// неверные креды (несуществующий username И неверный пароль — без различия в
// ответе), успешная выдача пары токенов. Сценарии с реальной БД — в
// login_integration_test.go (тег integration).
package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/yarabey/agentify/internal/auth"
	"github.com/yarabey/agentify/orchestrator/internal/db"
)

// doLogin прогоняет тело req через роутер с заданным Querier и возвращает
// записанный ответ POST /auth/login.
func doLogin(t *testing.T, q Querier, body any) *httptest.ResponseRecorder {
	t.Helper()
	router := NewRouter(newTestServer(q))
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal тела: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/auth/login", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

// TestLoginValidationRejectsEmptyFields — пустые username/password → 400.
func TestLoginValidationRejectsEmptyFields(t *testing.T) {
	cases := map[string]LoginRequest{
		"пустой username": {Username: "", Password: "p"},
		"пустой password": {Username: "u", Password: "  "},
	}
	for name, body := range cases {
		rec := doLogin(t, fakeQuerier{}, body)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: статус = %d, ожидался 400", name, rec.Code)
		}
	}
}

// TestLoginRejectsMalformedJSON — невалидный JSON → 400.
func TestLoginRejectsMalformedJSON(t *testing.T) {
	router := NewRouter(newTestServer(fakeQuerier{}))
	req := httptest.NewRequest(http.MethodPost, "/auth/login", bytes.NewReader([]byte("{not json")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("статус = %d, ожидался 400", rec.Code)
	}
}

// TestLoginUnauthorizedWhenUsernameNotFound — несуществующий username → 401
// (приёмочный сценарий «неверные креды → 401», FR A3).
func TestLoginUnauthorizedWhenUsernameNotFound(t *testing.T) {
	q := fakeQuerier{getUserByUsernameErr: pgx.ErrNoRows}
	rec := doLogin(t, q, LoginRequest{Username: "ghost", Password: "whatever"})
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("статус = %d (%s), ожидался 401", rec.Code, rec.Body.String())
	}
}

// TestLoginUnauthorizedWhenPasswordWrong — существующий username, неверный
// пароль → 401 (приёмочный сценарий «неверные креды → 401», FR A3).
func TestLoginUnauthorizedWhenPasswordWrong(t *testing.T) {
	hash, err := auth.HashPassword("correct-password")
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	q := fakeQuerier{getUserByUsernameUser: db.User{
		ID:           pgtype.UUID{Bytes: [16]byte{1}, Valid: true},
		Username:     "alice",
		PasswordHash: hash,
	}}
	rec := doLogin(t, q, LoginRequest{Username: "alice", Password: "wrong-password"})
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("статус = %d (%s), ожидался 401", rec.Code, rec.Body.String())
	}
}

// TestLoginUnauthorizedResponsesIndistinguishable — несуществующий username и
// неверный пароль дают ОДИНАКОВЫЙ код+тело ответа: протокол не должен
// раскрывать, какая часть учётных данных неверна (FR A3).
func TestLoginUnauthorizedResponsesIndistinguishable(t *testing.T) {
	hash, err := auth.HashPassword("correct-password")
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}

	noUser := doLogin(t, fakeQuerier{getUserByUsernameErr: pgx.ErrNoRows}, LoginRequest{Username: "ghost", Password: "x"})
	wrongPass := doLogin(t, fakeQuerier{getUserByUsernameUser: db.User{
		ID:           pgtype.UUID{Bytes: [16]byte{2}, Valid: true},
		Username:     "alice",
		PasswordHash: hash,
	}}, LoginRequest{Username: "alice", Password: "wrong"})

	if noUser.Code != wrongPass.Code {
		t.Fatalf("статусы различаются: несуществующий username=%d, неверный пароль=%d", noUser.Code, wrongPass.Code)
	}
	if noUser.Body.String() != wrongPass.Body.String() {
		t.Fatalf("тела ответов различаются: %q vs %q — утечка, какая часть кредов неверна", noUser.Body.String(), wrongPass.Body.String())
	}
}

// TestLoginSuccessIssuesTokenPair — верные креды → 200 + TokenPair с непустыми
// access/refresh и ExpiresIn = TTL access-токена в секундах (FR A3).
func TestLoginSuccessIssuesTokenPair(t *testing.T) {
	const password = "correct-password"
	hash, err := auth.HashPassword(password)
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	userID := pgtype.UUID{Bytes: [16]byte{3}, Valid: true}
	q := fakeQuerier{getUserByUsernameUser: db.User{
		ID:           userID,
		Username:     "alice",
		PasswordHash: hash,
	}}

	rec := doLogin(t, q, LoginRequest{Username: "alice", Password: password})
	if rec.Code != http.StatusOK {
		t.Fatalf("статус = %d (%s), ожидался 200", rec.Code, rec.Body.String())
	}

	var pair TokenPair
	if err := json.Unmarshal(rec.Body.Bytes(), &pair); err != nil {
		t.Fatalf("unmarshal TokenPair: %v", err)
	}
	if pair.AccessToken == "" {
		t.Error("AccessToken пуст")
	}
	if pair.RefreshToken == "" {
		t.Error("RefreshToken пуст")
	}
	if pair.ExpiresIn == nil || *pair.ExpiresIn != int(auth.AccessTokenTTL.Seconds()) {
		t.Errorf("ExpiresIn = %v, ожидалось %d", pair.ExpiresIn, int(auth.AccessTokenTTL.Seconds()))
	}

	// Access-токен подписан тем же ключом, что у тестового сервера, и содержит
	// верный userID — то, что тикет 1.4 будет проверять в middleware.
	gotUserID, err := auth.ParseAccessToken(pair.AccessToken, []byte(testJWTSigningKey))
	if err != nil {
		t.Fatalf("ParseAccessToken(выпущенный токен): %v", err)
	}
	if gotUserID.String() != userID.String() {
		t.Errorf("ParseAccessToken вернул %s, ожидался %s", gotUserID, userID.String())
	}
}
