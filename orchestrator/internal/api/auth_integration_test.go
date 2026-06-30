//go:build integration

// Integration-тесты POST /auth/login, /auth/refresh, /auth/logout на РЕАЛЬНОМ
// Postgres через testcontainers-go (тикет 1.3; стек —
// docs/01_tech_stack_and_architecture.md §3). Тег integration — как у
// register_integration_test.go (setupDB/seedActiveToken/testJWTSigningKey
// переиспользуются оттуда, тот же файл пакета api_test).
//
// Покрываем обязательные приёмочные сценарии тикета 1.3 (FR A3):
//  1. Ротация refresh: после /auth/refresh старый refresh-токен недействителен
//     (повторный /auth/refresh с ним → 401);
//  2. Отозванный токен: явный /auth/logout делает refresh недействительным —
//     последующий /auth/refresh с ним → 401;
//  3. Невалидные креды (несуществующий username И неверный пароль) → 401,
//     с реальной БД (argon2id-хэш, сохранённый ticket-1.2-кодом CreateUser).
//
// Сценарий «истёкший access-токен → 401» — на уровне internal/auth
// (jwt_test.go, TestParseAccessTokenRejectsExpired): полноценная HTTP-аутентификация
// access-токеном (middleware) — предмет тикета 1.4, здесь её ещё нет (ни один
// эндпоинт её не требует — login/refresh/logout сами выдают токены).
package api_test

import (
	"bytes"
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

// postJSON шлёт POST path с JSON-телом body через httptest поверх router.
func postJSON(t *testing.T, router http.Handler, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal тела: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

// TestIntegration_Login_InvalidCredentials — несуществующий username и
// неверный пароль для существующего пользователя оба дают 401 (FR A3),
// проверено на реальной БД с argon2id-хэшем.
func TestIntegration_Login_InvalidCredentials(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupDB(ctx, t)
	defer done()

	const username, password = "dave", "correct-horse-battery-staple"
	hash, err := auth.HashPassword(password)
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	if _, err := db.New(pool).CreateUser(ctx, db.CreateUserParams{
		Username:     username,
		PasswordHash: hash,
		IsAdmin:      false,
	}); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	router := api.NewRouter(api.NewServer(db.New(pool), nil, []byte(testJWTSigningKey)))

	unknownUser := postJSON(t, router, "/auth/login", api.LoginRequest{Username: "ghost", Password: "whatever"})
	if unknownUser.Code != http.StatusUnauthorized {
		t.Fatalf("[несуществующий username] статус = %d (%s), ожидался 401", unknownUser.Code, unknownUser.Body.String())
	}

	wrongPassword := postJSON(t, router, "/auth/login", api.LoginRequest{Username: username, Password: "wrong-password"})
	if wrongPassword.Code != http.StatusUnauthorized {
		t.Fatalf("[неверный пароль] статус = %d (%s), ожидался 401", wrongPassword.Code, wrongPassword.Body.String())
	}

	if unknownUser.Body.String() != wrongPassword.Body.String() {
		t.Fatalf("ответы различаются: %q vs %q — утечка, какая часть кредов неверна", unknownUser.Body.String(), wrongPassword.Body.String())
	}
	t.Logf("OK: несуществующий username и неверный пароль → единый 401 (%s)", unknownUser.Body.String())
}

// TestIntegration_Login_Success — верные креды на реальной БД → 200 + рабочая
// пара токенов (предусловие для остальных сценариев этого файла).
func TestIntegration_Login_Success(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupDB(ctx, t)
	defer done()

	const username, password = "erin", "s3cr3t-pass-erin"
	hash, err := auth.HashPassword(password)
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	if _, err := db.New(pool).CreateUser(ctx, db.CreateUserParams{
		Username:     username,
		PasswordHash: hash,
		IsAdmin:      false,
	}); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	router := api.NewRouter(api.NewServer(db.New(pool), nil, []byte(testJWTSigningKey)))

	rec := postJSON(t, router, "/auth/login", api.LoginRequest{Username: username, Password: password})
	if rec.Code != http.StatusOK {
		t.Fatalf("статус = %d (%s), ожидался 200", rec.Code, rec.Body.String())
	}
	var pair api.TokenPair
	if err := json.Unmarshal(rec.Body.Bytes(), &pair); err != nil {
		t.Fatalf("unmarshal TokenPair: %v", err)
	}
	if pair.AccessToken == "" || pair.RefreshToken == "" {
		t.Fatalf("пустые токены в ответе: %+v", pair)
	}
	t.Logf("OK: верные креды → 200, выдана пара токенов")
}

// TestIntegration_Refresh_RotationInvalidatesOldToken — приёмочный сценарий
// «ротация refresh инвалидирует старый токен» (FR A3): логин → refresh →
// повторный refresh со СТАРЫМ токеном → 401.
func TestIntegration_Refresh_RotationInvalidatesOldToken(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupDB(ctx, t)
	defer done()

	const username, password = "frank", "frank-secret-pass"
	hash, err := auth.HashPassword(password)
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	if _, err := db.New(pool).CreateUser(ctx, db.CreateUserParams{
		Username:     username,
		PasswordHash: hash,
		IsAdmin:      false,
	}); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	router := api.NewRouter(api.NewServer(db.New(pool), nil, []byte(testJWTSigningKey)))

	loginRec := postJSON(t, router, "/auth/login", api.LoginRequest{Username: username, Password: password})
	if loginRec.Code != http.StatusOK {
		t.Fatalf("login: статус = %d (%s), ожидался 200", loginRec.Code, loginRec.Body.String())
	}
	var first api.TokenPair
	if err := json.Unmarshal(loginRec.Body.Bytes(), &first); err != nil {
		t.Fatalf("unmarshal TokenPair (login): %v", err)
	}

	// Первый refresh: старый refresh-токен (выданный логином) обменивается на
	// новую пару — должен сработать (200).
	refreshRec := postJSON(t, router, "/auth/refresh", api.PostAuthRefreshJSONBody{RefreshToken: first.RefreshToken})
	if refreshRec.Code != http.StatusOK {
		t.Fatalf("первый refresh: статус = %d (%s), ожидался 200", refreshRec.Code, refreshRec.Body.String())
	}
	var second api.TokenPair
	if err := json.Unmarshal(refreshRec.Body.Bytes(), &second); err != nil {
		t.Fatalf("unmarshal TokenPair (refresh): %v", err)
	}
	if second.RefreshToken == first.RefreshToken {
		t.Fatal("новый refresh совпадает со старым — ротации не произошло")
	}

	// Повторная попытка обменять УЖЕ использованный (старый) refresh-токен —
	// должна быть отклонена: ротация необратимо инвалидирует старый токен.
	replayRec := postJSON(t, router, "/auth/refresh", api.PostAuthRefreshJSONBody{RefreshToken: first.RefreshToken})
	if replayRec.Code != http.StatusUnauthorized {
		t.Fatalf("повторный refresh старым токеном: статус = %d (%s), ожидался 401", replayRec.Code, replayRec.Body.String())
	}

	// Новый (второй) refresh-токен остаётся рабочим — ротация не задела его.
	thirdRec := postJSON(t, router, "/auth/refresh", api.PostAuthRefreshJSONBody{RefreshToken: second.RefreshToken})
	if thirdRec.Code != http.StatusOK {
		t.Fatalf("refresh новым (вторым) токеном: статус = %d (%s), ожидался 200", thirdRec.Code, thirdRec.Body.String())
	}
	t.Logf("OK: ротация — старый refresh после использования даёт 401 при повторе, новый остаётся рабочим")
}

// TestIntegration_Logout_RevokesTokenForRefresh — приёмочный сценарий
// «отозванный токен → 401 на refresh, в т.ч. после logout» (FR A3): логин →
// logout → refresh тем же токеном → 401.
func TestIntegration_Logout_RevokesTokenForRefresh(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupDB(ctx, t)
	defer done()

	const username, password = "grace", "grace-secret-pass"
	hash, err := auth.HashPassword(password)
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	if _, err := db.New(pool).CreateUser(ctx, db.CreateUserParams{
		Username:     username,
		PasswordHash: hash,
		IsAdmin:      false,
	}); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	router := api.NewRouter(api.NewServer(db.New(pool), nil, []byte(testJWTSigningKey)))

	loginRec := postJSON(t, router, "/auth/login", api.LoginRequest{Username: username, Password: password})
	if loginRec.Code != http.StatusOK {
		t.Fatalf("login: статус = %d (%s), ожидался 200", loginRec.Code, loginRec.Body.String())
	}
	var pair api.TokenPair
	if err := json.Unmarshal(loginRec.Body.Bytes(), &pair); err != nil {
		t.Fatalf("unmarshal TokenPair (login): %v", err)
	}

	logoutRec := postJSON(t, router, "/auth/logout", api.PostAuthLogoutJSONBody{RefreshToken: pair.RefreshToken})
	if logoutRec.Code != http.StatusNoContent {
		t.Fatalf("logout: статус = %d (%s), ожидался 204", logoutRec.Code, logoutRec.Body.String())
	}

	refreshRec := postJSON(t, router, "/auth/refresh", api.PostAuthRefreshJSONBody{RefreshToken: pair.RefreshToken})
	if refreshRec.Code != http.StatusUnauthorized {
		t.Fatalf("refresh после logout: статус = %d (%s), ожидался 401", refreshRec.Code, refreshRec.Body.String())
	}

	// Повторный logout тем же (уже отозванным) токеном — идемпотентно, тоже 204.
	secondLogoutRec := postJSON(t, router, "/auth/logout", api.PostAuthLogoutJSONBody{RefreshToken: pair.RefreshToken})
	if secondLogoutRec.Code != http.StatusNoContent {
		t.Fatalf("повторный logout: статус = %d (%s), ожидался 204", secondLogoutRec.Code, secondLogoutRec.Body.String())
	}
	t.Logf("OK: logout отзывает refresh, последующий refresh им → 401, повторный logout идемпотентен (204)")
}
