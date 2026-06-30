// Unit-тесты обработчика POST /auth/logout без БД (тикет 1.3, FR A3).
//
// Проверяют форму ответов через httptest поверх собранного роутера с
// подменённым слоем данных (fake Querier): валидация тела, успешный отзыв
// (204 + отозван правильный хэш), идемпотентность (повторный/неизвестный
// токен — тоже 204, без утечки информации о существовании токена).
// Сценарий «после logout refresh даёт 401» (полный цикл с реальной БД) — в
// logout_integration_test.go (тег integration).
package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/yarabey/agentify/internal/auth"
)

// doLogout прогоняет тело req через роутер с заданным Querier и возвращает
// записанный ответ POST /auth/logout.
func doLogout(t *testing.T, q Querier, body any) *httptest.ResponseRecorder {
	t.Helper()
	router := NewRouter(newTestServer(q))
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal тела: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/auth/logout", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

// TestLogoutValidationRejectsEmptyToken — пустой refresh_token → 400.
func TestLogoutValidationRejectsEmptyToken(t *testing.T) {
	rec := doLogout(t, fakeQuerier{}, PostAuthLogoutJSONBody{RefreshToken: "  "})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("статус = %d, ожидался 400", rec.Code)
	}
}

// TestLogoutRejectsMalformedJSON — невалидный JSON → 400.
func TestLogoutRejectsMalformedJSON(t *testing.T) {
	router := NewRouter(newTestServer(fakeQuerier{}))
	req := httptest.NewRequest(http.MethodPost, "/auth/logout", bytes.NewReader([]byte("{not json")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("статус = %d, ожидался 400", rec.Code)
	}
}

// TestLogoutSuccessRevokesToken — рабочий токен → 204, RevokeRefreshTokenByHash
// вызван с хэшем именно предъявленного токена.
func TestLogoutSuccessRevokesToken(t *testing.T) {
	var revoked []string
	q := fakeQuerier{revokedHashes: &revoked}

	rec := doLogout(t, q, PostAuthLogoutJSONBody{RefreshToken: "my-refresh-token"})
	if rec.Code != http.StatusNoContent {
		t.Fatalf("статус = %d (%s), ожидался 204", rec.Code, rec.Body.String())
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("тело ответа 204 непусто: %q", rec.Body.String())
	}

	wantHash := auth.HashRefreshToken("my-refresh-token")
	if len(revoked) != 1 || revoked[0] != wantHash {
		t.Fatalf("revokedHashes = %v, ожидался [%s]", revoked, wantHash)
	}
}

// TestLogoutIdempotentForUnknownToken — logout с неизвестным/уже отозванным
// токеном тоже отвечает 204 (не раскрываем через код ответа, существовал ли
// токен, FR A3): RevokeRefreshTokenByHash в реальной БД — это UPDATE ... WHERE
// token_hash = $1, который для несуществующей строки просто не затрагивает
// строк и не возвращает ошибку (см. orchestrator/queries/refresh_tokens.sql).
func TestLogoutIdempotentForUnknownToken(t *testing.T) {
	rec := doLogout(t, fakeQuerier{}, PostAuthLogoutJSONBody{RefreshToken: "never-issued-token"})
	if rec.Code != http.StatusNoContent {
		t.Fatalf("статус = %d (%s), ожидался 204", rec.Code, rec.Body.String())
	}
}
