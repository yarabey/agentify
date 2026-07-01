// Unit-тесты обработчика POST /auth/refresh без БД (тикет 1.3, FR A3).
//
// Проверяют форму ответов через httptest поверх собранного роутера с
// подменённым слоем данных (fake Querier): валидация тела, единый 401 на
// нерабочий refresh (не найден / отозван / истёк), успешная ротация (старый
// токен отзывается, выдаётся новая пара). Сценарии с реальной БД (ротация
// инвалидирует старый токен на повторном вызове) — в
// refresh_integration_test.go (тег integration).
package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/yarabey/agentify/internal/auth"
	"github.com/yarabey/agentify/orchestrator/internal/db"
)

// doRefresh прогоняет тело req через роутер с заданным Querier и возвращает
// записанный ответ POST /auth/refresh.
func doRefresh(t *testing.T, q Querier, body any) *httptest.ResponseRecorder {
	t.Helper()
	router := NewRouter(newTestServer(q))
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal тела: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/auth/refresh", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

// TestRefreshValidationRejectsEmptyToken — пустой refresh_token → 400.
func TestRefreshValidationRejectsEmptyToken(t *testing.T) {
	rec := doRefresh(t, fakeQuerier{}, PostAuthRefreshJSONBody{RefreshToken: "  "})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("статус = %d, ожидался 400", rec.Code)
	}
}

// TestRefreshRejectsMalformedJSON — невалидный JSON → 400.
func TestRefreshRejectsMalformedJSON(t *testing.T) {
	router := NewRouter(newTestServer(fakeQuerier{}))
	req := httptest.NewRequest(http.MethodPost, "/auth/refresh", bytes.NewReader([]byte("{not json")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("статус = %d, ожидался 400", rec.Code)
	}
}

// TestRefreshUnauthorizedWhenTokenNotFound — токен не найден в БД → 401.
func TestRefreshUnauthorizedWhenTokenNotFound(t *testing.T) {
	q := fakeQuerier{getRefreshTokenByHashErr: pgx.ErrNoRows}
	rec := doRefresh(t, q, PostAuthRefreshJSONBody{RefreshToken: "unknown-token"})
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("статус = %d (%s), ожидался 401", rec.Code, rec.Body.String())
	}
}

// TestRefreshUnauthorizedWhenTokenRevoked — токен уже отозван → 401
// (приёмочный сценарий «отозванный токен → 401», FR A3).
func TestRefreshUnauthorizedWhenTokenRevoked(t *testing.T) {
	q := fakeQuerier{getRefreshTokenByHashResult: db.RefreshToken{
		UserID:    pgtype.UUID{Bytes: [16]byte{1}, Valid: true},
		Revoked:   true,
		ExpiresAt: pgtype.Timestamptz{Time: time.Now().Add(time.Hour), Valid: true},
	}}
	rec := doRefresh(t, q, PostAuthRefreshJSONBody{RefreshToken: "revoked-token"})
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("статус = %d (%s), ожидался 401", rec.Code, rec.Body.String())
	}
}

// TestRefreshUnauthorizedWhenTokenExpired — токен истёк → 401 (приёмочный
// сценарий «истёкший токен → 401», FR A3).
func TestRefreshUnauthorizedWhenTokenExpired(t *testing.T) {
	q := fakeQuerier{getRefreshTokenByHashResult: db.RefreshToken{
		UserID:    pgtype.UUID{Bytes: [16]byte{1}, Valid: true},
		Revoked:   false,
		ExpiresAt: pgtype.Timestamptz{Time: time.Now().Add(-time.Hour), Valid: true},
	}}
	rec := doRefresh(t, q, PostAuthRefreshJSONBody{RefreshToken: "expired-token"})
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("статус = %d (%s), ожидался 401", rec.Code, rec.Body.String())
	}
}

// TestRefreshUnauthorizedWhenExpiresAtMissing — запись без выставленного
// expires_at (защитный случай) трактуется как нерабочая → 401.
func TestRefreshUnauthorizedWhenExpiresAtMissing(t *testing.T) {
	q := fakeQuerier{getRefreshTokenByHashResult: db.RefreshToken{
		UserID:  pgtype.UUID{Bytes: [16]byte{1}, Valid: true},
		Revoked: false,
		// ExpiresAt намеренно нулевой (Valid: false).
	}}
	rec := doRefresh(t, q, PostAuthRefreshJSONBody{RefreshToken: "no-expiry-token"})
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("статус = %d (%s), ожидался 401", rec.Code, rec.Body.String())
	}
}

// TestRefreshSuccessRotatesToken — рабочий refresh → 200, новая пара токенов,
// старый токен отозван (ротация: повторное использование украденного
// перехваченного refresh после легитимной ротации отклоняется, FR A3).
func TestRefreshSuccessRotatesToken(t *testing.T) {
	userID := pgtype.UUID{Bytes: [16]byte{7}, Valid: true}
	var revoked []string
	q := fakeQuerier{
		getRefreshTokenByHashResult: db.RefreshToken{
			UserID:    userID,
			TokenHash: auth.HashRefreshToken("old-refresh-token"),
			Revoked:   false,
			ExpiresAt: pgtype.Timestamptz{Time: time.Now().Add(time.Hour), Valid: true},
		},
		revokedHashes: &revoked,
	}

	rec := doRefresh(t, q, PostAuthRefreshJSONBody{RefreshToken: "old-refresh-token"})
	if rec.Code != http.StatusOK {
		t.Fatalf("статус = %d (%s), ожидался 200", rec.Code, rec.Body.String())
	}

	var pair TokenPair
	if err := json.Unmarshal(rec.Body.Bytes(), &pair); err != nil {
		t.Fatalf("unmarshal TokenPair: %v", err)
	}
	if pair.AccessToken == "" || pair.RefreshToken == "" {
		t.Fatalf("пустой access/refresh в новой паре: %+v", pair)
	}
	if pair.RefreshToken == "old-refresh-token" {
		t.Fatal("новый refresh совпадает со старым — ротации не произошло")
	}

	gotUserID, err := auth.ParseAccessToken(pair.AccessToken, []byte(testJWTSigningKey))
	if err != nil {
		t.Fatalf("ParseAccessToken(новый access): %v", err)
	}
	if gotUserID.String() != userID.String() {
		t.Errorf("ParseAccessToken вернул %s, ожидался %s", gotUserID, userID.String())
	}

	// Старый токен должен быть отозван ровно один раз, с правильным хэшем.
	wantHash := auth.HashRefreshToken("old-refresh-token")
	if len(revoked) != 1 || revoked[0] != wantHash {
		t.Fatalf("revokedHashes = %v, ожидался [%s]", revoked, wantHash)
	}
}
