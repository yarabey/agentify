//go:build integration

// Integration-тест POST /channels/telegram/link-code на РЕАЛЬНОМ Postgres
// через testcontainers-go (тикет 9.6, FR A2, D3, экран «Настройки»). setupDB/
// testJWTSigningKey/testEncryptionKey32/createTestUserWithToken
// переиспользуются из register_integration_test.go/integrations_integration_test.go
// (тот же пакет api_test).
//
// В отличие от orchestrator/internal/channel/codegen_integration_test.go (та
// же бизнес-логика channel.CodeIssuer.IssueLinkCode напрямую, без HTTP) —
// здесь проверяется полный путь HTTP-хендлера: NewRouter +
// server.SetChannelLinkCodeIssuer поверх реального channel.CodeIssuer, ровно
// как собирается orchestrator/main.go. Покрывает приёмку тикета 9.6 backend:
// «valid JWT → 200 + код в БД с корректным TTL».
package api_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/yarabey/agentify/orchestrator/internal/api"
	"github.com/yarabey/agentify/orchestrator/internal/channel"
	"github.com/yarabey/agentify/orchestrator/internal/db"
)

// newChannelLinkCodeTestRouter собирает роутер с реальным
// channel.NewCodeIssuer(pool, ttl) — тот же принцип сборки, что
// orchestrator/main.go (server.SetChannelLinkCodeIssuer сразу после
// NewServer).
func newChannelLinkCodeTestRouter(pool *pgxpool.Pool, ttl time.Duration) http.Handler {
	s := api.NewServer(db.New(pool), nil, []byte(testJWTSigningKey), []byte(testEncryptionKey32))
	s.SetChannelLinkCodeIssuer(channel.NewCodeIssuer(pool, ttl))
	return api.NewRouter(s)
}

// postChannelsTelegramLinkCode шлёт POST /channels/telegram/link-code через
// httptest поверх собранного роутера с указанным Bearer-токеном
// (bearerToken == "" — запрос без заголовка Authorization).
func postChannelsTelegramLinkCode(t *testing.T, router http.Handler, bearerToken string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/channels/telegram/link-code", nil)
	if bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+bearerToken)
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

// TestIntegration_ChannelsTelegramLinkCode_ValidJWTCreatesCodeWithTTL —
// приёмка тикета 9.6: валидный access-JWT → 201 + {code, expires_at}, в БД
// РОВНО одна строка channel_link_codes с этим кодом, user_id владельца токена
// и expires_at ~ now()+ttl.
func TestIntegration_ChannelsTelegramLinkCode_ValidJWTCreatesCodeWithTTL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupDB(ctx, t)
	defer done()
	q := db.New(pool)

	user, token := createTestUserWithToken(ctx, t, q, "heidi")

	const ttl = 20 * time.Minute
	router := newChannelLinkCodeTestRouter(pool, ttl)

	baseline := time.Now()
	rec := postChannelsTelegramLinkCode(t, router, token)
	if rec.Code != http.StatusCreated {
		t.Fatalf("статус = %d (%s), ожидался 201", rec.Code, rec.Body.String())
	}

	var body struct {
		Code      string    `json:"code"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	if uerr := json.Unmarshal(rec.Body.Bytes(), &body); uerr != nil {
		t.Fatalf("тело ответа не JSON: %v", uerr)
	}
	if body.Code == "" {
		t.Fatal("code пуст в теле ответа")
	}

	stored, err := q.GetChannelLinkCode(ctx, body.Code)
	if err != nil {
		t.Fatalf("GetChannelLinkCode(%q): %v", body.Code, err)
	}
	if stored.UserID != user.ID {
		t.Fatalf("stored.UserID = %v, ожидался %v (владелец Bearer-токена)", stored.UserID, user.ID)
	}
	if stored.Channel != "telegram" {
		t.Fatalf("stored.Channel = %q, ожидался telegram", stored.Channel)
	}
	if stored.UsedAt.Valid {
		t.Fatal("свежевыпущенный код уже помечен использованным")
	}

	wantExpiry := baseline.Add(ttl)
	diff := body.ExpiresAt.Sub(wantExpiry)
	if diff < 0 {
		diff = -diff
	}
	if diff > 5*time.Second {
		t.Fatalf("expires_at = %v, ожидалось ~%v (допуск 5s), разница %v", body.ExpiresAt, wantExpiry, diff)
	}

	var n int
	if qerr := pool.QueryRow(ctx, `SELECT count(*) FROM channel_link_codes WHERE code = $1`, body.Code).Scan(&n); qerr != nil {
		t.Fatalf("count channel_link_codes: %v", qerr)
	}
	if n != 1 {
		t.Fatalf("channel_link_codes(code=%q) = %d строк, ожидалась 1", body.Code, n)
	}
	t.Logf("OK: 201, код %s создан для %s, TTL ~%v", body.Code, user.ID, ttl)
}

// TestIntegration_ChannelsTelegramLinkCode_RequiresBearerToken — без
// Authorization — 401, ни одной строки channel_link_codes не создаётся (FR
// A2: доступ только аутентифицированному пользователю).
func TestIntegration_ChannelsTelegramLinkCode_RequiresBearerToken(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupDB(ctx, t)
	defer done()

	router := newChannelLinkCodeTestRouter(pool, 15*time.Minute)
	rec := postChannelsTelegramLinkCode(t, router, "")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("статус = %d (%s), ожидался 401", rec.Code, rec.Body.String())
	}

	var n int
	if qerr := pool.QueryRow(ctx, `SELECT count(*) FROM channel_link_codes`).Scan(&n); qerr != nil {
		t.Fatalf("count channel_link_codes: %v", qerr)
	}
	if n != 0 {
		t.Fatalf("channel_link_codes = %d строк, ожидалось 0 (без Bearer код не создаётся)", n)
	}
}

// TestIntegration_ChannelsTelegramLinkCode_TwoUsersGetDifferentCodes —
// каждый пользователь получает код НА СЕБЯ (FR A2): два разных
// аутентифицированных вызова дают два разных кода, привязанных каждый к
// своему user_id.
func TestIntegration_ChannelsTelegramLinkCode_TwoUsersGetDifferentCodes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupDB(ctx, t)
	defer done()
	q := db.New(pool)

	alice, aliceToken := createTestUserWithToken(ctx, t, q, "ivan")
	_, bobToken := createTestUserWithToken(ctx, t, q, "judy")

	router := newChannelLinkCodeTestRouter(pool, 15*time.Minute)

	aliceRec := postChannelsTelegramLinkCode(t, router, aliceToken)
	bobRec := postChannelsTelegramLinkCode(t, router, bobToken)

	var aliceBody, bobBody struct {
		Code string `json:"code"`
	}
	if uerr := json.Unmarshal(aliceRec.Body.Bytes(), &aliceBody); uerr != nil {
		t.Fatalf("unmarshal alice: %v", uerr)
	}
	if uerr := json.Unmarshal(bobRec.Body.Bytes(), &bobBody); uerr != nil {
		t.Fatalf("unmarshal bob: %v", uerr)
	}
	if aliceBody.Code == bobBody.Code {
		t.Fatalf("оба пользователя получили одинаковый код %q", aliceBody.Code)
	}

	stored, err := q.GetChannelLinkCode(ctx, aliceBody.Code)
	if err != nil {
		t.Fatalf("GetChannelLinkCode(alice): %v", err)
	}
	if stored.UserID != alice.ID {
		t.Fatalf("код alice привязан к user_id=%v, ожидался %v", stored.UserID, alice.ID)
	}
}
