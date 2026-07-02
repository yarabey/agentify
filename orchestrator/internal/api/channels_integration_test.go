//go:build integration

// Integration-тесты POST /channels/telegram/link на РЕАЛЬНОМ Postgres через
// testcontainers-go (тикет 10.2, FR D3 «привязка канала к аккаунту описана
// явно», Gherkin §6 «Уведомления»). setupDB/testJWTSigningKey/testEncryptionKey32 переиспользуются
// из register_integration_test.go (тот же пакет api_test).
//
// В отличие от orchestrator/internal/channel/link_integration_test.go (та же
// бизнес-логика channel.Linker.Exchange напрямую, без HTTP) — здесь
// проверяется полный путь HTTP-хендлера: NewRouter + server.SetChannelLinker
// поверх реального channel.Linker, ровно как собирается orchestrator/main.go.
// Покрываем приёмку тикета 10.2 на уровне контракта:
//   - валидный код → 200 + тело ChannelLink, в БД РОВНО одна строка
//     channel_links;
//   - истёкший код → 409 link_code_expired, без привязки;
//   - повторное использование уже использованного кода → 409 link_code_used,
//     без второй привязки.
package api_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/yarabey/agentify/orchestrator/internal/api"
	"github.com/yarabey/agentify/orchestrator/internal/channel"
	"github.com/yarabey/agentify/orchestrator/internal/db"
)

// newChannelLinkTestRouter собирает роутер с реальным channel.NewLinker(pool)
// — тот же принцип сборки, что orchestrator/main.go (server.SetChannelLinker
// сразу после NewServer).
func newChannelLinkTestRouter(pool *pgxpool.Pool) http.Handler {
	s := api.NewServer(db.New(pool), nil, []byte(testJWTSigningKey), []byte(testEncryptionKey32))
	s.SetChannelLinker(channel.NewLinker(pool))
	return api.NewRouter(s)
}

// randomLinkCode генерирует случайный hex-код для тестовых строк
// channel_link_codes.code.
func randomLinkCode(t *testing.T) string {
	t.Helper()
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("crypto/rand.Read: %v", err)
	}
	return hex.EncodeToString(b)
}

// seedChannelLinkCode вставляет код привязки напрямую через sqlc (генерация
// кода — тикет 9.6, здесь не реализуется; предусловие теста создаётся
// INSERT'ом, как предписано брифом тикета 10.2).
func seedChannelLinkCode(ctx context.Context, t *testing.T, q *db.Queries, userID pgtype.UUID, ttl time.Duration) string {
	t.Helper()
	code := randomLinkCode(t)
	if _, err := q.CreateChannelLinkCode(ctx, db.CreateChannelLinkCodeParams{
		Code:      code,
		UserID:    userID,
		Channel:   "telegram",
		ExpiresAt: pgtype.Timestamptz{Time: time.Now().Add(ttl), Valid: true},
	}); err != nil {
		t.Fatalf("CreateChannelLinkCode: %v", err)
	}
	return code
}

// postChannelsTelegramLink шлёт POST /channels/telegram/link через httptest
// поверх собранного роутера (без Authorization — маршрут `security: []`).
func postChannelsTelegramLink(t *testing.T, router http.Handler, body api.ChannelLinkRequest) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal тела: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/channels/telegram/link", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

// TestIntegration_ChannelsTelegramLink_ValidCodeLinks — валидный код → 200 +
// ChannelLink, в БД ровно одна строка channel_links (FR D3).
func TestIntegration_ChannelsTelegramLink_ValidCodeLinks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupDB(ctx, t)
	defer done()
	q := db.New(pool)

	user, err := q.CreateUser(ctx, db.CreateUserParams{Username: "dave", PasswordHash: "argon2id$stub"})
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	code := seedChannelLinkCode(ctx, t, q, user.ID, time.Hour)

	router := newChannelLinkTestRouter(pool)
	rec := postChannelsTelegramLink(t, router, api.ChannelLinkRequest{Code: code, TelegramUserId: "tg-100"})
	if rec.Code != http.StatusOK {
		t.Fatalf("статус = %d (%s), ожидался 200", rec.Code, rec.Body.String())
	}

	var got api.ChannelLink
	if uerr := json.Unmarshal(rec.Body.Bytes(), &got); uerr != nil {
		t.Fatalf("тело ответа не JSON ChannelLink: %v", uerr)
	}
	if got.ExternalId == nil || *got.ExternalId != "tg-100" {
		t.Fatalf("external_id = %v, ожидался tg-100", got.ExternalId)
	}

	var n int
	if qerr := pool.QueryRow(ctx, `SELECT count(*) FROM channel_links WHERE user_id = $1 AND external_id = $2`, user.ID, "tg-100").Scan(&n); qerr != nil {
		t.Fatalf("count channel_links: %v", qerr)
	}
	if n != 1 {
		t.Fatalf("channel_links = %d строк, ожидалась 1", n)
	}
	t.Logf("OK: валидный код → 200, привязка создана")
}

// TestIntegration_ChannelsTelegramLink_ExpiredCodeFails — истёкший код → 409
// link_code_expired, без привязки (приёмка тикета 10.2).
func TestIntegration_ChannelsTelegramLink_ExpiredCodeFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupDB(ctx, t)
	defer done()
	q := db.New(pool)

	user, err := q.CreateUser(ctx, db.CreateUserParams{Username: "erin", PasswordHash: "argon2id$stub"})
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	code := seedChannelLinkCode(ctx, t, q, user.ID, -time.Hour)

	router := newChannelLinkTestRouter(pool)
	rec := postChannelsTelegramLink(t, router, api.ChannelLinkRequest{Code: code, TelegramUserId: "tg-101"})
	if rec.Code != http.StatusConflict {
		t.Fatalf("статус = %d (%s), ожидался 409", rec.Code, rec.Body.String())
	}
	var got api.Error
	if uerr := json.Unmarshal(rec.Body.Bytes(), &got); uerr != nil {
		t.Fatalf("тело ответа не JSON Error: %v", uerr)
	}
	if got.Code != "link_code_expired" {
		t.Fatalf("code = %q, ожидался link_code_expired", got.Code)
	}

	var n int
	if qerr := pool.QueryRow(ctx, `SELECT count(*) FROM channel_links WHERE external_id = $1`, "tg-101").Scan(&n); qerr != nil {
		t.Fatalf("count channel_links: %v", qerr)
	}
	if n != 0 {
		t.Fatalf("channel_links = %d строк, ожидалось 0 (без привязки)", n)
	}
	t.Logf("OK: истёкший код → 409 link_code_expired, без привязки")
}

// TestIntegration_ChannelsTelegramLink_UsedCodeCannotBeReused — второй обмен
// уже использованным кодом → 409 link_code_used, без второй привязки.
func TestIntegration_ChannelsTelegramLink_UsedCodeCannotBeReused(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupDB(ctx, t)
	defer done()
	q := db.New(pool)

	user, err := q.CreateUser(ctx, db.CreateUserParams{Username: "frank", PasswordHash: "argon2id$stub"})
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	code := seedChannelLinkCode(ctx, t, q, user.ID, time.Hour)

	router := newChannelLinkTestRouter(pool)
	first := postChannelsTelegramLink(t, router, api.ChannelLinkRequest{Code: code, TelegramUserId: "tg-102"})
	if first.Code != http.StatusOK {
		t.Fatalf("первый обмен: статус = %d (%s), ожидался 200", first.Code, first.Body.String())
	}

	second := postChannelsTelegramLink(t, router, api.ChannelLinkRequest{Code: code, TelegramUserId: "tg-102"})
	if second.Code != http.StatusConflict {
		t.Fatalf("второй обмен: статус = %d (%s), ожидался 409", second.Code, second.Body.String())
	}
	var got api.Error
	if uerr := json.Unmarshal(second.Body.Bytes(), &got); uerr != nil {
		t.Fatalf("тело ответа не JSON Error: %v", uerr)
	}
	if got.Code != "link_code_used" {
		t.Fatalf("code = %q, ожидался link_code_used", got.Code)
	}

	var n int
	if qerr := pool.QueryRow(ctx, `SELECT count(*) FROM channel_links WHERE external_id = $1`, "tg-102").Scan(&n); qerr != nil {
		t.Fatalf("count channel_links: %v", qerr)
	}
	if n != 1 {
		t.Fatalf("channel_links = %d строк, ожидалась РОВНО 1 (без дубля)", n)
	}
	t.Logf("OK: повторный обмен → 409 link_code_used, дубль не создан")
}
