// Unit-тесты обработчика POST /channels/telegram/link-code без БД (тикет 9.6,
// FR A2, D3). Проверяют требование Bearer-токена, форму успешного ответа
// (201 + code/expires_at), аргументы, с которыми вызывается
// ChannelLinkCodeIssuer, и маппинг ошибки issuer'а на 500 — через httptest
// поверх собранного роутера с подменённым ChannelLinkCodeIssuer (fake).
// Сценарий с реальной БД (код действительно вставляется с корректным TTL) —
// в channels_link_code_integration_test.go (тег integration).
package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/yarabey/agentify/orchestrator/internal/db"
)

// fakeChannelLinkCodeIssuer — подменный ChannelLinkCodeIssuer для
// unit-тестов PostChannelsTelegramLinkCode (тикет 9.6).
type fakeChannelLinkCodeIssuer struct {
	result db.ChannelLinkCode
	err    error

	// gotUserID/gotChannel фиксируют аргументы последнего вызова
	// IssueLinkCode — тесты проверяют, что хендлер прокидывает РОВНО
	// userID из access-токена (не из тела запроса — его там и нет) и
	// channel="telegram".
	gotUserID  pgtype.UUID
	gotChannel string
}

func (f *fakeChannelLinkCodeIssuer) IssueLinkCode(_ context.Context, userID pgtype.UUID, channelName string) (db.ChannelLinkCode, error) {
	f.gotUserID = userID
	f.gotChannel = channelName
	if f.err != nil {
		return db.ChannelLinkCode{}, f.err
	}
	return f.result, nil
}

// doLinkCode прогоняет POST /channels/telegram/link-code через роутер с
// заданным ChannelLinkCodeIssuer и (опционально) Bearer-токеном userID и
// возвращает записанный ответ.
func doLinkCode(t *testing.T, issuer ChannelLinkCodeIssuer, userID *uuid.UUID) *httptest.ResponseRecorder {
	t.Helper()
	s := newTestServer(fakeQuerier{})
	s.SetChannelLinkCodeIssuer(issuer)
	router := NewRouter(s)

	req := httptest.NewRequest(http.MethodPost, "/channels/telegram/link-code", nil)
	if userID != nil {
		req.Header.Set("Authorization", "Bearer "+issueTestAccessToken(t, *userID, time.Now()))
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

// TestPostChannelsTelegramLinkCode_RequiresBearerToken — без Authorization —
// 401 от auth-middleware, не доходя до обработчика (в отличие от
// /channels/telegram/link, у этой операции нет `security: []`, тикет 1.4).
func TestPostChannelsTelegramLinkCode_RequiresBearerToken(t *testing.T) {
	rec := doLinkCode(t, &fakeChannelLinkCodeIssuer{}, nil)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("статус = %d (%s), ожидался 401", rec.Code, rec.Body.String())
	}
	assertErrorBody(t, rec)
}

// TestPostChannelsTelegramLinkCode_Success — валидный Bearer → 201 + тело
// {code, expires_at}; IssueLinkCode вызывается с userID из токена и
// channel="telegram" (FR A2 — код выпускается СТРОГО для себя, D3).
func TestPostChannelsTelegramLinkCode_Success(t *testing.T) {
	userID := uuid.New()
	expires := time.Now().Add(15 * time.Minute).Truncate(time.Second)
	issuer := &fakeChannelLinkCodeIssuer{result: db.ChannelLinkCode{
		Code:      "ABCD2345",
		UserID:    pgtype.UUID{Bytes: userID, Valid: true},
		Channel:   "telegram",
		ExpiresAt: pgtype.Timestamptz{Time: expires, Valid: true},
	}}

	rec := doLinkCode(t, issuer, &userID)
	if rec.Code != http.StatusCreated {
		t.Fatalf("статус = %d (%s), ожидался 201", rec.Code, rec.Body.String())
	}

	var body struct {
		Code      string    `json:"code"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal тела: %v", err)
	}
	if body.Code != "ABCD2345" {
		t.Fatalf("code = %q, ожидался ABCD2345", body.Code)
	}
	if !body.ExpiresAt.Equal(expires) {
		t.Fatalf("expires_at = %v, ожидался %v", body.ExpiresAt, expires)
	}

	if issuer.gotUserID.Bytes != userID {
		t.Fatalf("IssueLinkCode вызван с userID = %v, ожидался %v", issuer.gotUserID, userID)
	}
	if issuer.gotChannel != channelTelegram {
		t.Fatalf("IssueLinkCode вызван с channel = %q, ожидался %q", issuer.gotChannel, channelTelegram)
	}
}

// TestPostChannelsTelegramLinkCode_IssuerErrorIsServerError — сбой issuer'а
// (например, БД недоступна) → 500, не штатный пользовательский код ответа.
func TestPostChannelsTelegramLinkCode_IssuerErrorIsServerError(t *testing.T) {
	userID := uuid.New()
	issuer := &fakeChannelLinkCodeIssuer{err: errors.New("boom")}

	rec := doLinkCode(t, issuer, &userID)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("статус = %d (%s), ожидался 500", rec.Code, rec.Body.String())
	}
	assertErrorBody(t, rec)
}

// TestPostChannelsTelegramLinkCode_NoIssuerConfiguredIsServerError —
// channelLinkCodeIssuer не зарегистрирован (инвариант-сбой инициализации,
// тот же принцип, что у отсутствующего channelLinker) → 500, не паника.
func TestPostChannelsTelegramLinkCode_NoIssuerConfiguredIsServerError(t *testing.T) {
	userID := uuid.New()
	rec := doLinkCode(t, nil, &userID)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("статус = %d (%s), ожидался 500", rec.Code, rec.Body.String())
	}
	assertErrorBody(t, rec)
}
