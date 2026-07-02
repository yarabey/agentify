// Unit-тесты обработчика POST /channels/telegram/token без БД (тикет 10.3, FR
// D1). Проверяют разбор тела, проверку сервисного секрета
// (X-Bot-Service-Secret) и маппинг результатов на HTTP-статусы/коды ответа
// через httptest поверх собранного роутера с подменённым слоем данных (fake
// Querier). Сценарий с реальной БД (валидная привязка действительно
// резолвится в user_id) — в channels_token_integration_test.go (тег
// integration).
package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/yarabey/agentify/internal/auth"
	"github.com/yarabey/agentify/orchestrator/internal/db"
)

const testBotServiceSecret = "unit-test-bot-service-secret"

// doTelegramToken прогоняет тело req через роутер POST /channels/telegram/token
// с заданными Querier/секретом бота/заголовком X-Bot-Service-Secret и
// возвращает записанный ответ.
func doTelegramToken(t *testing.T, q Querier, configuredSecret, headerSecret string, body any) *httptest.ResponseRecorder {
	t.Helper()
	s := newTestServer(q)
	if configuredSecret != "" {
		s.SetBotServiceSecret([]byte(configuredSecret))
	}
	router := NewRouter(s)

	var raw []byte
	switch v := body.(type) {
	case []byte:
		raw = v
	default:
		var err error
		raw, err = json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal тела: %v", err)
		}
	}
	req := httptest.NewRequest(http.MethodPost, "/channels/telegram/token", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	if headerSecret != "" {
		req.Header.Set("X-Bot-Service-Secret", headerSecret)
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

// TestPostChannelsTelegramToken_NoUserBearerRequired — маршрут `security: []`
// (api/openapi.yaml): запрос без Authorization не отклоняется auth-middleware
// (ровно как /channels/telegram/link, тикет 10.2) — у этого эндпоинта своя,
// отдельная от bearerAuth, схема доверия (сервисный секрет), см. годок
// PostChannelsTelegramToken.
func TestPostChannelsTelegramToken_NoUserBearerRequired(t *testing.T) {
	userID := uuid.New()
	q := fakeQuerier{getChannelLinkByChannelAndExternalIDResult: db.ChannelLink{
		UserID: pgtype.UUID{Bytes: userID, Valid: true},
	}}
	rec := doTelegramToken(t, q, testBotServiceSecret, testBotServiceSecret, TelegramActingTokenRequest{TelegramUserId: "123"})
	if rec.Code != http.StatusOK {
		t.Fatalf("статус = %d (%s), ожидался 200 без Authorization", rec.Code, rec.Body.String())
	}
}

// TestPostChannelsTelegramToken_Success — валидный секрет и привязанный
// telegram_user_id → 200 с access_token, который auth.ParseAccessToken
// разбирает обратно в user_id владельца привязки (ChannelLink.UserID), и
// expires_at примерно now+AccessTokenTTL (FR D1, приёмка: acting-токен того
// же формата/TTL, что и обычный access-JWT).
func TestPostChannelsTelegramToken_Success(t *testing.T) {
	userID := uuid.New()
	q := fakeQuerier{getChannelLinkByChannelAndExternalIDResult: db.ChannelLink{
		UserID: pgtype.UUID{Bytes: userID, Valid: true},
	}}
	rec := doTelegramToken(t, q, testBotServiceSecret, testBotServiceSecret, TelegramActingTokenRequest{TelegramUserId: "555"})
	if rec.Code != http.StatusOK {
		t.Fatalf("статус = %d (%s), ожидался 200", rec.Code, rec.Body.String())
	}

	var got TelegramActingToken
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("тело ответа не JSON TelegramActingToken: %v", err)
	}
	if got.AccessToken == nil || *got.AccessToken == "" {
		t.Fatalf("access_token пуст")
	}
	gotUserID, err := auth.ParseAccessToken(*got.AccessToken, []byte(testJWTSigningKey))
	if err != nil {
		t.Fatalf("ParseAccessToken(access_token): %v", err)
	}
	if gotUserID != userID {
		t.Fatalf("sub access_token = %s, ожидался %s (user_id привязки)", gotUserID, userID)
	}
	if got.ExpiresAt == nil {
		t.Fatal("expires_at пуст")
	}
	wantExpiry := time.Now().Add(auth.AccessTokenTTL)
	if diff := wantExpiry.Sub(*got.ExpiresAt); diff < -time.Minute || diff > time.Minute {
		t.Fatalf("expires_at = %s, ожидался ~%s (±1m)", got.ExpiresAt, wantExpiry)
	}
}

// TestPostChannelsTelegramToken_WrongServiceSecret — заголовок присутствует,
// но не совпадает с настроенным секретом → 401, GetChannelLinkByChannelAndExternalID
// не должен вызываться (секрет проверяется ДО резолва привязки — атакующий
// без верного секрета не должен иметь возможность даже провести probing по
// telegram_user_id).
func TestPostChannelsTelegramToken_WrongServiceSecret(t *testing.T) {
	q := fakeQuerier{getChannelLinkByChannelAndExternalIDErr: pgx.ErrNoRows}
	rec := doTelegramToken(t, q, testBotServiceSecret, "wrong-secret", TelegramActingTokenRequest{TelegramUserId: "1"})
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("статус = %d, ожидался 401", rec.Code)
	}
}

// TestPostChannelsTelegramToken_MissingServiceSecretHeader — заголовок
// X-Bot-Service-Secret вовсе отсутствует → 400 (contract: `required: true` у
// параметра, api/openapi.yaml) — ошибка отдаёт сгенерированный
// ServerInterfaceWrapper ДО вызова обработчика (тот же генерируемый механизм,
// что и у обязательного Idempotency-Key в POST /tasks), а не сам обработчик.
func TestPostChannelsTelegramToken_MissingServiceSecretHeader(t *testing.T) {
	rec := doTelegramToken(t, fakeQuerier{}, testBotServiceSecret, "", TelegramActingTokenRequest{TelegramUserId: "1"})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("статус = %d, ожидался 400 (заголовок отсутствует)", rec.Code)
	}
}

// TestPostChannelsTelegramToken_ServiceSecretNotConfigured — пустой
// сконфигурированный секрет (ORCH_BOT_SERVICE_SECRET не задан) → эндпоинт
// ВСЕГДА отвечает 401, даже если presented-заголовок непуст (fail closed, см.
// годок SetBotServiceSecret про принцип «мягкое выключение через отказ», а не
// «любой заголовок совпадает с пустым секретом»).
func TestPostChannelsTelegramToken_ServiceSecretNotConfigured(t *testing.T) {
	rec := doTelegramToken(t, fakeQuerier{}, "", "any-value", TelegramActingTokenRequest{TelegramUserId: "1"})
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("статус = %d, ожидался 401 (секрет не настроен)", rec.Code)
	}
}

// TestPostChannelsTelegramToken_NotLinked — валидный секрет, но
// telegram_user_id не привязан ни к одному аккаунту (GetChannelLinkByChannelAndExternalID
// → pgx.ErrNoRows) → 404 not_linked (бот транслирует это в подсказку
// `/start <code>`, см. bot/internal/orchestrator.ErrNotLinked).
func TestPostChannelsTelegramToken_NotLinked(t *testing.T) {
	q := fakeQuerier{getChannelLinkByChannelAndExternalIDErr: pgx.ErrNoRows}
	rec := doTelegramToken(t, q, testBotServiceSecret, testBotServiceSecret, TelegramActingTokenRequest{TelegramUserId: "999"})
	if rec.Code != http.StatusNotFound {
		t.Fatalf("статус = %d (%s), ожидался 404", rec.Code, rec.Body.String())
	}
	var got Error
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("тело ответа не JSON Error: %v", err)
	}
	if got.Code != "not_linked" {
		t.Fatalf("code = %q, ожидался %q", got.Code, "not_linked")
	}
}

// TestPostChannelsTelegramToken_ValidationErrors — пустой telegram_user_id →
// 400.
func TestPostChannelsTelegramToken_ValidationErrors(t *testing.T) {
	rec := doTelegramToken(t, fakeQuerier{}, testBotServiceSecret, testBotServiceSecret, TelegramActingTokenRequest{TelegramUserId: "  "})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("статус = %d, ожидался 400", rec.Code)
	}
}

// TestPostChannelsTelegramToken_MalformedJSON — невалидный JSON → 400.
func TestPostChannelsTelegramToken_MalformedJSON(t *testing.T) {
	rec := doTelegramToken(t, fakeQuerier{}, testBotServiceSecret, testBotServiceSecret, []byte("{not json"))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("статус = %d, ожидался 400", rec.Code)
	}
}
