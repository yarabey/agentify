// Unit-тесты обработчика POST /channels/telegram/link без БД (тикет 10.2, FR
// D3). Проверяют разбор тела, маппинг сентинел-ошибок channel.Linker.Exchange
// на HTTP-статусы/коды ответа и форму успешного ответа через httptest поверх
// собранного роутера с подменённым ChannelLinker (fake). Сценарий с реальной
// БД (валидный код действительно создаёт channel_links, истёкший/использованный
// код не создаёт) — в channels_integration_test.go (тег integration).
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/yarabey/agentify/orchestrator/internal/channel"
	"github.com/yarabey/agentify/orchestrator/internal/db"
)

// fakeChannelLinker — подменный ChannelLinker для unit-тестов
// PostChannelsTelegramLink (тикет 10.2).
type fakeChannelLinker struct {
	result db.ChannelLink
	err    error

	// gotChannel/gotCode/gotExternalID фиксируют аргументы последнего вызова
	// Exchange — тесты проверяют, что хендлер прокидывает channel="telegram" и
	// содержимое тела запроса как есть, без искажений.
	gotChannel    string
	gotCode       string
	gotExternalID string
}

func (f *fakeChannelLinker) Exchange(_ context.Context, channelName, code, externalID string) (db.ChannelLink, error) {
	f.gotChannel = channelName
	f.gotCode = code
	f.gotExternalID = externalID
	if f.err != nil {
		return db.ChannelLink{}, f.err
	}
	return f.result, nil
}

// doLinkTelegram прогоняет тело req через роутер с заданным ChannelLinker и
// возвращает записанный ответ. querier — большинство сценариев этого файла не
// трогают БД напрямую (вся работа — за фейковым ChannelLinker), поэтому пустой
// fakeQuerier{} достаточно.
func doLinkTelegram(t *testing.T, linker ChannelLinker, body any) *httptest.ResponseRecorder {
	t.Helper()
	s := newTestServer(fakeQuerier{})
	s.SetChannelLinker(linker)
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
	req := httptest.NewRequest(http.MethodPost, "/channels/telegram/link", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

// TestPostChannelsTelegramLink_NoAuthRequired — маршрут `security: []`
// (api/openapi.yaml): запрос без Authorization не получает 401 от
// auth-middleware (в отличие от /channels/telegram/link-code, см.
// channels_link_code_test.go TestPostChannelsTelegramLinkCode_RequiresBearerToken)
// — в этой точке пользователь ещё не аутентифицирован как web-клиент (см.
// godoc channels.go).
func TestPostChannelsTelegramLink_NoAuthRequired(t *testing.T) {
	linker := &fakeChannelLinker{result: db.ChannelLink{
		Channel:    channelTelegram,
		ExternalID: "123",
		CreatedAt:  pgtype.Timestamptz{Time: time.Now(), Valid: true},
	}}
	rec := doLinkTelegram(t, linker, ChannelLinkRequest{Code: "abc", TelegramUserId: "123"})
	if rec.Code != http.StatusOK {
		t.Fatalf("статус = %d (%s), ожидался 200 без Authorization", rec.Code, rec.Body.String())
	}
}

// TestPostChannelsTelegramLink_Success — валидный обмен → 200 + тело
// ChannelLink с полями из результата Exchange; Exchange вызван с
// channel="telegram" и содержимым тела запроса (FR D3).
func TestPostChannelsTelegramLink_Success(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	linker := &fakeChannelLinker{result: db.ChannelLink{
		Channel:    channelTelegram,
		ExternalID: "555",
		CreatedAt:  pgtype.Timestamptz{Time: now, Valid: true},
	}}
	rec := doLinkTelegram(t, linker, ChannelLinkRequest{Code: "the-code", TelegramUserId: "555"})
	if rec.Code != http.StatusOK {
		t.Fatalf("статус = %d (%s), ожидался 200", rec.Code, rec.Body.String())
	}
	if linker.gotChannel != channelTelegram || linker.gotCode != "the-code" || linker.gotExternalID != "555" {
		t.Fatalf("Exchange вызван с (%q,%q,%q), ожидалось (%q,%q,%q)",
			linker.gotChannel, linker.gotCode, linker.gotExternalID, channelTelegram, "the-code", "555")
	}

	var got ChannelLink
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("тело ответа не JSON ChannelLink: %v", err)
	}
	if got.Channel == nil || *got.Channel != Telegram {
		t.Fatalf("channel = %v, ожидался %q", got.Channel, Telegram)
	}
	if got.ExternalId == nil || *got.ExternalId != "555" {
		t.Fatalf("external_id = %v, ожидался %q", got.ExternalId, "555")
	}
}

// TestPostChannelsTelegramLink_ValidationErrors — пустые обязательные поля →
// 400, Exchange не вызывается.
func TestPostChannelsTelegramLink_ValidationErrors(t *testing.T) {
	cases := map[string]ChannelLinkRequest{
		"пустой code":             {Code: "", TelegramUserId: "1"},
		"пустой telegram_user_id": {Code: "abc", TelegramUserId: "  "},
	}
	for name, body := range cases {
		linker := &fakeChannelLinker{}
		rec := doLinkTelegram(t, linker, body)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: статус = %d, ожидался 400", name, rec.Code)
		}
		if linker.gotCode != "" || linker.gotExternalID != "" {
			t.Errorf("%s: Exchange вызван несмотря на невалидное тело", name)
		}
	}
}

// TestPostChannelsTelegramLink_MalformedJSON — невалидный JSON → 400.
func TestPostChannelsTelegramLink_MalformedJSON(t *testing.T) {
	rec := doLinkTelegram(t, &fakeChannelLinker{}, []byte("{not json"))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("статус = %d, ожидался 400", rec.Code)
	}
}

// TestPostChannelsTelegramLink_ErrorMapping — сентинел-ошибки
// channel.Linker.Exchange мапятся на ожидаемые HTTP-статус и код ответа (FR
// D3, приёмка тикета 10.2: истёкший код и повторное использование
// отклоняются).
func TestPostChannelsTelegramLink_ErrorMapping(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		wantStatus int
		wantCode   string
	}{
		{"код не найден", channel.ErrLinkCodeNotFound, http.StatusNotFound, "link_code_not_found"},
		{"код истёк", channel.ErrLinkCodeExpired, http.StatusConflict, "link_code_expired"},
		{"код уже использован", channel.ErrLinkCodeUsed, http.StatusConflict, "link_code_used"},
		{"telegram уже привязан к другому", channel.ErrAlreadyLinked, http.StatusConflict, "channel_already_linked"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			linker := &fakeChannelLinker{err: tc.err}
			rec := doLinkTelegram(t, linker, ChannelLinkRequest{Code: "c", TelegramUserId: "1"})
			if rec.Code != tc.wantStatus {
				t.Fatalf("статус = %d, ожидался %d", rec.Code, tc.wantStatus)
			}
			var got Error
			if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
				t.Fatalf("тело ответа не JSON Error: %v", err)
			}
			if got.Code != tc.wantCode {
				t.Fatalf("code = %q, ожидался %q", got.Code, tc.wantCode)
			}
		})
	}
}

// TestPostChannelsTelegramLink_LinkerNotConfigured — channelLinker не
// настроен (SetChannelLinker не вызван, напр. сервис без БД) → 500, не
// паника (тот же принцип, что у отсутствующего transitioner/commandPublisher).
func TestPostChannelsTelegramLink_LinkerNotConfigured(t *testing.T) {
	s := newTestServer(fakeQuerier{})
	router := NewRouter(s)

	raw, _ := json.Marshal(ChannelLinkRequest{Code: "c", TelegramUserId: "1"})
	req := httptest.NewRequest(http.MethodPost, "/channels/telegram/link", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("статус = %d, ожидался 500", rec.Code)
	}
}
