package orchestrator

// Unit-тесты Client.LinkTelegram (тикет 10.2, FR D3) через httptest.Server —
// проверяют форму запроса и маппинг тела ответа Error.code на сентинелы
// пакета, без реального оркестратора.
import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestLinkTelegram_Success — 200 → nil, запрос содержит корректный путь и
// тело {code, telegram_user_id}.
func TestLinkTelegram_Success(t *testing.T) {
	var gotPath string
	var gotBody linkTelegramRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		if derr := json.NewDecoder(r.Body).Decode(&gotBody); derr != nil {
			t.Errorf("decode тела: %v", derr)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	if err := c.LinkTelegram(context.Background(), "the-code", "555"); err != nil {
		t.Fatalf("LinkTelegram: %v, ожидался успех", err)
	}
	if gotPath != "/channels/telegram/link" {
		t.Fatalf("путь = %q, ожидался /channels/telegram/link", gotPath)
	}
	if gotBody.Code != "the-code" || gotBody.TelegramUserID != "555" {
		t.Fatalf("тело запроса = %+v, ожидалось code=the-code telegram_user_id=555", gotBody)
	}
}

// TestLinkTelegram_ErrorMapping — коды ответа Error.code мапятся на
// ожидаемые сентинелы пакета (тикет 10.2, FR D3).
func TestLinkTelegram_ErrorMapping(t *testing.T) {
	cases := []struct {
		status int
		code   string
		want   error
	}{
		{http.StatusNotFound, "link_code_not_found", ErrLinkCodeNotFound},
		{http.StatusConflict, "link_code_expired", ErrLinkCodeExpired},
		{http.StatusConflict, "link_code_used", ErrLinkCodeUsed},
		{http.StatusConflict, "channel_already_linked", ErrAlreadyLinked},
	}
	for _, tc := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(tc.status)
			_ = json.NewEncoder(w).Encode(errorBody{Code: tc.code, Message: "тестовая ошибка"})
		}))

		c := NewClient(srv.URL)
		err := c.LinkTelegram(context.Background(), "c", "1")
		srv.Close()

		if !errors.Is(err, tc.want) {
			t.Errorf("code=%q: err = %v, ожидался %v", tc.code, err, tc.want)
		}
	}
}

// TestLinkTelegram_UnknownErrorCode — неизвестный code в теле ответа → ошибка
// с сообщением, но не паника и не один из известных сентинелов.
func TestLinkTelegram_UnknownErrorCode(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(errorBody{Code: "internal", Message: "внутренняя ошибка"})
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	err := c.LinkTelegram(context.Background(), "c", "1")
	if err == nil {
		t.Fatal("ожидалась ошибка")
	}
	for _, sentinel := range []error{ErrLinkCodeNotFound, ErrLinkCodeExpired, ErrLinkCodeUsed, ErrAlreadyLinked} {
		if errors.Is(err, sentinel) {
			t.Fatalf("err = %v неожиданно совпал с сентинелом %v", err, sentinel)
		}
	}
}

// TestLinkTelegram_BaseURLTrailingSlash — завершающий `/` в baseURL
// обрезается (путь не задваивается).
func TestLinkTelegram_BaseURLTrailingSlash(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := NewClient(srv.URL + "/")
	if err := c.LinkTelegram(context.Background(), "c", "1"); err != nil {
		t.Fatalf("LinkTelegram: %v", err)
	}
	if gotPath != "/channels/telegram/link" {
		t.Fatalf("путь = %q, ожидался /channels/telegram/link (без двойного слэша)", gotPath)
	}
}
