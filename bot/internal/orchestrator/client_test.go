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
	"time"
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

	c := NewClient(srv.URL, "")
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

		c := NewClient(srv.URL, "")
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

	c := NewClient(srv.URL, "")
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

	c := NewClient(srv.URL+"/", "")
	if err := c.LinkTelegram(context.Background(), "c", "1"); err != nil {
		t.Fatalf("LinkTelegram: %v", err)
	}
	if gotPath != "/channels/telegram/link" {
		t.Fatalf("путь = %q, ожидался /channels/telegram/link (без двойного слэша)", gotPath)
	}
}

// --- GetActingToken (тикет 10.3) ------------------------------------------

// TestGetActingToken_Success — 200 → access_token из тела, запрос содержит
// корректный путь, тело {telegram_user_id} и заголовок X-Bot-Service-Secret.
func TestGetActingToken_Success(t *testing.T) {
	var gotPath, gotSecret string
	var gotBody actingTokenRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotSecret = r.Header.Get("X-Bot-Service-Secret")
		if derr := json.NewDecoder(r.Body).Decode(&gotBody); derr != nil {
			t.Errorf("decode тела: %v", derr)
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(actingTokenResponse{AccessToken: "the-token", ExpiresAt: time.Now()})
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "the-secret")
	token, err := c.GetActingToken(context.Background(), "555")
	if err != nil {
		t.Fatalf("GetActingToken: %v, ожидался успех", err)
	}
	if token != "the-token" {
		t.Fatalf("token = %q, ожидался the-token", token)
	}
	if gotPath != "/channels/telegram/token" {
		t.Fatalf("путь = %q, ожидался /channels/telegram/token", gotPath)
	}
	if gotSecret != "the-secret" {
		t.Fatalf("X-Bot-Service-Secret = %q, ожидался the-secret", gotSecret)
	}
	if gotBody.TelegramUserID != "555" {
		t.Fatalf("telegram_user_id = %q, ожидался 555", gotBody.TelegramUserID)
	}
}

// TestGetActingToken_NotLinked — 404 not_linked → ErrNotLinked.
func TestGetActingToken_NotLinked(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(errorBody{Code: "not_linked", Message: "не привязан"})
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "secret")
	_, err := c.GetActingToken(context.Background(), "999")
	if !errors.Is(err, ErrNotLinked) {
		t.Fatalf("err = %v, ожидался ErrNotLinked", err)
	}
}

// TestGetActingToken_Unauthorized — 401 (неверный/не настроенный сервисный
// секрет) → ошибка, но НЕ один из бизнес-сентинелов (ErrNotLinked и т.п.) —
// вызывающая сторона (bot/task.go) отвечает общим текстом.
func TestGetActingToken_Unauthorized(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(errorBody{Code: "unauthorized", Message: "требуется валидный токен доступа"})
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "wrong-secret")
	_, err := c.GetActingToken(context.Background(), "1")
	if err == nil {
		t.Fatal("ожидалась ошибка")
	}
	if errors.Is(err, ErrNotLinked) {
		t.Fatal("401 не должен мапиться на ErrNotLinked")
	}
}

// --- ListIntegrations/CreateTask/AnswerTask/CancelTask (тикет 10.3) -------

// TestListIntegrations_Success — 200 → список интеграций, запрос содержит
// корректный путь и Bearer-заголовок.
func TestListIntegrations_Success(t *testing.T) {
	var gotPath, gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode([]Integration{{ID: "int-1", Name: "laptop"}})
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "")
	integrations, err := c.ListIntegrations(context.Background(), "the-token")
	if err != nil {
		t.Fatalf("ListIntegrations: %v", err)
	}
	if gotPath != "/integrations" {
		t.Fatalf("путь = %q, ожидался /integrations", gotPath)
	}
	if gotAuth != "Bearer the-token" {
		t.Fatalf("Authorization = %q, ожидался Bearer the-token", gotAuth)
	}
	if len(integrations) != 1 || integrations[0].ID != "int-1" {
		t.Fatalf("integrations = %+v, ожидался один элемент int-1", integrations)
	}
}

// TestCreateTask_Success201And200 — оба успешных статуса (201 новая задача,
// 200 идемпотентный повтор, тикет 5.5) декодируются одинаково; запрос
// содержит корректные путь/тело/заголовки (Bearer + Idempotency-Key).
func TestCreateTask_Success201And200(t *testing.T) {
	for _, status := range []int{http.StatusCreated, http.StatusOK} {
		var gotPath, gotAuth, gotIdemp string
		var gotBody taskCreateRequest
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotPath = r.URL.Path
			gotAuth = r.Header.Get("Authorization")
			gotIdemp = r.Header.Get("Idempotency-Key")
			if derr := json.NewDecoder(r.Body).Decode(&gotBody); derr != nil {
				t.Errorf("decode тела: %v", derr)
			}
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(Task{ID: "task-1", Status: "queued"})
		}))

		c := NewClient(srv.URL, "")
		task, err := c.CreateTask(context.Background(), "the-token", "int-1", "сделай дело", "idem-1")
		srv.Close()
		if err != nil {
			t.Fatalf("статус %d: CreateTask: %v", status, err)
		}
		if task.ID != "task-1" {
			t.Fatalf("статус %d: task.ID = %q, ожидался task-1", status, task.ID)
		}
		if gotPath != "/tasks" {
			t.Fatalf("статус %d: путь = %q, ожидался /tasks", status, gotPath)
		}
		if gotAuth != "Bearer the-token" {
			t.Fatalf("статус %d: Authorization = %q, ожидался Bearer the-token", status, gotAuth)
		}
		if gotIdemp != "idem-1" {
			t.Fatalf("статус %d: Idempotency-Key = %q, ожидался idem-1", status, gotIdemp)
		}
		if gotBody.IntegrationID != "int-1" || gotBody.Text != "сделай дело" {
			t.Fatalf("статус %d: тело = %+v, ожидалось integration_id=int-1 text=сделай дело", status, gotBody)
		}
	}
}

// TestCreateTask_NotFound — 404 not_found (чужая/несуществующая интеграция)
// → ErrNotFound.
func TestCreateTask_NotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(errorBody{Code: "not_found", Message: "интеграция не найдена"})
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "")
	_, err := c.CreateTask(context.Background(), "token", "int-x", "text", "idem")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, ожидался ErrNotFound", err)
	}
}

// TestAnswerTask_Success — 202 → nil, запрос содержит корректные путь/тело.
func TestAnswerTask_Success(t *testing.T) {
	var gotPath string
	var gotBody answerRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		if derr := json.NewDecoder(r.Body).Decode(&gotBody); derr != nil {
			t.Errorf("decode тела: %v", derr)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "")
	if err := c.AnswerTask(context.Background(), "token", "task-1", "q-1", "мой ответ"); err != nil {
		t.Fatalf("AnswerTask: %v", err)
	}
	if gotPath != "/tasks/task-1/answer" {
		t.Fatalf("путь = %q, ожидался /tasks/task-1/answer", gotPath)
	}
	if gotBody.QuestionID != "q-1" || gotBody.Text != "мой ответ" {
		t.Fatalf("тело = %+v, ожидалось question_id=q-1 text=мой ответ", gotBody)
	}
}

// TestAnswerTask_NotFound — 404 not_found → ErrNotFound.
func TestAnswerTask_NotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(errorBody{Code: "not_found", Message: "вопрос не найден"})
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "")
	err := c.AnswerTask(context.Background(), "token", "task-1", "q-x", "ответ")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, ожидался ErrNotFound", err)
	}
}

// TestCancelTask_Success — 202 → nil, запрос содержит корректный путь.
func TestCancelTask_Success(t *testing.T) {
	var gotPath, gotMethod string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotMethod = r.Method
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "")
	if err := c.CancelTask(context.Background(), "token", "task-1"); err != nil {
		t.Fatalf("CancelTask: %v", err)
	}
	if gotPath != "/tasks/task-1/cancel" {
		t.Fatalf("путь = %q, ожидался /tasks/task-1/cancel", gotPath)
	}
	if gotMethod != http.MethodPost {
		t.Fatalf("метод = %q, ожидался POST", gotMethod)
	}
}

// TestCancelTask_NotFound — 404 not_found → ErrNotFound.
func TestCancelTask_NotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(errorBody{Code: "not_found", Message: "задача не найдена"})
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "")
	err := c.CancelTask(context.Background(), "token", "task-x")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, ожидался ErrNotFound", err)
	}
}
