package webhook

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	tele "gopkg.in/telebot.v3"
)

// recordingProcessor — фейковый UpdateProcessor: запоминает переданные апдейты,
// чтобы тест проверил приём без реального *tele.Bot, токена и сети (FR D1).
type recordingProcessor struct {
	got []tele.Update
}

func (p *recordingProcessor) ProcessUpdate(u tele.Update) {
	p.got = append(p.got, u)
}

// validUpdate — минимальный, но валидный Telegram-апдейт (message с текстом),
// в формате, который присылает Bot API webhook'ом.
const validUpdate = `{"update_id":123456789,"message":{"message_id":1,"chat":{"id":42,"type":"private"},"text":"/start"}}`

// TestServeHTTP_AcceptsValidUpdate — приёмка FR D1: webhook-handler принимает
// валидный Telegram-апдейт (httptest, без реальной сети), отвечает 200 и
// передаёт апдейт в маршрутизатор.
func TestServeHTTP_AcceptsValidUpdate(t *testing.T) {
	const secret = "s3cr3t"
	proc := &recordingProcessor{}
	h := NewHandler(proc, secret, nil)

	req := httptest.NewRequest(http.MethodPost, Path(secret), strings.NewReader(validUpdate))
	req.Header.Set(secretTokenHeader, secret)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("статус = %d, ожидался %d; тело: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if len(proc.got) != 1 {
		t.Fatalf("ProcessUpdate вызван %d раз(а), ожидался 1", len(proc.got))
	}
	if proc.got[0].ID != 123456789 {
		t.Fatalf("update_id = %d, ожидался 123456789", proc.got[0].ID)
	}
	if proc.got[0].Message == nil || proc.got[0].Message.Text != "/start" {
		t.Fatalf("апдейт декодирован неверно: %+v", proc.got[0])
	}
}

// TestServeHTTP_RejectsWrongSecretToken — запрос с неверным secret_token
// отклоняется (403) и не доходит до маршрутизатора: секрет отсекает запросы не
// от Telegram.
func TestServeHTTP_RejectsWrongSecretToken(t *testing.T) {
	proc := &recordingProcessor{}
	h := NewHandler(proc, "expected", nil)

	req := httptest.NewRequest(http.MethodPost, Path("expected"), strings.NewReader(validUpdate))
	req.Header.Set(secretTokenHeader, "wrong")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("статус = %d, ожидался %d", rec.Code, http.StatusForbidden)
	}
	if len(proc.got) != 0 {
		t.Fatalf("ProcessUpdate не должен вызываться при неверном secret_token, вызван %d раз(а)", len(proc.got))
	}
}

// TestServeHTTP_EmptySecretSkipsHeaderCheck — при пустом secretToken проверка
// заголовка отключена (защита на уровне секретного пути): валидный апдейт без
// заголовка принимается.
func TestServeHTTP_EmptySecretSkipsHeaderCheck(t *testing.T) {
	proc := &recordingProcessor{}
	h := NewHandler(proc, "", nil)

	req := httptest.NewRequest(http.MethodPost, Path(""), strings.NewReader(validUpdate))
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("статус = %d, ожидался %d", rec.Code, http.StatusOK)
	}
	if len(proc.got) != 1 {
		t.Fatalf("ProcessUpdate вызван %d раз(а), ожидался 1", len(proc.got))
	}
}

// TestServeHTTP_RejectsNonPost — Telegram доставляет апдейты только POST'ом;
// прочие методы получают 405 и до маршрутизатора не доходят.
func TestServeHTTP_RejectsNonPost(t *testing.T) {
	proc := &recordingProcessor{}
	h := NewHandler(proc, "", nil)

	req := httptest.NewRequest(http.MethodGet, Path(""), nil)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("статус = %d, ожидался %d", rec.Code, http.StatusMethodNotAllowed)
	}
	if len(proc.got) != 0 {
		t.Fatalf("ProcessUpdate не должен вызываться для GET, вызван %d раз(а)", len(proc.got))
	}
}

// TestServeHTTP_RejectsInvalidJSON — некорректное тело → 400, апдейт не
// маршрутизируется.
func TestServeHTTP_RejectsInvalidJSON(t *testing.T) {
	proc := &recordingProcessor{}
	h := NewHandler(proc, "", nil)

	req := httptest.NewRequest(http.MethodPost, Path(""), strings.NewReader("{not json"))
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("статус = %d, ожидался %d", rec.Code, http.StatusBadRequest)
	}
	if len(proc.got) != 0 {
		t.Fatalf("ProcessUpdate не должен вызываться при битом JSON, вызван %d раз(а)", len(proc.got))
	}
}

// TestPath — секрет включается в путь, префикс фиксирован.
func TestPath(t *testing.T) {
	if got := Path("abc"); got != "/webhook/abc" {
		t.Fatalf("Path(\"abc\") = %q, ожидался \"/webhook/abc\"", got)
	}
}
