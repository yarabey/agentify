package platform

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestHealthzReturnsOK проверяет приёмку «/healthz отвечает» (тикет 0.3/0.6):
// GET /healthz даёт 200 и тело {"status":"ok","service":"<name>"} с корректным
// content-type. Проверяем через httptest на роутере, без поднятия реального сокета.
func TestHealthzReturnsOK(t *testing.T) {
	router := NewHealthRouter("orchestrator", NewLogger(Config{LogFormat: LogFormatText}, "orchestrator"))

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	res := rec.Result()
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("статус = %d, ожидалось 200", res.StatusCode)
	}
	if ct := res.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, ожидалось application/json", ct)
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("чтение тела: %v", err)
	}

	var got healthResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("тело не парсится как JSON: %v (тело: %s)", err, body)
	}
	if got.Status != "ok" {
		t.Errorf("status = %q, ожидалось ok", got.Status)
	}
	if got.Service != "orchestrator" {
		t.Errorf("service = %q, ожидалось orchestrator", got.Service)
	}
}

// TestHealthzUnknownPath проверяет, что роутер каркаса не отвечает 200 на
// посторонние пути (только /healthz обслуживается на этом этапе).
func TestHealthzUnknownPath(t *testing.T) {
	router := NewHealthRouter("bot", NewLogger(Config{LogFormat: LogFormatText}, "bot"))

	req := httptest.NewRequest(http.MethodGet, "/nope", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Result().StatusCode == http.StatusOK {
		t.Error("посторонний путь вернул 200, ожидалось 404")
	}
}
