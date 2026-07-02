package platform

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestMetricsEndpointExposesPrometheusFormat проверяет приёмку тикета 11.4
// («метрики экспонируются» — GET /metrics отдаёт валидный Prometheus-формат с
// ожидаемыми метриками): поднимаем реальный Service.Run на эфемерном порту,
// делаем несколько запросов к /healthz (чтобы счётчик HTTP-запросов был
// ненулевым), затем читаем /metrics и проверяем и формат (Prometheus text
// exposition — HELP/TYPE-строки), и присутствие наших метрик с корректными
// лейблами.
func TestMetricsEndpointExposesPrometheusFormat(t *testing.T) {
	cfg := freeConfig()
	cfg.HealthAddr = freeAddr(t)

	svc, err := NewService("orchestrator", cfg)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	healthzURL := fmt.Sprintf("http://%s/healthz", cfg.HealthAddr)
	waitUntilServing(t, healthzURL)

	// Несколько запросов к /healthz — чтобы agentify_http_requests_total по
	// пути /healthz был гарантированно ненулевым к моменту чтения /metrics.
	for i := 0; i < 3; i++ {
		resp, err := http.Get(healthzURL) //nolint:gosec // эфемерный loopback-адрес теста
		if err != nil {
			t.Fatalf("GET %s: %v", healthzURL, err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}

	metricsURL := fmt.Sprintf("http://%s/metrics", cfg.HealthAddr)
	// Первый запрос к /metrics ещё не мог сам себя учесть (метрика
	// записывается ПОСЛЕ ответа) — делаем его дважды, второй ответ должен уже
	// содержать запись о первом (доказательство, что middleware инструментирует
	// и сам /metrics, а не только смонтированный router).
	firstResp, err := http.Get(metricsURL) //nolint:gosec // эфемерный loopback-адрес теста
	if err != nil {
		t.Fatalf("GET %s (первый раз): %v", metricsURL, err)
	}
	_, _ = io.Copy(io.Discard, firstResp.Body)
	_ = firstResp.Body.Close()

	resp, err := http.Get(metricsURL) //nolint:gosec // эфемерный loopback-адрес теста
	if err != nil {
		t.Fatalf("GET %s: %v", metricsURL, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("статус /metrics = %d, ожидалось 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("Content-Type /metrics = %q, ожидался text/plain (Prometheus exposition format)", ct)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("чтение тела /metrics: %v", err)
	}
	text := string(body)

	// Валидный Prometheus text exposition format начинается с HELP/TYPE-строк
	// перед значением метрики.
	if !strings.Contains(text, "# HELP agentify_http_requests_total") {
		t.Error("/metrics не содержит HELP-строку agentify_http_requests_total — формат не похож на Prometheus exposition")
	}
	if !strings.Contains(text, "# TYPE agentify_http_requests_total counter") {
		t.Error("/metrics не содержит TYPE-строку agentify_http_requests_total counter")
	}
	if !strings.Contains(text, `agentify_http_requests_total{method="GET",path="/healthz",service="orchestrator",status="200"}`) {
		t.Errorf("/metrics не содержит счётчик запросов /healthz с ожидаемыми лейблами, тело:\n%s", text)
	}
	if !strings.Contains(text, "# HELP agentify_http_request_duration_seconds") {
		t.Error("/metrics не содержит HELP-строку agentify_http_request_duration_seconds")
	}
	if !strings.Contains(text, "agentify_http_request_duration_seconds_bucket") {
		t.Error("/metrics не содержит бакеты гистограммы agentify_http_request_duration_seconds")
	}
	// Стандартные Go-коллекторы (go_goroutines и т.п.) — доказательство, что
	// registry.MustRegister(collectors.NewGoCollector()) реально сработал.
	if !strings.Contains(text, "go_goroutines") {
		t.Error("/metrics не содержит стандартную метрику go_goroutines — Go-коллектор не зарегистрирован")
	}
	// /metrics — не подставная заглушка вне маршрутизации chi: сам себя
	// middleware тоже инструментирует.
	if !strings.Contains(text, `path="/metrics"`) {
		t.Error("/metrics не инструментирован middleware (нет записи с path=\"/metrics\")")
	}
}

// TestMetricsSurviveSetHandler проверяет, что GET /metrics остаётся доступен
// и после Service.SetHandler (тикеты 1.2+ заменяют router целиком на
// сгенерированный из openapi) — /metrics не завязан на конкретный router,
// смонтирован снаружи в Service.Run (см. Metrics.wrapRouter).
func TestMetricsSurviveSetHandler(t *testing.T) {
	cfg := freeConfig()
	cfg.HealthAddr = freeAddr(t)

	svc, err := NewService("bot", cfg)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	// Router без /metrics и без /healthz вовсе — имитация SetHandler(...)
	// произвольным сгенерированным роутером конкретного сервиса.
	replacement := NewHealthRouter("bot", svc.Logger())
	svc.SetHandler(replacement)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	metricsURL := fmt.Sprintf("http://%s/metrics", cfg.HealthAddr)
	waitUntilServing(t, metricsURL)

	resp, err := http.Get(metricsURL) //nolint:gosec // эфемерный loopback-адрес теста
	if err != nil {
		t.Fatalf("GET %s: %v", metricsURL, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("статус /metrics после SetHandler = %d, ожидалось 200", resp.StatusCode)
	}
}

// waitUntilServing опрашивает url до первого успешного ответа (сервер ещё
// поднимается в отдельной горутине) — тот же приём, что и в
// TestRunServesHealthzWhileRunning (service_test.go).
func waitUntilServing(t *testing.T, url string) {
	t.Helper()
	var lastErr error
	for i := 0; i < 20; i++ {
		resp, err := http.Get(url) //nolint:gosec // эфемерный loopback-адрес теста в тестах пакета
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			return
		}
		lastErr = err
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("сервер не поднялся вовремя: GET %s: %v", url, lastErr)
}
