package platform

import (
	"context"
	"fmt"
	"net/http"
	"syscall"
	"testing"
	"time"
)

// freeConfig возвращает базовый конфиг с эфемерным портом (:0), чтобы тесты не
// конфликтовали за фиксированный адрес и могли идти параллельно/повторно.
func freeConfig() Config {
	return Config{
		Env:             EnvDev,
		LogLevel:        "error", // тише в выводе тестов
		LogFormat:       LogFormatText,
		HealthAddr:      "127.0.0.1:0",
		ShutdownTimeout: 2 * time.Second,
	}
}

// TestRunGracefulShutdownOnContextCancel проверяет ключевую приёмку тикета 0.6:
// каркас стартует и корректно гасится. Здесь путь shutdown инициируется отменой
// контекста (программный аналог сигнала); Run обязан вернуться без ошибки в
// разумный таймаут.
func TestRunGracefulShutdownOnContextCancel(t *testing.T) {
	svc, err := NewService("orchestrator", freeConfig())
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()

	// Дать серверу подняться, затем инициировать остановку через контекст.
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run вернул ошибку при graceful shutdown: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run не вернулся в таймаут после отмены контекста — graceful shutdown завис")
	}
}

// TestRunGracefulShutdownOnSIGTERM покрывает реальный путь сигнала: процесс
// получает SIGTERM (как при остановке контейнера), и Run корректно завершается.
// Это прямая проверка приёмки «сервис гасится по SIGTERM».
func TestRunGracefulShutdownOnSIGTERM(t *testing.T) {
	svc, err := NewService("agent", freeConfig())
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- svc.Run(context.Background()) }()

	time.Sleep(100 * time.Millisecond)
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("не удалось послать SIGTERM: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run вернул ошибку после SIGTERM: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run не вернулся в таймаут после SIGTERM")
	}
}

// TestRunServesHealthzWhileRunning проверяет, что во время работы каркаса
// /healthz реально отвечает по сети на сконфигурированном адресе.
func TestRunServesHealthzWhileRunning(t *testing.T) {
	cfg := freeConfig()
	// Фиксированный loopback-порт для обращения по сети; при занятости тест
	// просто упадёт с понятной ошибкой, что приемлемо для smoke-проверки.
	cfg.HealthAddr = "127.0.0.1:18099"

	svc, err := NewService("bot", cfg)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()
	defer func() { <-done }()

	url := fmt.Sprintf("http://%s/healthz", cfg.HealthAddr)
	var resp *http.Response
	// Дать серверу подняться: несколько попыток с коротким ожиданием.
	for i := 0; i < 20; i++ {
		resp, err = http.Get(url) //nolint:gosec // адрес — наш фиксированный loopback в тесте
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("GET %s не удался: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("статус /healthz = %d, ожидалось 200", resp.StatusCode)
	}
}

// TestNewServiceRejectsBadConfig проверяет, что каркас не поднимается на
// невалидном конфиге (быстрый отказ вместо запуска с битыми настройками).
func TestNewServiceRejectsBadConfig(t *testing.T) {
	bad := Config{Env: "staging", LogLevel: "info", HealthAddr: ":8080", ShutdownTimeout: time.Second}
	if _, err := NewService("orchestrator", bad); err == nil {
		t.Error("NewService принял невалидный конфиг, ожидалась ошибка")
	}
}
