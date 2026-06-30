package platform

import (
	"testing"
	"time"
)

// embeddingConfig имитирует конфиг конкретного сервиса: встроенный базовый
// Config плюс специфичное поле под тем же env-префиксом.
type embeddingConfig struct {
	Config
	Extra string `env:"EXTRA" envDefault:"none"`
}

// TestLoadConfigDefaults проверяет, что без единой переменной окружения конфиг
// заполняется разумными дефолтами (приёмка тикета 0.6: сервис стартует «из
// коробки» в dev).
func TestLoadConfigDefaults(t *testing.T) {
	var cfg embeddingConfig
	if err := LoadConfig(&cfg, "SVC_"); err != nil {
		t.Fatalf("LoadConfig вернул ошибку на дефолтах: %v", err)
	}

	if cfg.Env != EnvDev {
		t.Errorf("Env по умолчанию = %q, ожидалось %q", cfg.Env, EnvDev)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("LogLevel по умолчанию = %q, ожидалось info", cfg.LogLevel)
	}
	if cfg.HealthAddr != ":8080" {
		t.Errorf("HealthAddr по умолчанию = %q, ожидалось :8080", cfg.HealthAddr)
	}
	if cfg.ShutdownTimeout != 10*time.Second {
		t.Errorf("ShutdownTimeout по умолчанию = %s, ожидалось 10s", cfg.ShutdownTimeout)
	}
	if cfg.Extra != "none" {
		t.Errorf("Extra по умолчанию = %q, ожидалось none", cfg.Extra)
	}
}

// TestLoadConfigOverrideFromEnv проверяет, что переменные окружения с нужным
// префиксом переопределяют дефолты (12-factor, caarlos0/env).
func TestLoadConfigOverrideFromEnv(t *testing.T) {
	t.Setenv("SVC_ENV", "prod")
	t.Setenv("SVC_LOG_LEVEL", "debug")
	t.Setenv("SVC_LOG_FORMAT", "json")
	t.Setenv("SVC_HEALTH_ADDR", ":9090")
	t.Setenv("SVC_SHUTDOWN_TIMEOUT", "3s")
	t.Setenv("SVC_EXTRA", "custom")

	var cfg embeddingConfig
	if err := LoadConfig(&cfg, "SVC_"); err != nil {
		t.Fatalf("LoadConfig вернул ошибку: %v", err)
	}

	if cfg.Env != EnvProd {
		t.Errorf("Env = %q, ожидалось %q", cfg.Env, EnvProd)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("LogLevel = %q, ожидалось debug", cfg.LogLevel)
	}
	if cfg.LogFormat != LogFormatJSON {
		t.Errorf("LogFormat = %q, ожидалось json", cfg.LogFormat)
	}
	if cfg.HealthAddr != ":9090" {
		t.Errorf("HealthAddr = %q, ожидалось :9090", cfg.HealthAddr)
	}
	if cfg.ShutdownTimeout != 3*time.Second {
		t.Errorf("ShutdownTimeout = %s, ожидалось 3s", cfg.ShutdownTimeout)
	}
	if cfg.Extra != "custom" {
		t.Errorf("Extra = %q, ожидалось custom", cfg.Extra)
	}
}

// TestPrefixIsolation проверяет, что переменные чужого префикса не влияют на
// конфиг — три сервиса сосуществуют в одном окружении без коллизий.
func TestPrefixIsolation(t *testing.T) {
	t.Setenv("OTHER_LOG_LEVEL", "error")

	var cfg embeddingConfig
	if err := LoadConfig(&cfg, "SVC_"); err != nil {
		t.Fatalf("LoadConfig вернул ошибку: %v", err)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("LogLevel = %q: чужой префикс OTHER_ не должен влиять", cfg.LogLevel)
	}
}

// TestValidateNormalizesLogFormat проверяет, что при пустом LOG_FORMAT он
// нормализуется по режиму: json в prod, text в dev.
func TestValidateNormalizesLogFormat(t *testing.T) {
	prod := Config{Env: EnvProd, LogLevel: "info", HealthAddr: ":8080", ShutdownTimeout: time.Second}
	if err := prod.Validate(); err != nil {
		t.Fatalf("Validate(prod) вернул ошибку: %v", err)
	}
	if prod.LogFormat != LogFormatJSON {
		t.Errorf("в prod формат по умолчанию = %q, ожидалось json", prod.LogFormat)
	}

	dev := Config{Env: EnvDev, LogLevel: "info", HealthAddr: ":8080", ShutdownTimeout: time.Second}
	if err := dev.Validate(); err != nil {
		t.Fatalf("Validate(dev) вернул ошибку: %v", err)
	}
	if dev.LogFormat != LogFormatText {
		t.Errorf("в dev формат по умолчанию = %q, ожидалось text", dev.LogFormat)
	}
}

// TestValidateRejectsBadValues проверяет, что недопустимые значения базовых
// полей отвергаются с ошибкой.
func TestValidateRejectsBadValues(t *testing.T) {
	cases := map[string]Config{
		"плохой env":      {Env: "staging", LogLevel: "info", HealthAddr: ":8080", ShutdownTimeout: time.Second},
		"плохой level":    {Env: EnvDev, LogLevel: "trace", HealthAddr: ":8080", ShutdownTimeout: time.Second},
		"плохой format":   {Env: EnvDev, LogLevel: "info", LogFormat: "xml", HealthAddr: ":8080", ShutdownTimeout: time.Second},
		"пустой addr":     {Env: EnvDev, LogLevel: "info", HealthAddr: "", ShutdownTimeout: time.Second},
		"нулевой таймаут": {Env: EnvDev, LogLevel: "info", HealthAddr: ":8080", ShutdownTimeout: 0},
	}
	for name, cfg := range cases {
		cfg := cfg
		t.Run(name, func(t *testing.T) {
			if err := cfg.Validate(); err == nil {
				t.Errorf("Validate(%+v) не вернул ошибку, а должен был", cfg)
			}
		})
	}
}

// TestLoadConfigBadDuration проверяет, что неразбираемая длительность из env
// приводит к ошибке загрузки конфига.
func TestLoadConfigBadDuration(t *testing.T) {
	t.Setenv("SVC_SHUTDOWN_TIMEOUT", "не-длительность")
	var cfg embeddingConfig
	if err := LoadConfig(&cfg, "SVC_"); err == nil {
		t.Error("LoadConfig не вернул ошибку на неверной длительности")
	}
}
