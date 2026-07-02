package main

import (
	"testing"

	"github.com/yarabey/agentify/internal/platform"
)

// TestLoadConfig_BotFields проверяет, что специфичные для бота поля (тикет 10.1)
// читаются из env под префиксом BOT_ и что базовый операционный конфиг (0.6)
// продолжает загружаться. Токен/секрет не коммитятся — здесь используются
// заведомо фиктивные значения только для проверки маппинга имён переменных.
func TestLoadConfig_BotFields(t *testing.T) {
	t.Setenv("BOT_TOKEN", "123:FAKE")
	t.Setenv("BOT_WEBHOOK_SECRET", "sekret")
	t.Setenv("BOT_PUBLIC_URL", "https://bot.example.com")
	t.Setenv("BOT_ENV", "prod")

	var cfg config
	if err := platform.LoadConfig(&cfg, envPrefix); err != nil {
		t.Fatalf("LoadConfig вернул ошибку: %v", err)
	}

	if cfg.Token != "123:FAKE" {
		t.Errorf("Token = %q, ожидался \"123:FAKE\"", cfg.Token)
	}
	if cfg.WebhookSecret != "sekret" {
		t.Errorf("WebhookSecret = %q, ожидался \"sekret\"", cfg.WebhookSecret)
	}
	if cfg.PublicURL != "https://bot.example.com" {
		t.Errorf("PublicURL = %q, ожидался \"https://bot.example.com\"", cfg.PublicURL)
	}
	if cfg.Env != platform.EnvProd {
		t.Errorf("Env = %q, ожидался %q", cfg.Env, platform.EnvProd)
	}
}

// TestLoadConfig_RedpandaSeeds проверяет, что BOT_REDPANDA_SEEDS (тикет 10.4,
// доставка уведомлений, см. bot/notify.go) читается как срез через запятую —
// тот же формат/тег envSeparator, что и ORCH_REDPANDA_SEEDS в
// orchestrator/main.go.
func TestLoadConfig_RedpandaSeeds(t *testing.T) {
	t.Setenv("BOT_REDPANDA_SEEDS", "redpanda-1:9092,redpanda-2:9092")

	var cfg config
	if err := platform.LoadConfig(&cfg, envPrefix); err != nil {
		t.Fatalf("LoadConfig вернул ошибку: %v", err)
	}

	want := []string{"redpanda-1:9092", "redpanda-2:9092"}
	if len(cfg.RedpandaSeeds) != len(want) {
		t.Fatalf("RedpandaSeeds = %v, ожидался %v", cfg.RedpandaSeeds, want)
	}
	for i := range want {
		if cfg.RedpandaSeeds[i] != want[i] {
			t.Errorf("RedpandaSeeds[%d] = %q, ожидался %q", i, cfg.RedpandaSeeds[i], want[i])
		}
	}
}

// TestLoadConfig_Defaults проверяет, что без BOT_-переменных конфиг остаётся
// валидным каркасом 0.6 (dev, без токена/webhook) — то есть бот запускается в
// dev без единой переменной и не падает (см. run).
func TestLoadConfig_Defaults(t *testing.T) {
	var cfg config
	if err := platform.LoadConfig(&cfg, envPrefix); err != nil {
		t.Fatalf("LoadConfig вернул ошибку: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate вернул ошибку на дефолтном конфиге: %v", err)
	}

	if cfg.Token != "" {
		t.Errorf("Token по умолчанию = %q, ожидалась пустая строка", cfg.Token)
	}
	if cfg.WebhookSecret != "" {
		t.Errorf("WebhookSecret по умолчанию = %q, ожидалась пустая строка", cfg.WebhookSecret)
	}
	if cfg.PublicURL != "" {
		t.Errorf("PublicURL по умолчанию = %q, ожидалась пустая строка", cfg.PublicURL)
	}
	if len(cfg.RedpandaSeeds) != 0 {
		t.Errorf("RedpandaSeeds по умолчанию = %v, ожидался пустой срез (доставка уведомлений отключена)", cfg.RedpandaSeeds)
	}
}
