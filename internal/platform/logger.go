package platform

import (
	"log/slog"
	"os"
)

// NewLogger строит slog-логгер по конфигу: JSON-хендлер для prod (структурный
// сбор логов, наблюдаемость) и text-хендлер для dev (читаемость), с уровнем из
// Config.LogLevel. Логи пишутся в stderr (12-factor: поток событий процесса).
//
// Поле service добавляется ко всем записям как атрибут "service", чтобы в общем
// логопотоке (docker compose, агрегатор) можно было отличать оркестратор/бот/
// агент. Конфиг должен быть провалидирован (Config.Validate) — формат уже
// нормализован; при неизвестном уровне используется info как безопасный дефолт.
func NewLogger(cfg Config, service string) *slog.Logger {
	level, err := parseLogLevel(cfg.LogLevel)
	if err != nil {
		level = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{Level: level}

	var handler slog.Handler
	if cfg.LogFormat == LogFormatJSON {
		handler = slog.NewJSONHandler(os.Stderr, opts)
	} else {
		handler = slog.NewTextHandler(os.Stderr, opts)
	}

	return slog.New(handler).With(slog.String("service", service))
}
