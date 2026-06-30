// Package main — точка входа агента-демона на машине пользователя.
//
// Назначение (бизнес): агент — Go-демон, который ставится на машину
// пользователя одной командой (FR C1, C5), держит исходящий WebSocket к
// оркестратору, локальный durable outbox и запускает провайдеров Claude /
// Claude Code. В compose не входит — распространяется как бинарь через
// GoReleaser. В тикете 0.6 здесь реализован лишь общий операционный каркас
// (конфиг из env, slog, /healthz, graceful shutdown); WS-транспорт и провайдеры
// добавляются в EPIC 3/4. /healthz и graceful shutdown нужны агенту уже сейчас
// для демонизации (systemd/launchd, FR C5) и проверок живости.
//
// Как устроено (тех): main — тонкий: грузит конфиг под префиксом AGENT_ через
// общий пакет platform, поднимает каркас сервиса (slog + chi /healthz) и
// блокируется до SIGTERM/SIGINT, после чего гасится gracefully.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/yarabey/agentify/internal/platform"
)

// serviceName — каноническое имя сервиса в логах и в теле /healthz.
const serviceName = "agent"

// envPrefix — префикс env-переменных агента; на машине пользователя коллизий с
// другими сервисами нет, но единый стиль упрощает конфигурацию и доку.
const envPrefix = "AGENT_"

// config — конфиг агента: общий операционный базис плюс место для специфичных
// полей (адрес оркестратора, UUID, провайдер появятся в EPIC 3/4 под тем же
// префиксом AGENT_).
type config struct {
	platform.Config
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "agent: фатальная ошибка:", err)
		os.Exit(1)
	}
}

// run загружает конфиг, собирает каркас сервиса и блокируется до остановки.
func run() error {
	var cfg config
	if err := platform.LoadConfig(&cfg, envPrefix); err != nil {
		return err
	}

	svc, err := platform.NewService(serviceName, cfg.Config)
	if err != nil {
		return err
	}

	return svc.Run(context.Background())
}
