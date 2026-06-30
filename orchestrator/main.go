// Package main — точка входа сервиса-оркестратора.
//
// Назначение (бизнес): оркестратор — ядро системы (FR E*, A*, F*): REST+WS,
// FSM задач, auth, история, мост к Redpanda. В тикете 0.6 здесь реализован лишь
// общий операционный каркас (конфиг из env, slog, /healthz, graceful shutdown);
// бизнес-логика добавляется в EPIC 1/3/5+.
//
// Как устроено (тех): main — тонкий: грузит конфиг под префиксом ORCH_ через
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
const serviceName = "orchestrator"

// envPrefix — префикс env-переменных оркестратора, чтобы три сервиса не
// конфликтовали по именам в общем окружении (docker compose).
const envPrefix = "ORCH_"

// config — конфиг оркестратора: общий операционный базис плюс место для
// специфичных полей (БД, Redpanda, JWT появятся в следующих тикетах под тем же
// префиксом ORCH_).
type config struct {
	platform.Config
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "orchestrator: фатальная ошибка:", err)
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
