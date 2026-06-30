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
	"github.com/yarabey/agentify/orchestrator/internal/migrate"
	"github.com/yarabey/agentify/orchestrator/migrations"
)

// serviceName — каноническое имя сервиса в логах и в теле /healthz.
const serviceName = "orchestrator"

// envPrefix — префикс env-переменных оркестратора, чтобы три сервиса не
// конфликтовали по именам в общем окружении (docker compose).
const envPrefix = "ORCH_"

// config — конфиг оркестратора: общий операционный базис плюс специфичные поля
// под тем же префиксом ORCH_ (Redpanda, JWT появятся в следующих тикетах).
type config struct {
	platform.Config

	// DatabaseURL — строка подключения к Postgres (pgx/libpq DSN). Нужна уже в
	// тикете 0.3 для применения goose-миграций на старте; имя переменной —
	// ORCH_DATABASE_URL (префикс ORCH_ как у остального конфига оркестратора).
	// Пустое значение допустимо для запусков без БД (юнит-тесты каркаса): тогда
	// шаг миграций пропускается.
	DatabaseURL string `env:"DATABASE_URL"`
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

	ctx := context.Background()

	// Миграции-на-старте (тикет 0.3): прежде чем сервис начнёт отвечать готовым
	// на /healthz, приводим схему БД к актуальной версии встроенными
	// goose-миграциями. Если DATABASE_URL не задан (например, в каркасных
	// прогонах без БД), шаг пропускаем — это инфраструктурная, а не бизнес-часть.
	if cfg.DatabaseURL != "" {
		if err := migrate.Apply(ctx, cfg.DatabaseURL, migrations.FS, svc.Logger()); err != nil {
			return err
		}
	} else {
		svc.Logger().Warn("ORCH_DATABASE_URL пуст — пропускаю применение миграций на старте")
	}

	return svc.Run(ctx)
}
