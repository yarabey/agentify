// cmd_bootstrap.go — подкоманда `orchestrator bootstrap` (тикет 1.7, FR A2).
//
// Назначение (бизнес): закрытая система (FR A1) не может выдать сама себе
// первый аккаунт — обычная регистрация (POST /auth/register) сама требует
// активного токена регистрации. Кто-то должен один раз завести и
// администратора, и сам этот токен при разворачивании. Эта подкоманда делает
// ровно это и идемпотентна: повторный запуск (например, при каждом деплое)
// не падает и не плодит дубликаты — см. orchestrator/internal/bootstrap.
//
// Как устроено (тех): отдельная подкоманда без CLI-фреймворка — `orchestrator
// bootstrap` (main перехватывает os.Args[1] == "bootstrap" ДО запуска
// обычного сервиса; без аргументов поведение не меняется, сервис стартует как
// раньше). Подключается к той же БД (ORCH_DATABASE_URL), что и сервис, и сама
// приводит схему к актуальной версии (goose, как и run() в main.go) — так
// команду можно безопасно гонять и до, и после первого старта сервиса.
// Бизнес-логика и идемпотентность — в orchestrator/internal/bootstrap, здесь
// только сборка конфига и подключения (тонкий cmd-слой в духе run()).
package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/yarabey/agentify/internal/platform"
	"github.com/yarabey/agentify/orchestrator/internal/bootstrap"
	"github.com/yarabey/agentify/orchestrator/internal/db"
	"github.com/yarabey/agentify/orchestrator/internal/migrate"
	"github.com/yarabey/agentify/orchestrator/migrations"
)

// bootstrapConfig — конфиг подкоманды `orchestrator bootstrap`: общий
// platform.Config (ENV/LOG_* — те же переменные, что у сервиса, под тем же
// префиксом ORCH_, используются только для формата логов) и ORCH_DATABASE_URL,
// плюс три специфичных для bootstrap секрета:
//   - ORCH_BOOTSTRAP_ADMIN_USERNAME / ORCH_BOOTSTRAP_ADMIN_PASSWORD — креды
//     первого администратора. Имена выбраны по аналогии с уже принятым в
//     проекте ORCH_JWT_SIGNING_KEY: префикс ORCH_, без дефолта — пустое
//     значение для bootstrap фатально (см. runBootstrap), а не тихо
//     пропускается, как ORCH_DATABASE_URL у обычного запуска сервиса
//     (там пустая БД — осознанно валидный «каркасный» режим без API, здесь же
//     bootstrap без кредов администратора просто бессмыслен).
//   - ORCH_INITIAL_REGISTRATION_TOKEN — значение стартового токена
//     регистрации. Имя секрета зафиксировано в docs/MANUAL_STEPS.md как
//     `INITIAL_REGISTRATION_TOKEN`; здесь читается под тем же префиксом
//     ORCH_, что и остальной конфиг оркестратора (см. envPrefix в main.go).
type bootstrapConfig struct {
	platform.Config

	// DatabaseURL — DSN Postgres; то же поле, что у config в main.go, см. его
	// godoc. В отличие от обычного запуска сервиса, для bootstrap оно
	// обязательно: без БД бутстрапить нечего.
	DatabaseURL string `env:"DATABASE_URL"`

	// BootstrapAdminUsername — username первого администратора.
	BootstrapAdminUsername string `env:"BOOTSTRAP_ADMIN_USERNAME"`
	// BootstrapAdminPassword — пароль первого администратора в открытом виде
	// (хэшируется argon2id внутри orchestrator/internal/bootstrap, не
	// сервисным конфигом).
	BootstrapAdminPassword string `env:"BOOTSTRAP_ADMIN_PASSWORD"`
	// InitialRegistrationToken — значение стартового токена регистрации
	// (FR A2); генерируется однократно вручную (см. docs/MANUAL_STEPS.md,
	// `openssl rand -hex 16`) и кладётся в секреты окружения.
	InitialRegistrationToken string `env:"INITIAL_REGISTRATION_TOKEN"`
}

// runBootstrap выполняет подкоманду `orchestrator bootstrap`: грузит конфиг из
// окружения, приводит схему БД к актуальной версии и идемпотентно создаёт
// первого администратора и активный токен регистрации (FR A2).
//
// Возвращает ошибку при невалидном/неполном конфиге или сбое подключения к
// БД/применения миграций/самого bootstrap'а (orchestrator/internal/bootstrap.Run).
// Успешный повторный запуск с теми же переменными окружения — не ошибка (см.
// godoc bootstrap.Run): это и есть требуемая идемпотентность (тикет 1.7).
func runBootstrap() error {
	var cfg bootstrapConfig
	if err := platform.LoadConfig(&cfg, envPrefix); err != nil {
		return err
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	logger := platform.NewLogger(cfg.Config, serviceName)

	if cfg.DatabaseURL == "" {
		return fmt.Errorf("orchestrator bootstrap: ORCH_DATABASE_URL пуст — bootstrap требует подключения к БД")
	}
	if cfg.BootstrapAdminUsername == "" || cfg.BootstrapAdminPassword == "" {
		return fmt.Errorf("orchestrator bootstrap: ORCH_BOOTSTRAP_ADMIN_USERNAME и ORCH_BOOTSTRAP_ADMIN_PASSWORD обязательны")
	}
	if cfg.InitialRegistrationToken == "" {
		return fmt.Errorf("orchestrator bootstrap: ORCH_INITIAL_REGISTRATION_TOKEN пуст — задайте секрет (см. docs/MANUAL_STEPS.md)")
	}

	ctx := context.Background()

	// Та же логика, что и в run(): приводим схему к актуальной версии перед
	// работой с таблицами — bootstrap безопасно гонять и до первого старта
	// сервиса (например, отдельным шагом деплоя), и после.
	if err := migrate.Apply(ctx, cfg.DatabaseURL, migrations.FS, logger); err != nil {
		return err
	}

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("orchestrator bootstrap: создание пула соединений к Postgres: %w", err)
	}
	defer pool.Close()

	return bootstrap.Run(ctx, db.New(pool), logger, bootstrap.Config{
		AdminUsername:            cfg.BootstrapAdminUsername,
		AdminPassword:            cfg.BootstrapAdminPassword,
		InitialRegistrationToken: cfg.InitialRegistrationToken,
	})
}
