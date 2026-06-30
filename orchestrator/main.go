// Package main — точка входа сервиса-оркестратора.
//
// Назначение (бизнес): оркестратор — ядро системы (FR E*, A*, F*): REST+WS,
// FSM задач, auth, история, мост к Redpanda. В тикете 0.6 здесь реализован лишь
// общий операционный каркас (конфиг из env, slog, /healthz, graceful shutdown);
// бизнес-логика добавляется в EPIC 1/3/5+.
//
// Как устроено (тех): main — тонкий: грузит конфиг под префиксом ORCH_ через
// общий пакет platform, применяет миграции на старте, при наличии БД поднимает
// pgxpool и монтирует сгенерированный из openapi API-роутер (тикет 1.2: реальный
// POST /auth/register; тикет 1.3: POST /auth/login, /auth/refresh, /auth/logout;
// прочие операции — 501), затем блокируется до SIGTERM/SIGINT и гасится
// gracefully, закрывая пул. Бинарь поддерживает одну подкоманду —
// `orchestrator bootstrap` (тикет 1.7, см. cmd_bootstrap.go): без аргументов
// запускается обычный сервис (как раньше), с аргументом "bootstrap" —
// идемпотентно создаёт первого администратора и стартовый токен регистрации и
// завершается, не поднимая HTTP.
package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/yarabey/agentify/internal/platform"
	"github.com/yarabey/agentify/orchestrator/internal/api"
	"github.com/yarabey/agentify/orchestrator/internal/db"
	"github.com/yarabey/agentify/orchestrator/internal/migrate"
	"github.com/yarabey/agentify/orchestrator/migrations"
)

// serviceName — каноническое имя сервиса в логах и в теле /healthz.
const serviceName = "orchestrator"

// envPrefix — префикс env-переменных оркестратора, чтобы три сервиса не
// конфликтовали по именам в общем окружении (docker compose).
const envPrefix = "ORCH_"

// appEncryptionKeyLen — требуемая длина мастер-ключа шифрования после
// base64-декодирования ORCH_APP_ENCRYPTION_KEY: 32 байта (AES-256, см.
// internal/crypto.Encrypt).
const appEncryptionKeyLen = 32

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

	// JWTSigningKey — секрет HMAC для подписи/проверки access-JWT (тикет 1.3,
	// FR A3). Переменная ORCH_JWT_SIGNING_KEY генерируется ОДНОКРАТНО вручную
	// (`openssl rand -base64 48`, см. docs/MANUAL_STEPS.md) и кладётся в секреты
	// окружения — здесь НЕТ дефолта и НЕ генерируется фолбэк: пустой ключ в
	// проде означал бы предсказуемую подпись токенов доступа, поэтому при
	// поднятом API (DatabaseURL непуст) пустой JWTSigningKey — фатальная ошибка
	// старта (см. run), а не тихий запуск с небезопасным ключом.
	JWTSigningKey string `env:"JWT_SIGNING_KEY"`

	// AppEncryptionKey — base64-кодированный мастер-ключ (32 байта после
	// декодирования) для at-rest шифрования и HMAC-отпечатков (internal/crypto,
	// тикет 2.2, зерно общего крипто-модуля тикета 11.1). Переменная
	// ORCH_APP_ENCRYPTION_KEY генерируется ОДНОКРАТНО вручную (`openssl rand
	// -base64 32`, см. docs/MANUAL_STEPS.md) и кладётся в секреты окружения —
	// как и JWTSigningKey, здесь НЕТ дефолта и НЕ генерируется фолбэк: пустой
	// или предсказуемый мастер-ключ означал бы, что UUID-секреты интеграций
	// (FR B2) можно расшифровать/подделать HMAC без секрета, поэтому при
	// поднятом API пустое/некорректное значение — фатальная ошибка старта
	// (см. run), а не тихий запуск с небезопасным ключом.
	AppEncryptionKey string `env:"APP_ENCRYPTION_KEY"`
}

func main() {
	// Подкоманда `orchestrator bootstrap` (тикет 1.7) перехватывается ДО
	// обычного запуска сервиса: без аргументов (len(os.Args) == 1) поведение
	// не меняется — стартует сервис, как и раньше. Простого разбора os.Args[1]
	// достаточно для единственной подкоманды MVP — без новой CLI-библиотеки.
	if len(os.Args) > 1 && os.Args[1] == "bootstrap" {
		if err := runBootstrap(); err != nil {
			fmt.Fprintln(os.Stderr, "orchestrator bootstrap: фатальная ошибка:", err)
			os.Exit(1)
		}
		return
	}

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

	// Пул соединений и реальный API-сервер (тикеты 1.2/1.3). Без БД API не
	// поднимаем — auth-эндпоинты обращаются к Postgres; остаётся только /healthz
	// из каркаса platform. С БД монтируем сгенерированный из openapi chi-роутер
	// (POST /auth/register, /auth/login, /auth/refresh, /auth/logout +
	// 501-заглушки прочих операций) и отдаём его сервису через SetHandler,
	// сохраняя единый graceful shutdown.
	if cfg.DatabaseURL != "" {
		// Auth-эндпоинты подписывают access-JWT этим ключом (FR A3, тикет 1.3) —
		// пустой ключ означал бы предсказуемую подпись токенов в проде, поэтому
		// падаем на старте, а не поднимаем API с небезопасным дефолтом.
		if cfg.JWTSigningKey == "" {
			return fmt.Errorf("orchestrator: ORCH_JWT_SIGNING_KEY пуст — задайте секрет (см. docs/MANUAL_STEPS.md), пустой/предсказуемый ключ подписи JWT недопустим в проде")
		}

		// Мастер-ключ шифрования (тикет 2.2, internal/crypto): декодируем base64
		// (как сгенерировано `openssl rand -base64 32`) и требуем РОВНО 32 байта
		// после декодирования — иначе тихий запуск с нулевым/коротким/мусорным
		// ключом сделал бы UUID-секреты интеграций расшифровываемыми/подделываемыми.
		if cfg.AppEncryptionKey == "" {
			return fmt.Errorf("orchestrator: ORCH_APP_ENCRYPTION_KEY пуст — задайте секрет (см. docs/MANUAL_STEPS.md), пустой/предсказуемый ключ шифрования недопустим в проде")
		}
		encryptionKey, err := base64.StdEncoding.DecodeString(cfg.AppEncryptionKey)
		if err != nil {
			return fmt.Errorf("orchestrator: ORCH_APP_ENCRYPTION_KEY не является валидным base64 (ожидается вывод `openssl rand -base64 32`): %w", err)
		}
		if len(encryptionKey) != appEncryptionKeyLen {
			return fmt.Errorf("orchestrator: ORCH_APP_ENCRYPTION_KEY после base64-декодирования должен быть длиной %d байт, получено %d", appEncryptionKeyLen, len(encryptionKey))
		}

		pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
		if err != nil {
			return fmt.Errorf("orchestrator: создание пула соединений к Postgres: %w", err)
		}
		// Пул закрываем после возврата Run (т.е. после graceful shutdown HTTP):
		// новые запросы уже не принимаются, активные доиграны.
		defer pool.Close()

		server := api.NewServer(db.New(pool), svc.Logger(), []byte(cfg.JWTSigningKey), encryptionKey)
		svc.SetHandler(api.NewRouter(server))
	}

	return svc.Run(ctx)
}
