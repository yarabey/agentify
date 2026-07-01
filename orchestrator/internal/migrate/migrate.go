// Package migrate применяет встроенные goose-миграции схемы оркестратора к
// Postgres при старте сервиса.
//
// Назначение (бизнес): оркестратор обязан сам довести схему БД до актуальной
// версии до того, как начнёт обслуживать запросы (тикет 0.3 «goose-миграции
// применяются на старте»). Так локальный compose и прод поднимаются одной
// командой, без ручного шага миграций и без отдельного образа с goose CLI. Это
// инфраструктурное подключение к БД (открыть соединение, применить миграции,
// закрыть) — бизнес-запросы здесь НЕ выполняются.
//
// Как устроено (тех): источник миграций — embed.FS из пакета
// orchestrator/migrations (файлы 0000N_*.sql в goose-формате). Apply открывает
// database/sql-соединение к Postgres через pgx-драйвер stdlib, настраивает goose
// на встроенную FS с диалектом postgres и прогоняет все непримененные миграции
// «вверх». Применённые версии goose фиксирует в служебной таблице
// goose_db_version — повторный старт идемпотентен (применяются только новые).
package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"log/slog"
	"time"

	// pgx stdlib регистрирует драйвер database/sql "pgx"; goose работает поверх
	// database/sql, поэтому нужен именно такой адаптер, а не нативный pgxpool.
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

// migrationsDir — путь к миграциям внутри embed.FS. Файлы лежат в корне
// встроенной FS пакета migrations, поэтому каталог — текущий (".").
const migrationsDir = "."

// driverName — имя зарегистрированного pgx-драйвера database/sql.
const driverName = "pgx"

// dialect — SQL-диалект goose. Схема рассчитана на PostgreSQL (см. миграции).
const dialect = "postgres"

// Apply открывает соединение с Postgres по databaseURL и применяет все
// непримененные встроенные goose-миграции из migrationsFS «вверх».
//
// Параметр databaseURL — строка подключения pgx/libpq
// (postgres://user:pass@host:port/db?sslmode=...). migrationsFS — встроенная
// файловая система с goose-миграциями (обычно migrations.FS). logger
// используется для диагностики; допускается nil (тогда логи goose молчат).
//
// Функция блокирующая и предназначена для вызова при старте сервиса ДО приёма
// трафика: пока миграции не применены, оркестратор не должен отвечать готовым.
// Возвращает ошибку, если не удалось подключиться, настроить goose или
// применить миграцию; в этом случае старт сервиса должен быть прерван.
func Apply(ctx context.Context, databaseURL string, migrationsFS fs.FS, logger *slog.Logger) error {
	if databaseURL == "" {
		return fmt.Errorf("migrate: пустой DATABASE_URL — нечем подключиться к Postgres")
	}

	db, err := sql.Open(driverName, databaseURL)
	if err != nil {
		return fmt.Errorf("migrate: открытие соединения с Postgres: %w", err)
	}
	defer func() {
		if cerr := db.Close(); cerr != nil && logger != nil {
			logger.Warn("migrate: ошибка закрытия соединения с Postgres", slog.Any("error", cerr))
		}
	}()

	goose.SetBaseFS(migrationsFS)
	if logger != nil {
		// Прокидываем логи goose в slog сервиса, чтобы прогресс миграций был
		// виден в общем потоке логов контейнера.
		goose.SetLogger(newGooseLogger(logger))
	}
	if err := goose.SetDialect(dialect); err != nil {
		return fmt.Errorf("migrate: установка диалекта %q: %w", dialect, err)
	}

	if logger != nil {
		logger.Info("migrate: применяю миграции схемы", slog.String("dialect", dialect))
	}
	start := time.Now()
	if err := goose.UpContext(ctx, db, migrationsDir); err != nil {
		return fmt.Errorf("migrate: применение миграций: %w", err)
	}
	if logger != nil {
		logger.Info("migrate: миграции применены", slog.Duration("took", time.Since(start)))
	}
	return nil
}
