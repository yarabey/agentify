package migrate

import (
	"database/sql"
	"io/fs"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/yarabey/agentify/orchestrator/migrations"
)

// TestEmbeddedMigrationsNonEmpty проверяет, что встроенная FS реально содержит
// goose-миграции. Это страховка приёмки тикета 0.3: если *.sql не попали в
// бинарь (например, ошибочный путь embed), миграции-на-старте молча ничего не
// сделают, и схема БД останется пустой. Тест не требует ни БД, ни сети.
func TestEmbeddedMigrationsNonEmpty(t *testing.T) {
	entries, err := fs.Glob(migrations.FS, "*.sql")
	if err != nil {
		t.Fatalf("glob по встроенной FS миграций: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("встроенная FS миграций пуста: ожидался хотя бы один 0000N_*.sql")
	}
}

// TestMigrationProviderBuilds проверяет, что из встроенной FS собирается
// валидный goose-провайдер: goose разбирает все *.sql, проверяет нумерацию
// версий и отсутствие дублей. Это «провайдер миграций собирается» из приёмки
// тикета 0.3 — без подключения к Postgres (sql.Open не открывает соединение,
// провайдер лишь читает и парсит файлы из FS).
func TestMigrationProviderBuilds(t *testing.T) {
	// sql.Open с pgx-драйвером не устанавливает соединение, пока не выполнен
	// запрос; NewProvider использует db только для определения store-таблицы, а
	// миграции читает и валидирует из переданной FS.
	db, err := sql.Open(driverName, "postgres://user:pass@127.0.0.1:5432/db?sslmode=disable")
	if err != nil {
		t.Fatalf("sql.Open (без реального коннекта): %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	provider, err := goose.NewProvider(goose.DialectPostgres, db, migrations.FS)
	if err != nil {
		t.Fatalf("сборка goose-провайдера из встроенной FS: %v", err)
	}

	got := provider.ListSources()
	if len(got) == 0 {
		t.Fatal("goose-провайдер не нашёл ни одной миграции во встроенной FS")
	}

	// Версии должны идти строго по возрастанию без дублей — иначе goose отверг бы
	// провайдер выше, но фиксируем инвариант явно для наглядности приёмки.
	var prev int64
	for _, src := range got {
		if src.Version <= prev {
			t.Fatalf("версии миграций не строго возрастают: %d после %d", src.Version, prev)
		}
		prev = src.Version
	}
}
