//go:build integration

// Integration-тест приёмки тикета 11.5 (FR I2): восстановление pg_dump на
// ЧИСТЫЙ инстанс Postgres проходит и данные на месте. Помечен тегом integration
// (как остальные тесты на реальном Postgres, см. deploy/README.md) — обычный
// `make test` его не гоняет и docker не требует; CI-джоба integration запускает
// `go test -tags=integration ./...`.
//
// Сценарий (повторяет механику deploy/backup/backup.sh + restore-check.sh):
//   1. поднять Postgres-контейнер A, создать таблицу и вставить строки;
//   2. снять pg_dump в custom-формате (-Fc) внутри A, вынести файл дампа;
//   3. поднять ВТОРОЙ, заведомо чистый Postgres-контейнер B;
//   4. восстановить дамп в B через pg_restore;
//   5. убедиться, что таблица и все строки на месте (иначе бэкап бесполезен).
package pgbackup

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	// pgImage тянется через настроенный daemon registry-mirror (тикет 0.3) —
	// тот же образ, что и сервис postgres/бэкап-сервис (мажор 16), чтобы
	// pg_dump/pg_restore были версионно совместимы.
	pgImage = "postgres:16-alpine"
	pgUser  = "backup_test"
	pgPass  = "backup_test"
	pgDB    = "backup_test"

	// Путь дампа внутри контейнеров.
	dumpPath = "/tmp/backup_check.dump"
)

// startPG поднимает одиночный Postgres в контейнере и возвращает контейнер и
// строку подключения (pgx URL). Очистку регистрируем через t.Cleanup.
func startPG(ctx context.Context, t *testing.T, name string) (testcontainers.Container, string) {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image:        pgImage,
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     pgUser,
			"POSTGRES_PASSWORD": pgPass,
			"POSTGRES_DB":       pgDB,
		},
		// Ждём ВТОРОЕ вхождение "ready to accept connections": официальный образ
		// postgres поднимает сервер дважды (initdb + финальный старт), поэтому
		// раннее срабатывание дало бы "connection reset" на первом подключении
		// (та же причина, что в orchestrator/internal/db тестах).
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(2 * time.Minute),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("поднять Postgres-контейнер %s: %v", name, err)
	}
	t.Cleanup(func() {
		if terr := testcontainers.TerminateContainer(container); terr != nil {
			t.Logf("terminate Postgres %s: %v", name, terr)
		}
	})

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("host контейнера %s: %v", name, err)
	}
	port, err := container.MappedPort(ctx, "5432/tcp")
	if err != nil {
		t.Fatalf("порт контейнера %s: %v", name, err)
	}
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		pgUser, pgPass, host, port.Port(), pgDB)
	return container, dsn
}

// execOK выполняет команду в контейнере и валит тест при ненулевом коде выхода.
func execOK(ctx context.Context, t *testing.T, c testcontainers.Container, what string, cmd []string) {
	t.Helper()
	code, reader, err := c.Exec(ctx, cmd)
	if err != nil {
		t.Fatalf("%s: exec: %v", what, err)
	}
	out, _ := io.ReadAll(reader)
	if code != 0 {
		t.Fatalf("%s: код выхода %d, вывод:\n%s", what, code, string(out))
	}
}

// TestIntegration_BackupRestoreRoundtrip — прямая приёмка 11.5 (FR I2):
// наполнить БД → pg_dump → восстановить на ЧИСТЫЙ инстанс → данные на месте.
func TestIntegration_BackupRestoreRoundtrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	// --- 1. Инстанс-источник A: наполняем данными --------------------------
	src, srcDSN := startPG(ctx, t, "source")

	srcPool, err := pgxpool.New(ctx, srcDSN)
	if err != nil {
		t.Fatalf("pgxpool.New(source): %v", err)
	}
	if _, err = srcPool.Exec(ctx, `
		CREATE TABLE history (
			id    bigint PRIMARY KEY,
			note  text NOT NULL
		)`); err != nil {
		srcPool.Close()
		t.Fatalf("create table на источнике: %v", err)
	}
	const wantRows = 3
	for i := 1; i <= wantRows; i++ {
		if _, err = srcPool.Exec(ctx,
			`INSERT INTO history (id, note) VALUES ($1, $2)`,
			i, fmt.Sprintf("запись истории №%d (FR I2)", i)); err != nil {
			srcPool.Close()
			t.Fatalf("insert строки %d: %v", i, err)
		}
	}
	srcPool.Close()

	// --- 2. pg_dump (-Fc) внутри A, выносим файл дампа ----------------------
	execOK(ctx, t, src, "pg_dump", []string{
		"sh", "-c",
		fmt.Sprintf("PGPASSWORD=%s pg_dump -U %s -d %s -Fc -f %s",
			pgPass, pgUser, pgDB, dumpPath),
	})
	rc, err := src.CopyFileFromContainer(ctx, dumpPath)
	if err != nil {
		t.Fatalf("CopyFileFromContainer(dump): %v", err)
	}
	dump, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatalf("прочитать дамп: %v", err)
	}
	if len(dump) == 0 {
		t.Fatal("дамп пустой — pg_dump ничего не вернул")
	}

	// --- 3. Чистый инстанс B ------------------------------------------------
	// Отдельный свежий контейнер: гарантированно пустая база, никакого
	// состояния источника — это и есть «восстановление на чистый инстанс».
	dst, dstDSN := startPG(ctx, t, "restore")

	// Подтверждаем чистоту B: таблицы history там ещё нет.
	dstPool, err := pgxpool.New(ctx, dstDSN)
	if err != nil {
		t.Fatalf("pgxpool.New(restore): %v", err)
	}
	defer dstPool.Close()
	var existsBefore bool
	if err = dstPool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM information_schema.tables
		 WHERE table_schema='public' AND table_name='history')`).Scan(&existsBefore); err != nil {
		t.Fatalf("проверка чистоты инстанса B: %v", err)
	}
	if existsBefore {
		t.Fatal("инстанс B не чист: таблица history уже существует до восстановления")
	}

	// --- 4. pg_restore дампа в B -------------------------------------------
	if err = dst.CopyToContainer(ctx, dump, dumpPath, 0o644); err != nil {
		t.Fatalf("CopyToContainer(dump) в B: %v", err)
	}
	execOK(ctx, t, dst, "pg_restore", []string{
		"sh", "-c",
		fmt.Sprintf("PGPASSWORD=%s pg_restore --no-owner --no-privileges --exit-on-error -U %s -d %s %s",
			pgPass, pgUser, pgDB, dumpPath),
	})

	// --- 5. Данные на месте -------------------------------------------------
	var gotRows int
	if err = dstPool.QueryRow(ctx, `SELECT count(*) FROM history`).Scan(&gotRows); err != nil {
		t.Fatalf("count после восстановления: %v", err)
	}
	if gotRows != wantRows {
		t.Fatalf("после восстановления строк %d, ожидалось %d", gotRows, wantRows)
	}
	// Точная сверка содержимого одной строки — не только количество.
	var note string
	if err = dstPool.QueryRow(ctx, `SELECT note FROM history WHERE id = 2`).Scan(&note); err != nil {
		t.Fatalf("select восстановленной строки: %v", err)
	}
	if want := "запись истории №2 (FR I2)"; note != want {
		t.Fatalf("содержимое строки не совпало: получено %q, ожидалось %q", note, want)
	}

	// Санити: содержимое дампа — это pg_custom-архив (сигнатура "PGDMP").
	if !bytes.HasPrefix(dump, []byte("PGDMP")) {
		t.Fatalf("дамп не в custom-формате pg_dump (нет сигнатуры PGDMP): % x", dump[:min(5, len(dump))])
	}
}
