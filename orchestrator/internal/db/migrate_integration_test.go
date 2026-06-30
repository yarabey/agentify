//go:build integration

// Integration-тесты слоя данных auth на РЕАЛЬНОМ Postgres через testcontainers-go
// (тикет 1.1; стек — docs/01_tech_stack_and_architecture.md §3). Помечены тегом
// integration, чтобы обычный `make test` (unit) не требовал docker и был быстрым;
// CI-джоба `integration` гоняет `go test -tags=integration ./...`.
//
// Проверяемые сценарии (приёмка 1.1):
//   (a) миграция up/down — все goose-миграции применяются «вверх», таблицы auth
//       (users/registration_tokens/refresh_tokens) появляются; затем откат «вниз»
//       до версии 0 проходит без ошибок и удаляет таблицы (FR A1–A4);
//   (b) CRUD-roundtrip sgened sqlc против реальной схемы: создать user → прочитать
//       по username; вставить refresh_token → найти по hash → отозвать (FR A1, A3).
//
// Контейнер чистится через testcontainers terminate (defer) + Ryuk reaper.
package db_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/yarabey/agentify/orchestrator/internal/db"
	"github.com/yarabey/agentify/orchestrator/migrations"
)

const (
	// pgImage тянется через настроенный daemon registry-mirror (тикет 0.3,
	// mirror.gcr.io) — прокси не обходим.
	pgImage = "postgres:16-alpine"
	pgUser  = "auth_test"
	pgPass  = "auth_test"
	pgDB    = "auth_test"
)

// startPostgres поднимает одиночный Postgres в контейнере и возвращает строку
// подключения (pgx/libpq URL) и функцию очистки (terminate).
func startPostgres(ctx context.Context, t *testing.T) (string, func()) {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image:        pgImage,
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     pgUser,
			"POSTGRES_PASSWORD": pgPass,
			"POSTGRES_DB":       pgDB,
		},
		WaitingFor: wait.ForListeningPort("5432/tcp").WithStartupTimeout(2 * time.Minute),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("поднять Postgres-контейнер: %v", err)
	}
	cleanup := func() {
		if terr := testcontainers.TerminateContainer(container); terr != nil {
			t.Logf("terminate Postgres: %v", terr)
		}
	}

	host, err := container.Host(ctx)
	if err != nil {
		cleanup()
		t.Fatalf("получить host контейнера: %v", err)
	}
	port, err := container.MappedPort(ctx, "5432/tcp")
	if err != nil {
		cleanup()
		t.Fatalf("получить порт контейнера: %v", err)
	}
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		pgUser, pgPass, host, port.Port(), pgDB)
	return dsn, cleanup
}

// tablesExist проверяет наличие всех auth-таблиц в схеме public.
func tablesExist(ctx context.Context, t *testing.T, sqlDB *sql.DB, names ...string) {
	t.Helper()
	for _, name := range names {
		var exists bool
		err := sqlDB.QueryRowContext(ctx,
			`SELECT EXISTS (SELECT 1 FROM information_schema.tables
			 WHERE table_schema = 'public' AND table_name = $1)`, name).Scan(&exists)
		if err != nil {
			t.Fatalf("проверка существования таблицы %q: %v", name, err)
		}
		if !exists {
			t.Fatalf("после миграции up таблица %q не найдена", name)
		}
	}
}

// noTablesExist проверяет, что ни одной из перечисленных таблиц нет (после down).
func noTablesExist(ctx context.Context, t *testing.T, sqlDB *sql.DB, names ...string) {
	t.Helper()
	for _, name := range names {
		var exists bool
		err := sqlDB.QueryRowContext(ctx,
			`SELECT EXISTS (SELECT 1 FROM information_schema.tables
			 WHERE table_schema = 'public' AND table_name = $1)`, name).Scan(&exists)
		if err != nil {
			t.Fatalf("проверка отсутствия таблицы %q: %v", name, err)
		}
		if exists {
			t.Fatalf("после миграции down таблица %q всё ещё существует", name)
		}
	}
}

// TestIntegration_MigrationUpDown — прямая приёмка 1.1: применяем все миграции
// «вверх» (auth-таблицы появляются), затем откатываем «вниз» до 0 без ошибок.
func TestIntegration_MigrationUpDown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dsn, cleanup := startPostgres(ctx, t)
	defer cleanup()

	sqlDB, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer func() { _ = sqlDB.Close() }()

	goose.SetBaseFS(migrations.FS)
	if derr := goose.SetDialect("postgres"); derr != nil {
		t.Fatalf("goose SetDialect: %v", derr)
	}

	// Up: применяем все встроенные миграции.
	if uperr := goose.UpContext(ctx, sqlDB, "."); uperr != nil {
		t.Fatalf("goose Up: %v", uperr)
	}
	authTables := []string{"users", "registration_tokens", "refresh_tokens"}
	tablesExist(ctx, t, sqlDB, authTables...)

	// Down: откатываем все миграции до версии 0 — проверяем обратимость схемы.
	if derr := goose.DownToContext(ctx, sqlDB, ".", 0); derr != nil {
		t.Fatalf("goose Down до 0: %v", derr)
	}
	noTablesExist(ctx, t, sqlDB, authTables...)
}

// TestIntegration_AuthCRUDRoundtrip — короткий roundtrip sgened sqlc против
// реальной схемы: доказывает, что запросы CRUD валидны (FR A1, A3).
func TestIntegration_AuthCRUDRoundtrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dsn, cleanup := startPostgres(ctx, t)
	defer cleanup()

	// Применяем миграции через database/sql (goose).
	sqlDB, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	goose.SetBaseFS(migrations.FS)
	if derr := goose.SetDialect("postgres"); derr != nil {
		t.Fatalf("goose SetDialect: %v", derr)
	}
	if uperr := goose.UpContext(ctx, sqlDB, "."); uperr != nil {
		t.Fatalf("goose Up: %v", uperr)
	}
	_ = sqlDB.Close()

	// Запросы sqlc работают поверх pgx-пула.
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	defer pool.Close()
	q := db.New(pool)

	// users: создать → прочитать по username.
	created, err := q.CreateUser(ctx, db.CreateUserParams{
		Username:     "alice",
		PasswordHash: "argon2id$hash",
		IsAdmin:      true,
	})
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	got, err := q.GetUserByUsername(ctx, "alice")
	if err != nil {
		t.Fatalf("GetUserByUsername: %v", err)
	}
	if got.ID != created.ID || got.Username != "alice" || !got.IsAdmin {
		t.Fatalf("GetUserByUsername вернул неожиданного пользователя: %+v", got)
	}

	// refresh_tokens: вставить → найти по hash → отозвать.
	expires := pgtype.Timestamptz{Time: time.Now().Add(time.Hour), Valid: true}
	rt, err := q.CreateRefreshToken(ctx, db.CreateRefreshTokenParams{
		UserID:    created.ID,
		TokenHash: "sha256:deadbeef",
		ExpiresAt: expires,
	})
	if err != nil {
		t.Fatalf("CreateRefreshToken: %v", err)
	}
	if rt.Revoked {
		t.Fatal("свежий refresh-токен не должен быть revoked")
	}
	found, err := q.GetRefreshTokenByHash(ctx, "sha256:deadbeef")
	if err != nil {
		t.Fatalf("GetRefreshTokenByHash: %v", err)
	}
	if found.ID != rt.ID {
		t.Fatalf("GetRefreshTokenByHash вернул не тот токен: %+v", found)
	}
	if rerr := q.RevokeRefreshTokenByHash(ctx, "sha256:deadbeef"); rerr != nil {
		t.Fatalf("RevokeRefreshTokenByHash: %v", rerr)
	}
	after, err := q.GetRefreshTokenByHash(ctx, "sha256:deadbeef")
	if err != nil {
		t.Fatalf("GetRefreshTokenByHash после отзыва: %v", err)
	}
	if !after.Revoked {
		t.Fatal("после RevokeRefreshTokenByHash токен должен быть revoked=true")
	}
}
