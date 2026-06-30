//go:build integration

// Integration-тесты bootstrap'а (тикет 1.7, FR A2) на РЕАЛЬНОМ Postgres через
// testcontainers-go (стек — docs/01_tech_stack_and_architecture.md §3).
// Помечены тегом integration, чтобы обычный `make test` (unit) не требовал
// docker; CI-джоба integration гоняет `go test -tags=integration ./...`.
//
// Главный сценарий приёмки тикета — идемпотентность: повторный Run с теми же
// параметрами НЕ должен падать с ошибкой уникальности и НЕ должен создавать
// второго администратора/второй активный токен (см. TestIntegration_Bootstrap_Idempotent).
// Дополнительно проверяется промоушен уже существующего пользователя (созданного
// в обход bootstrap, например обычной регистрацией) до администратора.
//
// Контейнер чистится через testcontainers terminate (defer) + Ryuk reaper —
// после прогона висящих контейнеров не остаётся.
package bootstrap_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/yarabey/agentify/orchestrator/internal/bootstrap"
	"github.com/yarabey/agentify/orchestrator/internal/db"
	"github.com/yarabey/agentify/orchestrator/migrations"
)

const (
	// pgImage тянется через настроенный daemon registry-mirror (тикет 0.3,
	// mirror.gcr.io) — прокси не обходим.
	pgImage = "postgres:16-alpine"
	pgUser  = "bootstrap_test"
	pgPass  = "bootstrap_test"
	pgDB    = "bootstrap_test"

	adminUsername = "admin"
	adminPassword = "s3cr3t-admin-pass"
	initialToken  = "super-secret-initial-registration-token"
)

// setupDB поднимает одиночный Postgres в контейнере, применяет миграции и
// возвращает pgxpool и функцию очистки.
func setupDB(ctx context.Context, t *testing.T) (*pgxpool.Pool, func()) {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image:        pgImage,
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     pgUser,
			"POSTGRES_PASSWORD": pgPass,
			"POSTGRES_DB":       pgDB,
		},
		// ВАЖНО: НЕ используем wait.ForListeningPort — у официального образа
		// postgres entrypoint во время initdb кратко поднимает Postgres,
		// останавливает его и поднимает заново для внешних соединений; порт
		// 5432 в какой-то момент закрывается и переоткрывается, поэтому
		// "слушает порт" может сработать между двумя стартами и первое
		// реальное подключение клиента словит "connection reset by peer"
		// (известная проблема именно с этим образом, см. testcontainers-go
		// issues/docs про postgres wait strategy). Строка "database system is
		// ready to accept connections" печатается в логах ДВАЖДЫ — один раз
		// при временном старте initdb и один раз при финальном — поэтому ждём
		// именно второе вхождение.
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(2 * time.Minute),
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
		t.Fatalf("host контейнера: %v", err)
	}
	port, err := container.MappedPort(ctx, "5432/tcp")
	if err != nil {
		cleanup()
		t.Fatalf("порт контейнера: %v", err)
	}
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		pgUser, pgPass, host, port.Port(), pgDB)

	// Миграции через database/sql (goose), как в internal/migrate.
	sqlDB, err := sql.Open("pgx", dsn)
	if err != nil {
		cleanup()
		t.Fatalf("sql.Open: %v", err)
	}
	goose.SetBaseFS(migrations.FS)
	if derr := goose.SetDialect("postgres"); derr != nil {
		_ = sqlDB.Close()
		cleanup()
		t.Fatalf("goose SetDialect: %v", derr)
	}
	if uperr := goose.UpContext(ctx, sqlDB, "."); uperr != nil {
		_ = sqlDB.Close()
		cleanup()
		t.Fatalf("goose Up: %v", uperr)
	}
	_ = sqlDB.Close()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		cleanup()
		t.Fatalf("pgxpool.New: %v", err)
	}

	return pool, func() {
		pool.Close()
		cleanup()
	}
}

// countRows возвращает количество строк в таблице table — прямой SQL в обход
// sqlc, т.к. нужен только в тесте (доказать отсутствие дублей при идемпотентности).
func countRows(ctx context.Context, t *testing.T, pool *pgxpool.Pool, table string) int {
	t.Helper()
	var n int
	// table контролируется только этим файлом (константы ниже), не вводом — без риска инъекции.
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("count(*) FROM %s: %v", table, err)
	}
	return n
}

// TestIntegration_Bootstrap_Idempotent — приёмка тикета 1.7: повторный
// bootstrap с теми же параметрами не падает и не создаёт дубликаты (ни
// второго администратора, ни второго активного токена регистрации).
func TestIntegration_Bootstrap_Idempotent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupDB(ctx, t)
	defer done()
	q := db.New(pool)

	cfg := bootstrap.Config{
		AdminUsername:            adminUsername,
		AdminPassword:            adminPassword,
		InitialRegistrationToken: initialToken,
	}

	// Первый запуск — создаёт администратора и активный токен.
	if err := bootstrap.Run(ctx, q, nil, cfg); err != nil {
		t.Fatalf("первый Run: %v", err)
	}

	if n := countRows(ctx, t, pool, "users"); n != 1 {
		t.Fatalf("после первого Run users count = %d, ожидалось 1", n)
	}
	if n := countRows(ctx, t, pool, "registration_tokens"); n != 1 {
		t.Fatalf("после первого Run registration_tokens count = %d, ожидалось 1", n)
	}

	user1, err := q.GetUserByUsername(ctx, adminUsername)
	if err != nil {
		t.Fatalf("GetUserByUsername после первого Run: %v", err)
	}
	if !user1.IsAdmin {
		t.Fatal("после первого Run пользователь не администратор")
	}
	if user1.PasswordHash == adminPassword || strings.Contains(user1.PasswordHash, adminPassword) {
		t.Fatalf("пароль администратора сохранён в plaintext (нарушение FR I1): %q", user1.PasswordHash)
	}
	if !strings.HasPrefix(user1.PasswordHash, "$argon2id$") {
		t.Fatalf("пароль администратора не argon2id: %q", user1.PasswordHash)
	}

	token1, err := q.GetActiveRegistrationToken(ctx)
	if err != nil {
		t.Fatalf("GetActiveRegistrationToken после первого Run: %v", err)
	}
	if token1.Token != initialToken {
		t.Fatalf("активный токен = %q, ожидался %q", token1.Token, initialToken)
	}

	// Повторные запуски (минимум дважды, чтобы исключить «повезло один раз») —
	// тот же cfg, та же БД. Ключевая проверка: ни ошибки, ни дублей.
	for i := 0; i < 2; i++ {
		if err := bootstrap.Run(ctx, q, nil, cfg); err != nil {
			t.Fatalf("повторный Run #%d вернул ошибку (идемпотентность нарушена): %v", i+1, err)
		}
	}

	if n := countRows(ctx, t, pool, "users"); n != 1 {
		t.Fatalf("после повторных Run users count = %d, ожидалось 1 (второй администратор не должен создаваться)", n)
	}
	if n := countRows(ctx, t, pool, "registration_tokens"); n != 1 {
		t.Fatalf("после повторных Run registration_tokens count = %d, ожидалось 1 (второй токен не должен создаваться)", n)
	}

	user2, err := q.GetUserByUsername(ctx, adminUsername)
	if err != nil {
		t.Fatalf("GetUserByUsername после повторных Run: %v", err)
	}
	if user2.ID != user1.ID {
		t.Fatalf("после повторных Run id администратора изменился: было %v, стало %v", user1.ID, user2.ID)
	}
	if user2.PasswordHash != user1.PasswordHash {
		t.Fatal("после повторных Run хэш пароля администратора изменился — bootstrap не должен перевыпускать пароль")
	}
	if !user2.IsAdmin {
		t.Fatal("после повторных Run пользователь перестал быть администратором")
	}

	token2, err := q.GetActiveRegistrationToken(ctx)
	if err != nil {
		t.Fatalf("GetActiveRegistrationToken после повторных Run: %v", err)
	}
	if token2.ID != token1.ID {
		t.Fatalf("после повторных Run id активного токена изменился: было %v, стало %v", token1.ID, token2.ID)
	}

	t.Logf("OK: первый Run создал администратора+токен, %d повторных Run не создали дублей и не упали", 2)
}

// TestIntegration_Bootstrap_PromotesExistingUser — пользователь с тем же
// username, что у bootstrap-администратора, уже существует (например, создан
// обычной регистрацией), но не администратор: Run должен довести его до
// администратора, а не пытаться создать второй аккаунт (тоже идемпотентность,
// FR A2).
func TestIntegration_Bootstrap_PromotesExistingUser(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupDB(ctx, t)
	defer done()
	q := db.New(pool)

	// Пользователь заведён в обход bootstrap, не администратор.
	preexisting, err := q.CreateUser(ctx, db.CreateUserParams{
		Username:     adminUsername,
		PasswordHash: "$argon2id$v=19$m=1,t=1,p=1$c2FsdA$aGFzaA",
		IsAdmin:      false,
	})
	if err != nil {
		t.Fatalf("предусловие CreateUser: %v", err)
	}

	cfg := bootstrap.Config{
		AdminUsername:            adminUsername,
		AdminPassword:            adminPassword,
		InitialRegistrationToken: initialToken,
	}
	if err := bootstrap.Run(ctx, q, nil, cfg); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if n := countRows(ctx, t, pool, "users"); n != 1 {
		t.Fatalf("users count = %d, ожидалось 1 (Run не должен создавать второго пользователя)", n)
	}
	promoted, err := q.GetUserByUsername(ctx, adminUsername)
	if err != nil {
		t.Fatalf("GetUserByUsername: %v", err)
	}
	if promoted.ID != preexisting.ID {
		t.Fatal("Run создал нового пользователя вместо промоушена существующего")
	}
	if !promoted.IsAdmin {
		t.Fatal("существующий пользователь не был промоутирован до администратора")
	}
	// Пароль предсуществующего пользователя НЕ должен быть перезаписан паролем
	// из bootstrap-конфига — Run только меняет is_admin, не трогает password_hash.
	if promoted.PasswordHash != preexisting.PasswordHash {
		t.Fatal("Run изменил password_hash существующего пользователя при промоушене — не должен")
	}

	t.Logf("OK: существующий не-админ пользователь промоутирован до администратора без дублей")
}
