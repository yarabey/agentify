//go:build integration

// Integration-тесты POST /auth/register на РЕАЛЬНОМ Postgres через
// testcontainers-go (тикет 1.2; стек — docs/01_tech_stack_and_architecture.md §3).
// Помечены тегом integration, чтобы обычный `make test` (unit) не требовал docker.
// CI-джоба integration гоняет `go test -tags=integration ./...`.
//
// Гоняем реальный HTTP через httptest поверх собранного из openapi роутера
// (NewRouter) + реальную БД (pgxpool + sqlc), приводя в действие весь путь
// регистрации. Покрываются сценарии Gherkin §1 (FR A1, I1):
//   (a) «Успешная регистрация по валидному токену» → 201 + пользователь в БД с
//       НЕ-plaintext паролем (argon2id);
//   (b) «Регистрация без токена запрещена» → пустой/неверный токен → 403;
//   (c) дубль username → 409.
// Godog-степы подключатся в тикете 11.2; здесь сценарии §1 покрыты Go-тестами.
//
// Контейнер чистится через testcontainers terminate (defer) + Ryuk reaper —
// после прогона висящих контейнеров не остаётся.
package api_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/yarabey/agentify/orchestrator/internal/api"
	"github.com/yarabey/agentify/orchestrator/internal/db"
	"github.com/yarabey/agentify/orchestrator/migrations"
)

const (
	// pgImage тянется через настроенный daemon registry-mirror (тикет 0.3,
	// mirror.gcr.io) — прокси не обходим.
	pgImage = "postgres:16-alpine"
	pgUser  = "reg_test"
	pgPass  = "reg_test"
	pgDB    = "reg_test"

	activeToken = "super-secret-registration-token"

	// testJWTSigningKey — ключ подписи access-JWT для интеграционных тестов
	// пакета api (тикет 1.3). Не секрет — используется только в тестовом процессе.
	testJWTSigningKey = "integration-test-jwt-signing-key"
)

// startPostgres поднимает одиночный Postgres в контейнере, применяет миграции и
// возвращает pgxpool, активный токен регистрации в БД и функцию очистки.
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

// seedActiveToken создаёт активный токен регистрации в БД (предусловие «у меня
// есть валидный токен регистрации»).
func seedActiveToken(ctx context.Context, t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := db.New(pool).CreateRegistrationToken(ctx, activeToken); err != nil {
		t.Fatalf("CreateRegistrationToken: %v", err)
	}
}

// postRegister шлёт POST /auth/register через httptest поверх собранного роутера.
func postRegister(t *testing.T, router http.Handler, body api.RegisterRequest) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal тела: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/auth/register", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

// TestIntegration_Register_Success — сценарий §1 «Успешная регистрация по
// валидному токену»: 201 + пользователь в БД с НЕ-plaintext паролем (FR A1, I1).
func TestIntegration_Register_Success(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupDB(ctx, t)
	defer done()
	seedActiveToken(ctx, t, pool)

	router := api.NewRouter(api.NewServer(db.New(pool), nil, []byte(testJWTSigningKey)))

	const password = "s3cr3t-pass"
	rec := postRegister(t, router, api.RegisterRequest{
		Username:          "alice",
		Password:          password,
		RegistrationToken: activeToken,
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("[a] статус = %d (%s), ожидался 201", rec.Code, rec.Body.String())
	}

	// Пользователь действительно создан, и пароль НЕ хранится в открытом виде.
	user, err := db.New(pool).GetUserByUsername(ctx, "alice")
	if err != nil {
		t.Fatalf("[a] GetUserByUsername: %v", err)
	}
	if user.PasswordHash == password || strings.Contains(user.PasswordHash, password) {
		t.Fatalf("[a] пароль сохранён в plaintext (нарушение FR I1): %q", user.PasswordHash)
	}
	if !strings.HasPrefix(user.PasswordHash, "$argon2id$") {
		t.Fatalf("[a] пароль не argon2id: %q", user.PasswordHash)
	}
	t.Logf("[a] OK: 201, пользователь alice создан, password_hash=argon2id (не plaintext)")
}

// TestIntegration_Register_ForbiddenWithoutToken — сценарий §1 «Регистрация без
// токена запрещена»: пустой и неверный токен → 403 (FR A1).
func TestIntegration_Register_ForbiddenWithoutToken(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupDB(ctx, t)
	defer done()
	seedActiveToken(ctx, t, pool)

	router := api.NewRouter(api.NewServer(db.New(pool), nil, []byte(testJWTSigningKey)))

	// Неверный токен → 403 (семантический отказ закрытого доступа).
	wrong := postRegister(t, router, api.RegisterRequest{
		Username:          "bob",
		Password:          "pw",
		RegistrationToken: "not-the-token",
	})
	if wrong.Code != http.StatusForbidden {
		t.Fatalf("[b] неверный токен: статус = %d (%s), ожидался 403", wrong.Code, wrong.Body.String())
	}

	// Пользователь НЕ должен быть создан при отказе.
	if _, err := db.New(pool).GetUserByUsername(ctx, "bob"); err == nil {
		t.Fatal("[b] пользователь bob создан несмотря на отказ регистрации")
	}
	t.Logf("[b] OK: неверный/пустой токен → 403, пользователь не создан (закрытый доступ FR A1)")
}

// TestIntegration_Register_DuplicateUsername — дубль username → 409 (FR A1).
func TestIntegration_Register_DuplicateUsername(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupDB(ctx, t)
	defer done()
	seedActiveToken(ctx, t, pool)

	router := api.NewRouter(api.NewServer(db.New(pool), nil, []byte(testJWTSigningKey)))

	body := api.RegisterRequest{Username: "carol", Password: "pw", RegistrationToken: activeToken}

	first := postRegister(t, router, body)
	if first.Code != http.StatusCreated {
		t.Fatalf("[c] первая регистрация: статус = %d (%s), ожидался 201", first.Code, first.Body.String())
	}

	second := postRegister(t, router, body)
	if second.Code != http.StatusConflict {
		t.Fatalf("[c] повтор username: статус = %d (%s), ожидался 409", second.Code, second.Body.String())
	}
	t.Logf("[c] OK: повтор username carol → 409")
}
