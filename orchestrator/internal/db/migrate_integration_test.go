//go:build integration

// Integration-тесты слоя данных auth на РЕАЛЬНОМ Postgres через testcontainers-go
// (тикет 1.1; стек — docs/01_tech_stack_and_architecture.md §3). Помечены тегом
// integration, чтобы обычный `make test` (unit) не требовал docker и был быстрым;
// CI-джоба `integration` гоняет `go test -tags=integration ./...`.
//
// Проверяемые сценарии (приёмка 1.1, плюс приёмка 2.1 — см. ниже):
//   (a) миграция up/down — все goose-миграции применяются «вверх», таблицы auth
//       (users/registration_tokens/refresh_tokens) появляются; затем откат «вниз»
//       до версии 0 проходит без ошибок и удаляет таблицы (FR A1–A4);
//   (b) CRUD-roundtrip sgened sqlc против реальной схемы: создать user → прочитать
//       по username; вставить refresh_token → найти по hash → отозвать (FR A1, A3).
//
// Тикет 2.1 («Схема интеграций», deps: 1.1, миграция 00002_integrations.sql —
// не редактируется этим тикетом, схема уже подготовлена) добавляет к (a)
// проверку таблицы integrations и её ключевых колонок (id, user_id, name,
// ip_hint, uuid_hmac, uuid_enc, status, last_seen_at, created_at, updated_at)
// после up и её исчезновение после down (FR B1–B6, Gherkin §2), а также
// отдельный тест TestIntegration_IntegrationsUUIDHMACUnique — uuid_hmac должен
// быть UNIQUE, что критично для будущей аутентификации машины по HMAC(UUID)
// без коллизий (тикет 2.3, FR B6).
//
// Тикет 5.1 («Схема задач», deps: 1.1, 2.1, миграция 00003_tasks_and_events.sql —
// не редактируется этим тикетом, схема уже подготовлена) добавляет к (a)
// проверку таблиц tasks и task_events и их ключевых колонок после up и их
// исчезновение после down, а также отдельный тест
// TestIntegration_TaskEventsSeqUnique — task_events.seq должен быть UNIQUE в
// рамках task_id, что гарантирует детерминированный порядок событий задачи
// для FSM/аудита (FR F2).
//
// Контейнер чистится через testcontainers terminate (defer) + Ryuk reaper.
package db_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
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

	// pgUniqueViolationCode — SQLSTATE 23505 (unique_violation), используется в
	// TestIntegration_IntegrationsUUIDHMACUnique для проверки UNIQUE на
	// integrations.uuid_hmac (приёмка тикета 2.1).
	pgUniqueViolationCode = "23505"
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

// columnsExist проверяет, что у таблицы есть все перечисленные колонки
// (по information_schema.columns, без проверки типов — точное соответствие
// типам/constraint'ам остаётся на ревью самой миграции; здесь нужна только
// уверенность, что после up схема содержит ожидаемые поля).
func columnsExist(ctx context.Context, t *testing.T, sqlDB *sql.DB, table string, columns ...string) {
	t.Helper()
	for _, col := range columns {
		var exists bool
		err := sqlDB.QueryRowContext(ctx,
			`SELECT EXISTS (SELECT 1 FROM information_schema.columns
			 WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2)`, table, col).Scan(&exists)
		if err != nil {
			t.Fatalf("проверка колонки %s.%s: %v", table, col, err)
		}
		if !exists {
			t.Fatalf("после миграции up колонка %s.%s не найдена", table, col)
		}
	}
}

// TestIntegration_MigrationUpDown — прямая приёмка 1.1 и 2.1: применяем все
// миграции «вверх» (auth-таблицы и таблица integrations появляются), затем
// откатываем «вниз» до 0 без ошибок (все таблицы исчезают).
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

	// Тикет 2.1: таблица integrations (миграция 00002_integrations.sql) —
	// сама таблица и её ключевые колонки, включая поля аутентификации машины
	// uuid_hmac/uuid_enc (FR B1–B6, Gherkin §2).
	const integrationsTable = "integrations"
	tablesExist(ctx, t, sqlDB, integrationsTable)
	columnsExist(ctx, t, sqlDB, integrationsTable,
		"id", "user_id", "name", "ip_hint", "uuid_hmac", "uuid_enc",
		"status", "last_seen_at", "created_at", "updated_at")

	// Тикет 5.1: таблицы tasks и task_events (миграция
	// 00003_tasks_and_events.sql, уже подготовлена и этим тикетом не
	// редактируется) — сами таблицы и их ключевые колонки, включая поле
	// упорядочивания событий seq (FR F1–F2).
	const tasksTable = "tasks"
	const taskEventsTable = "task_events"
	tablesExist(ctx, t, sqlDB, tasksTable, taskEventsTable)
	columnsExist(ctx, t, sqlDB, tasksTable,
		"id", "user_id", "integration_id", "text_enc", "status",
		"idempotency_key", "created_at", "updated_at")
	columnsExist(ctx, t, sqlDB, taskEventsTable,
		"id", "task_id", "seq", "type", "ref_event_id", "payload_enc", "created_at")

	allTables := append(append([]string{}, authTables...), integrationsTable, tasksTable, taskEventsTable)

	// Down: откатываем все миграции до версии 0 — проверяем обратимость схемы.
	if derr := goose.DownToContext(ctx, sqlDB, ".", 0); derr != nil {
		t.Fatalf("goose Down до 0: %v", derr)
	}
	noTablesExist(ctx, t, sqlDB, allTables...)
}

// TestIntegration_IntegrationsUUIDHMACUnique — приёмка 2.1: integrations.uuid_hmac
// обязан быть UNIQUE. Это не декоративное ограничение — будущая аутентификация
// машины (тикет 2.3, FR B6) ищет интеграцию строго по HMAC(UUID) и обязана
// получать не более одной строки; коллизия HMAC двух разных машин не должна
// быть физически представима в схеме. Поднимаем отдельный контейнер (как
// TestIntegration_AuthCRUDRoundtrip/TestIntegration_RLSCrossUserIsolation),
// чтобы тест не зависел от состояния, оставленного TestIntegration_MigrationUpDown.
func TestIntegration_IntegrationsUUIDHMACUnique(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dsn, cleanup := startPostgres(ctx, t)
	defer cleanup()

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

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	defer pool.Close()

	// integrations.user_id — NOT NULL FK на users(id), нужен реальный владелец
	// для обеих вставок (используем sqlc-запрос CreateUser, как и соседние тесты).
	q := db.New(pool)
	owner, err := q.CreateUser(ctx, db.CreateUserParams{
		Username:     "uuid-hmac-owner",
		PasswordHash: "argon2id$stub",
		IsAdmin:      false,
	})
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	const sameHMAC = "hmac-duplicate-probe"
	// Первая вставка с этим uuid_hmac должна пройти без ошибок.
	_, err = pool.Exec(ctx, `
		INSERT INTO integrations (user_id, name, uuid_hmac, uuid_enc)
		VALUES ($1, 'machine-one', $2, $3)`,
		owner.ID, sameHMAC, []byte("ciphertext-one"))
	if err != nil {
		t.Fatalf("первая вставка integrations с uuid_hmac=%q: %v", sameHMAC, err)
	}

	// Вторая вставка с ТЕМ ЖЕ uuid_hmac (другие name/uuid_enc, тот же владелец)
	// обязана упасть с нарушением уникальности (Postgres SQLSTATE 23505).
	_, err = pool.Exec(ctx, `
		INSERT INTO integrations (user_id, name, uuid_hmac, uuid_enc)
		VALUES ($1, 'machine-two', $2, $3)`,
		owner.ID, sameHMAC, []byte("ciphertext-two"))
	if err == nil {
		t.Fatal("вставка дубликата uuid_hmac прошла без ошибки — UNIQUE-ограничение не работает")
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("ожидалась ошибка Postgres (*pgconn.PgError) при дубликате uuid_hmac, получено: %v", err)
	}
	if pgErr.Code != pgUniqueViolationCode {
		t.Fatalf("ожидался SQLSTATE %s (unique_violation) при дубликате uuid_hmac, получено %s: %v",
			pgUniqueViolationCode, pgErr.Code, pgErr)
	}

	// Контрольная вставка с ДРУГИМ uuid_hmac (тот же владелец) обязана пройти —
	// доказывает, что отказ выше вызван именно дубликатом uuid_hmac, а не
	// случайной поломкой INSERT/FK для этого пользователя.
	_, err = pool.Exec(ctx, `
		INSERT INTO integrations (user_id, name, uuid_hmac, uuid_enc)
		VALUES ($1, 'machine-three', 'hmac-distinct-probe', $2)`,
		owner.ID, []byte("ciphertext-three"))
	if err != nil {
		t.Fatalf("вставка integrations с уникальным uuid_hmac неожиданно упала: %v", err)
	}
}

// TestIntegration_TaskEventsSeqUnique — приёмка 5.1: task_events.seq обязан
// быть UNIQUE в рамках одной задачи (уникальный индекс uq_task_events_seq на
// (task_id, seq)). Это не декоративное ограничение: seq задаёт единственно
// верный порядок событий задачи, на который опирается и FSM (последовательная
// обработка команд/ответов агента), и аудит/replay истории задачи для
// пользователя (FR F2) — при коллизии seq порядок стал бы недетерминированным.
// Поднимаем отдельный контейнер (как TestIntegration_IntegrationsUUIDHMACUnique),
// чтобы тест не зависел от состояния, оставленного другими тестами файла.
func TestIntegration_TaskEventsSeqUnique(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dsn, cleanup := startPostgres(ctx, t)
	defer cleanup()

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

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	defer pool.Close()

	// tasks.user_id/integration_id — NOT NULL FK, нужны реальные владелец и
	// интеграция для вставки задачи (владелец — через sqlc CreateUser, как и
	// соседние тесты; интеграция — прямым INSERT, как в
	// TestIntegration_IntegrationsUUIDHMACUnique, отдельного sqlc-запроса для
	// этого в файле нет и он не нужен для 5.1).
	q := db.New(pool)
	owner, err := q.CreateUser(ctx, db.CreateUserParams{
		Username:     "seq-test-owner",
		PasswordHash: "argon2id$stub",
		IsAdmin:      false,
	})
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	var integrationID pgtype.UUID
	err = pool.QueryRow(ctx, `
		INSERT INTO integrations (user_id, name, uuid_hmac, uuid_enc)
		VALUES ($1, 'seq-test-machine', 'seq-test-hmac-probe', $2)
		RETURNING id`,
		owner.ID, []byte("ciphertext-stub")).Scan(&integrationID)
	if err != nil {
		t.Fatalf("вставка integrations для теста seq: %v", err)
	}

	var taskID pgtype.UUID
	err = pool.QueryRow(ctx, `
		INSERT INTO tasks (user_id, integration_id, text_enc)
		VALUES ($1, $2, $3)
		RETURNING id`,
		owner.ID, integrationID, []byte("ciphertext-stub")).Scan(&taskID)
	if err != nil {
		t.Fatalf("вставка tasks для теста seq: %v", err)
	}

	// Первая вставка task_events с seq=1 должна пройти без ошибок.
	_, err = pool.Exec(ctx, `
		INSERT INTO task_events (task_id, seq, type, payload_enc)
		VALUES ($1, 1, 'status_change', $2)`,
		taskID, []byte("payload-one"))
	if err != nil {
		t.Fatalf("первая вставка task_events с seq=1: %v", err)
	}

	// Вторая вставка с ТЕМ ЖЕ task_id и ТЕМ ЖЕ seq=1 (другой payload_enc)
	// обязана упасть с нарушением уникальности (Postgres SQLSTATE 23505).
	_, err = pool.Exec(ctx, `
		INSERT INTO task_events (task_id, seq, type, payload_enc)
		VALUES ($1, 1, 'status_change', $2)`,
		taskID, []byte("payload-one-duplicate"))
	if err == nil {
		t.Fatal("вставка дубликата (task_id, seq) прошла без ошибки — UNIQUE-ограничение не работает")
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("ожидалась ошибка Postgres (*pgconn.PgError) при дубликате seq, получено: %v", err)
	}
	if pgErr.Code != pgUniqueViolationCode {
		t.Fatalf("ожидался SQLSTATE %s (unique_violation) при дубликате seq, получено %s: %v",
			pgUniqueViolationCode, pgErr.Code, pgErr)
	}

	// Контрольная вставка с тем же task_id, но seq=2 обязана пройти —
	// доказывает, что отказ выше вызван именно дубликатом seq в рамках
	// задачи, а не случайной поломкой INSERT/FK.
	_, err = pool.Exec(ctx, `
		INSERT INTO task_events (task_id, seq, type, payload_enc)
		VALUES ($1, 2, 'status_change', $2)`,
		taskID, []byte("payload-two"))
	if err != nil {
		t.Fatalf("вставка task_events с уникальным seq=2 неожиданно упала: %v", err)
	}
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
