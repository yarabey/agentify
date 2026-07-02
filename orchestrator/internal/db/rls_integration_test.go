//go:build integration

// Integration-тест Row Level Security на РЕАЛЬНОМ Postgres через
// testcontainers-go (тикет 1.5, FR A4, I3, Gherkin §1 «Изоляция данных между
// пользователями»). Тег integration — как и migrate_integration_test.go
// (тикет 1.1): обычный `make test` (unit) docker не требует, CI-джоба
// `integration` гоняет `go test -tags=integration ./...`.
//
// Что доказывает (приёмка 1.5): два пользователя, перекрёстный доступ к
// строке integrations другого пользователя — пусто; дополнительно — без
// выставленной app.user_id доступ запрещён ко всем строкам (fail-closed,
// см. orchestrator/migrations/00005_rls.sql, missing_ok=true).
//
// КРИТИЧЕСКИЙ нюанс этого теста (без него тест был бы ложно-зелёным): роль из
// POSTGRES_USER в официальном образе postgres — суперпользователь И владелец
// всех объектов, которые создаёт (в т.ч. таблиц через goose-миграции).
// Postgres НИКОГДА не применяет RLS-политики к владельцу таблицы или
// суперпользователю — даже с FORCE ROW LEVEL SECURITY (это поведение самого
// Postgres, см. комментарий в 00005_rls.sql). Поэтому миграции применяются
// admin-ролью (как в migrate_integration_test.go), но сама проверка RLS
// идёт через ОТДЕЛЬНУЮ непривилегированную роль app_runtime, созданную здесь
// же (GRANT, не SUPERUSER, не владелец таблиц) — именно её используют
// CRUD-обработчики tasks/integrations в будущих тикетах EPIC 2/5.
package db_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
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
	// rlsPgImage/rlsPgUser/... — отдельные константы (не переиспользуем
	// pgImage/pgUser/... из migrate_integration_test.go), чтобы тест этого
	// файла не зависел от соседнего и не ломался при независимых правках.
	rlsPgImage = "postgres:16-alpine"
	rlsPgUser  = "rls_admin"
	rlsPgPass  = "rls_admin"
	rlsPgDB    = "rls_test"

	// appRuntimeUser/appRuntimePass — непривилегированная роль БД, от имени
	// которой в проде ходит само приложение (см. godoc файла выше): не
	// суперпользователь и не владелец таблиц, поэтому RLS-политики на
	// tasks/integrations к ней реально применяются.
	appRuntimeUser = "app_runtime"
	appRuntimePass = "app_runtime_pw"
)

// startRLSPostgres — копия startPostgres из migrate_integration_test.go под
// отдельные константы этого файла (см. комментарий к ним выше); сама логика
// и обоснование wait-стратегии (двойное вхождение лога) идентичны —
// см. подробный комментарий в migrate_integration_test.go.
//
// В отличие от startPostgres, возвращает не один DSN, а функцию dsnFor(user,
// pass) — тесту нужно подключаться под разными ролями (admin-роль rls_admin
// для миграций и непривилегированная app_runtime для самой проверки RLS, см.
// godoc файла) к одному и тому же контейнеру/базе.
func startRLSPostgres(ctx context.Context, t *testing.T) (dsnFor func(user, pass string) string, cleanup func()) {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image:        rlsPgImage,
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     rlsPgUser,
			"POSTGRES_PASSWORD": rlsPgPass,
			"POSTGRES_DB":       rlsPgDB,
		},
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
	cleanup = func() {
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
	dsnFor = func(user, pass string) string {
		return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
			url.QueryEscape(user), url.QueryEscape(pass), host, port.Port(), rlsPgDB)
	}
	return dsnFor, cleanup
}

// TestIntegration_RLSCrossUserIsolation — приёмка тикета 1.5: два
// пользователя, перекрёстный доступ → пусто (FR A4, I3, Gherkin §1); плюс
// fail-closed без выставленной app.user_id (см. godoc файла).
func TestIntegration_RLSCrossUserIsolation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dsnFor, cleanup := startRLSPostgres(ctx, t)
	defer cleanup()
	adminDSN := dsnFor(rlsPgUser, rlsPgPass)

	// --- 1. Миграции admin/owner-ролью (rls_admin) — как в migrate_integration_test.go. ---
	sqlDB, err := sql.Open("pgx", adminDSN)
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

	adminPool, err := pgxpool.New(ctx, adminDSN)
	if err != nil {
		t.Fatalf("pgxpool.New (admin): %v", err)
	}
	defer adminPool.Close()

	// --- 2. Непривилегированная роль app_runtime: не суперпользователь, не
	// владелец таблиц — именно поэтому RLS к ней применяется (см. godoc файла). ---
	_, err = adminPool.Exec(ctx, fmt.Sprintf(
		`CREATE ROLE %s LOGIN PASSWORD %s NOSUPERUSER`, appRuntimeUser, quoteLiteral(appRuntimePass)))
	if err != nil {
		t.Fatalf("CREATE ROLE app_runtime: %v", err)
	}
	for _, stmt := range []string{
		fmt.Sprintf(`GRANT USAGE ON SCHEMA public TO %s`, appRuntimeUser),
		fmt.Sprintf(`GRANT SELECT, INSERT, UPDATE, DELETE ON tasks, integrations TO %s`, appRuntimeUser),
		// users нужен только для FK-проверки при INSERT в integrations — самих
		// пользователей создаём ещё под admin-ролью ниже, до переключения на
		// app_runtime (так проще, и не входит в предмет этого тикета — CRUD
		// users не меняется тикетом 1.5).
		fmt.Sprintf(`GRANT SELECT ON users TO %s`, appRuntimeUser),
	} {
		if _, err = adminPool.Exec(ctx, stmt); err != nil {
			t.Fatalf("GRANT app_runtime (%q): %v", stmt, err)
		}
	}

	// --- 3. Два реальных пользователя (нужны валидные user_id для FK на
	// integrations.user_id) — создаём ещё admin-ролью, проще и не предмет
	// этого тикета. ---
	q := db.New(adminPool)
	userA, err := q.CreateUser(ctx, db.CreateUserParams{
		Username:     "rls-alice",
		PasswordHash: "argon2id$stub-a",
		IsAdmin:      false,
	})
	if err != nil {
		t.Fatalf("CreateUser alice: %v", err)
	}
	userB, err := q.CreateUser(ctx, db.CreateUserParams{
		Username:     "rls-bob",
		PasswordHash: "argon2id$stub-b",
		IsAdmin:      false,
	})
	if err != nil {
		t.Fatalf("CreateUser bob: %v", err)
	}
	aliceID := pgtypeUUIDToUUID(userA.ID)
	bobID := pgtypeUUIDToUUID(userB.ID)

	// --- 4. Отдельный пул ОТ ИМЕНИ app_runtime — тот же контейнер/база, другая роль/пароль. ---
	runtimePool, err := pgxpool.New(ctx, dsnFor(appRuntimeUser, appRuntimePass))
	if err != nil {
		t.Fatalf("pgxpool.New (app_runtime): %v", err)
	}
	defer runtimePool.Close()

	// --- 4б. Fail-closed на ЕЩЁ НИ РАЗУ не использованном соединении пула —
	// сделать ДО первого вызова SetAppUserID на этом пуле, это важно для
	// корректности проверки (см. развёрнутый комментарий ниже и в rls.go,
	// раздел "Нюанс пула соединений"): current_setting('app.user_id', true)
	// возвращает NULL, только пока в текущей физической сессии (соединении)
	// set_config для этого имени НИ РАЗУ не вызывался. Сравнение
	// `user_id = NULL` даёт NULL (не TRUE) — Postgres тихо не возвращает ни
	// одной строки, без ошибки SQL. Именно это поведение описано в
	// orchestrator/migrations/00005_rls.sql ("current_setting возвращает
	// NULL... не падает с ошибкой") — но оно справедливо только для первого
	// использования соединения, см. пункт 7 ниже.
	preTouchTx, err := runtimePool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin (fail-closed, untouched connection): %v", err)
	}
	var preTouchCount int
	if qerr := preTouchTx.QueryRow(ctx, `SELECT count(*) FROM integrations`).Scan(&preTouchCount); qerr != nil {
		t.Fatalf("SELECT count(*) integrations (fail-closed, untouched connection): %v", qerr)
	}
	if preTouchCount != 0 {
		t.Fatalf("fail-closed нарушен на свежем соединении: видно %d строк(и), ожидалось 0", preTouchCount)
	}
	if cerr := preTouchTx.Commit(ctx); cerr != nil {
		t.Fatalf("Commit (fail-closed, untouched connection): %v", cerr)
	}

	// --- 5. Вставляем интеграцию ОТ ИМЕНИ alice: SetAppUserID(alice) в
	// транзакции app_runtime-пула, INSERT, commit. ---
	insertTx, err := runtimePool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin (insert as alice): %v", err)
	}
	if serr := db.SetAppUserID(ctx, insertTx, aliceID); serr != nil {
		t.Fatalf("SetAppUserID(alice): %v", serr)
	}
	_, err = insertTx.Exec(ctx, `
		INSERT INTO integrations (user_id, name, uuid_hmac, uuid_enc)
		VALUES ($1, 'alice-laptop', 'hmac-alice-laptop', $2)`,
		aliceID, []byte("ciphertext-stub"))
	if err != nil {
		t.Fatalf("INSERT integrations as alice: %v", err)
	}
	if cerr := insertTx.Commit(ctx); cerr != nil {
		t.Fatalf("Commit (insert as alice): %v", cerr)
	}

	// --- 6. Перекрёстный доступ: НОВАЯ транзакция, SetAppUserID(bob), SELECT
	// по integrations alice → должно быть пусто (приёмка тикета 1.5). ---
	crossTx, err := runtimePool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin (cross-access as bob): %v", err)
	}
	if serr := db.SetAppUserID(ctx, crossTx, bobID); serr != nil {
		t.Fatalf("SetAppUserID(bob): %v", serr)
	}
	var crossCount int
	if qerr := crossTx.QueryRow(ctx, `SELECT count(*) FROM integrations WHERE user_id = $1`, aliceID).
		Scan(&crossCount); qerr != nil {
		t.Fatalf("SELECT integrations as bob (cross-access): %v", qerr)
	}
	if crossCount != 0 {
		t.Fatalf("RLS не сработал: bob увидел %d строк(и) integrations alice, ожидалось 0", crossCount)
	}
	// Тот же SELECT без фильтра user_id в самом запросе — на случай, если
	// кто-то решит, что приложенческий фильтр сам всё решает: RLS обязан
	// скрыть строку alice независимо от WHERE-условия запроса.
	var crossAllCount int
	if qerr := crossTx.QueryRow(ctx, `SELECT count(*) FROM integrations`).Scan(&crossAllCount); qerr != nil {
		t.Fatalf("SELECT count(*) integrations as bob: %v", qerr)
	}
	if crossAllCount != 0 {
		t.Fatalf("RLS не сработал: bob видит %d строк(и) integrations без явного фильтра, ожидалось 0", crossAllCount)
	}
	if cerr := crossTx.Commit(ctx); cerr != nil {
		t.Fatalf("Commit (cross-access as bob): %v", cerr)
	}

	// --- 6б. Контроль: alice в своей транзакции ДОЛЖНА видеть свою же строку —
	// иначе пункт 6 мог бы «пройти» просто потому, что INSERT не сохранился. ---
	ownTx, err := runtimePool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin (own-access as alice): %v", err)
	}
	if serr := db.SetAppUserID(ctx, ownTx, aliceID); serr != nil {
		t.Fatalf("SetAppUserID(alice, own-access): %v", serr)
	}
	var ownCount int
	if qerr := ownTx.QueryRow(ctx, `SELECT count(*) FROM integrations WHERE user_id = $1`, aliceID).
		Scan(&ownCount); qerr != nil {
		t.Fatalf("SELECT integrations as alice (own-access): %v", qerr)
	}
	if ownCount != 1 {
		t.Fatalf("alice не видит свою же строку integrations: count=%d, ожидалось 1", ownCount)
	}
	if cerr := ownTx.Commit(ctx); cerr != nil {
		t.Fatalf("Commit (own-access as alice): %v", cerr)
	}

	// --- 7. Fail-closed на УЖЕ использованном (тем же физическим соединением)
	// пуле, если забыть вызвать SetAppUserID, — реалистичный сценарий
	// "обработчик забыл вызвать SetAppUserID" при переиспользовании
	// соединений pgxpool. ВАЖНО (расхождение с буквальным текстом комментария
	// в 00005_rls.sql, обнаружено эмпирически при написании этого теста —
	// см. идентичный комментарий в rls.go, раздел "Нюанс пула соединений"):
	// после ПЕРВОГО set_config('app.user_id', ..., true) в рамках сессии
	// Postgres заводит у себя placeholder для этого custom-GUC; при ROLLBACK
	// is_local-значения откатываются обратно, но НЕ к "переменная не
	// существует" (что давало бы NULL), а к '' (пустая строка) — т.к.
	// placeholder для имени уже создан. current_setting(..., true) поэтому
	// возвращает '' вместо NULL, а `''::uuid` в политике падает с ошибкой
	// Postgres 22P02 (invalid_text_representation), а не тихо отдаёт 0 строк.
	// Это РАВНОЦЕННО fail-closed с точки зрения безопасности — чужая строка
	// никогда не попадает в результат, запрос целиком обрывается ошибкой —
	// но НЕ совпадает с идеализированным "тихий 0 без ошибки" из комментария
	// миграции. Практический вывод для будущих обработчиков EPIC 2/5: нельзя
	// полагаться на "забыл SetAppUserID = аккуратные пустые 404", это может
	// быть 500 — поэтому SetAppUserID обязателен в начале КАЖДОЙ транзакции,
	// без исключений, а не "не помешает, но не критично".
	noVarTx, err := runtimePool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin (no app.user_id set, reused connection): %v", err)
	}
	var noVarCount int
	qerr := noVarTx.QueryRow(ctx, `SELECT count(*) FROM integrations`).Scan(&noVarCount)
	_ = noVarTx.Rollback(ctx) // транзакция либо уже упала с ошибкой на сервере, либо безвредна для rollback в любом случае
	switch {
	case qerr == nil && noVarCount != 0:
		t.Fatalf("fail-closed нарушен: без app.user_id (переиспользованное соединение) видно %d строк(и), ожидалось 0 или ошибка", noVarCount)
	case qerr == nil:
		// 0 строк без ошибки — тоже валидный fail-closed исход (напр. если
		// Postgres в этой версии/конфигурации иначе обходится с placeholder-GUC).
	default:
		// Ошибка SQL (как правило 22P02 invalid_text_representation, см.
		// комментарий выше) — тоже валидный fail-closed исход: запрос не
		// вернул ни одной чужой строки, т.к. не вернул строк вообще.
		t.Logf("fail-closed (переиспользованное соединение) сработал через SQL-ошибку, не тихий 0: %v", qerr)
	}
}

// pgtypeUUIDToUUID конвертирует pgtype.UUID (тип, который возвращают
// sqlc-модели, напр. db.CreateUser) в google/uuid.UUID — тип, который
// принимает db.SetAppUserID (см. rls.go) и который реально кладёт в контекст
// запроса auth-middleware (orchestrator/internal/api/middleware.go,
// UserIDFromContext). Оба типа поверх одних и тех же 16 байт UUID, поэтому
// конвертация — простое копирование без потери информации; created.ID.Valid
// здесь всегда true (только что вернул RETURNING после успешного INSERT).
func pgtypeUUIDToUUID(id pgtype.UUID) uuid.UUID {
	return uuid.UUID(id.Bytes)
}

// quoteLiteral экранирует строковый литерал для подстановки в DDL (CREATE
// ROLE ... PASSWORD не поддерживает bind-параметры $1 — пароль вынужденно
// идёт в текст SQL, в отличие от обычных DML-запросов в этом тесте, которые
// все параметризованы). Удваивает одинарные кавычки по правилам
// SQL-строкового литерала; appRuntimePass — константа без кавычек/спецсимволов,
// поэтому риска инъекции на практике нет, но экранирование сделано корректно,
// а не «просто потому что мы знаем, что там нет кавычек».
func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
