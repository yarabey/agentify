//go:build integration

// Integration-тесты channel.Linker.Exchange на РЕАЛЬНОМ Postgres через
// testcontainers-go (тикет 10.2, FR D3 «привязка канала к аккаунту описана
// явно», Gherkin §6 «Уведомления», предусловие «мой Telegram привязан к
// аккаунту») — тот же паттерн, что и
// orchestrator/internal/task/transition_integration_test.go: собственный
// startPostgres/setupPool-хелпер (пакеты оркестратора не шарят его
// специально, см. годок transition_integration_test.go), тег integration,
// чтобы `make test` (unit) оставался быстрым и не требовал docker; CI-джоба
// `integration` гоняет `go test -tags=integration ./...`.
//
// Приёмка тикета 10.2:
//   - валидный код привязывает: Exchange создаёт РОВНО одну строку
//     channel_links с ожидаемыми user_id/channel/external_id, и код
//     помечается использованным (used_at проставлен);
//   - истёкший код → ошибка (ErrLinkCodeExpired), БЕЗ привязки: ни одной
//     строки channel_links не появляется, used_at кода остаётся NULL (код
//     можно было бы использовать заново, если бы не истёк, — попытка не
//     сжигает истёкший код);
//   - повторное использование уже использованного кода → ошибка
//     (ErrLinkCodeUsed), вторая попытка тоже не создаёт вторую строку
//     channel_links.
package channel_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/yarabey/agentify/orchestrator/internal/channel"
	"github.com/yarabey/agentify/orchestrator/internal/db"
	"github.com/yarabey/agentify/orchestrator/migrations"
)

const (
	// pgImage тянется через настроенный daemon registry-mirror (тикет 0.3,
	// mirror.gcr.io) — прокси не обходим.
	pgImage = "postgres:16-alpine"
	pgUser  = "channel_test"
	pgPass  = "channel_test"
	pgDB    = "channel_test"
)

// startPostgres поднимает одиночный Postgres в контейнере и возвращает строку
// подключения (pgx/libpq URL) и функцию очистки (terminate). Копия паттерна
// orchestrator/internal/task/transition_integration_test.go.
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
		// ВАЖНО: НЕ используем wait.ForListeningPort — см. обоснование в
		// orchestrator/internal/db/migrate_integration_test.go (образ postgres
		// печатает готовность дважды: временный старт initdb + финальный).
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

// setupPool поднимает Postgres, применяет миграции goose и возвращает готовый
// *pgxpool.Pool + функцию очистки (закрывает пул и завершает контейнер).
func setupPool(ctx context.Context, t *testing.T) (*pgxpool.Pool, func()) {
	t.Helper()
	dsn, cleanupContainer := startPostgres(ctx, t)

	sqlDB, err := sql.Open("pgx", dsn)
	if err != nil {
		cleanupContainer()
		t.Fatalf("sql.Open: %v", err)
	}
	goose.SetBaseFS(migrations.FS)
	if derr := goose.SetDialect("postgres"); derr != nil {
		cleanupContainer()
		t.Fatalf("goose SetDialect: %v", derr)
	}
	if uperr := goose.UpContext(ctx, sqlDB, "."); uperr != nil {
		cleanupContainer()
		t.Fatalf("goose Up: %v", uperr)
	}
	_ = sqlDB.Close()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		cleanupContainer()
		t.Fatalf("pgxpool.New: %v", err)
	}
	return pool, func() {
		pool.Close()
		cleanupContainer()
	}
}

// randomCode генерирует случайный hex-код для тестовых строк
// channel_link_codes.code — уникальность между тестами/строками не важна для
// PRIMARY KEY на code, важна лишь непредсказуемость в духе реального 9.6
// (здесь достаточно случайности ради уникальности между подтестами).
func randomCode(t *testing.T) string {
	t.Helper()
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("crypto/rand.Read: %v", err)
	}
	return hex.EncodeToString(b)
}

// seedUser создаёт владельца кода привязки — предусловие всех сценариев
// этого файла.
func seedUser(ctx context.Context, t *testing.T, q *db.Queries, username string) db.User {
	t.Helper()
	user, err := q.CreateUser(ctx, db.CreateUserParams{
		Username:     username,
		PasswordHash: "argon2id$stub",
		IsAdmin:      false,
	})
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	return user
}

// seedLinkCode вставляет код привязки напрямую через sqlc (тикет 10.2 не
// реализует HTTP-эндпоинт генерации кода — это тикет 9.6; здесь код
// создаётся как тестовое предусловие, ровно как предписано брифом тикета).
func seedLinkCode(ctx context.Context, t *testing.T, q *db.Queries, userID pgtype.UUID, ttl time.Duration) string {
	t.Helper()
	code := randomCode(t)
	if _, err := q.CreateChannelLinkCode(ctx, db.CreateChannelLinkCodeParams{
		Code:      code,
		UserID:    userID,
		Channel:   "telegram",
		ExpiresAt: pgtype.Timestamptz{Time: time.Now().Add(ttl), Valid: true},
	}); err != nil {
		t.Fatalf("CreateChannelLinkCode: %v", err)
	}
	return code
}

// countChannelLinks возвращает число строк channel_links для данного
// external_id — используется, чтобы убедиться, что неудачная попытка
// Exchange НЕ создала привязку.
func countChannelLinks(ctx context.Context, t *testing.T, pool *pgxpool.Pool, externalID string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM channel_links WHERE external_id = $1`, externalID).Scan(&n); err != nil {
		t.Fatalf("count channel_links: %v", err)
	}
	return n
}

// TestIntegration_Exchange_ValidCodeLinksAccount — валидный код привязывает:
// Exchange создаёт РОВНО одну строку channel_links с ожидаемыми
// user_id/channel/external_id, код помечается использованным (FR D3).
func TestIntegration_Exchange_ValidCodeLinksAccount(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupPool(ctx, t)
	defer done()
	q := db.New(pool)

	user := seedUser(ctx, t, q, "alice")
	code := seedLinkCode(ctx, t, q, user.ID, time.Hour)

	linker := channel.NewLinker(pool)
	link, err := linker.Exchange(ctx, "telegram", code, "tg-42")
	if err != nil {
		t.Fatalf("Exchange: %v, ожидался успех", err)
	}
	if link.UserID != user.ID {
		t.Fatalf("link.UserID = %v, ожидался %v", link.UserID, user.ID)
	}
	if link.Channel != "telegram" || link.ExternalID != "tg-42" {
		t.Fatalf("link = %+v, ожидались channel=telegram external_id=tg-42", link)
	}

	if got := countChannelLinks(ctx, t, pool, "tg-42"); got != 1 {
		t.Fatalf("channel_links(external_id=tg-42) = %d строк, ожидалась 1", got)
	}

	// Код должен быть помечен использованным (одноразовость, Gherkin §6).
	usedCode, err := q.GetChannelLinkCode(ctx, code)
	if err != nil {
		t.Fatalf("GetChannelLinkCode: %v", err)
	}
	if !usedCode.UsedAt.Valid {
		t.Fatal("used_at не проставлен после успешного Exchange")
	}
	t.Logf("OK: валидный код привязал tg-42 → %s, код помечен использованным", user.ID)
}

// TestIntegration_Exchange_ExpiredCodeFails — истёкший код → ошибка
// (ErrLinkCodeExpired), БЕЗ привязки: ни одной строки channel_links, used_at
// остаётся NULL (приёмка тикета 10.2: истёкший код отклоняется, без
// привязки).
func TestIntegration_Exchange_ExpiredCodeFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupPool(ctx, t)
	defer done()
	q := db.New(pool)

	user := seedUser(ctx, t, q, "bob")
	// TTL отрицательный: expires_at уже в прошлом на момент вставки.
	code := seedLinkCode(ctx, t, q, user.ID, -time.Hour)

	linker := channel.NewLinker(pool)
	_, err := linker.Exchange(ctx, "telegram", code, "tg-43")
	if !errors.Is(err, channel.ErrLinkCodeExpired) {
		t.Fatalf("Exchange err = %v, ожидался channel.ErrLinkCodeExpired", err)
	}

	if got := countChannelLinks(ctx, t, pool, "tg-43"); got != 0 {
		t.Fatalf("channel_links(external_id=tg-43) = %d строк, ожидалось 0 (без привязки)", got)
	}

	stillUnused, err := q.GetChannelLinkCode(ctx, code)
	if err != nil {
		t.Fatalf("GetChannelLinkCode: %v", err)
	}
	if stillUnused.UsedAt.Valid {
		t.Fatal("used_at проставлен несмотря на отказ по истечению — код не должен сжигаться впустую")
	}
	t.Logf("OK: истёкший код отклонён (%v), привязка не создана, код остался неиспользованным", err)
}

// TestIntegration_Exchange_UsedCodeCannotBeReused — повторное использование
// уже использованного кода → ошибка (ErrLinkCodeUsed); вторая попытка не
// создаёт вторую строку channel_links (одноразовость, Gherkin §6).
func TestIntegration_Exchange_UsedCodeCannotBeReused(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupPool(ctx, t)
	defer done()
	q := db.New(pool)

	user := seedUser(ctx, t, q, "carol")
	code := seedLinkCode(ctx, t, q, user.ID, time.Hour)

	linker := channel.NewLinker(pool)
	if _, err := linker.Exchange(ctx, "telegram", code, "tg-44"); err != nil {
		t.Fatalf("первый Exchange: %v, ожидался успех", err)
	}

	// Повторный обмен ТЕМ ЖЕ кодом (напр. пользователь дважды нажал
	// /start <code>) — должен быть отклонён, а не создать вторую привязку.
	_, err := linker.Exchange(ctx, "telegram", code, "tg-44")
	if !errors.Is(err, channel.ErrLinkCodeUsed) {
		t.Fatalf("второй Exchange err = %v, ожидался channel.ErrLinkCodeUsed", err)
	}

	if got := countChannelLinks(ctx, t, pool, "tg-44"); got != 1 {
		t.Fatalf("channel_links(external_id=tg-44) = %d строк, ожидалась РОВНО 1 (без дубля)", got)
	}
	t.Logf("OK: повторное использование кода отклонено (%v), дубль привязки не создан", err)
}
