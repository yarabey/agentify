//go:build integration

// Integration-тесты Notifier.Notify на РЕАЛЬНОМ Postgres через
// testcontainers-go (тикет 7.3, FR G1) — тот же паттерн, что и
// orchestrator/internal/channel/link_integration_test.go /
// orchestrator/internal/task/transition_integration_test.go: собственный
// startPostgres/setupPool-хелпер, тег integration.
//
// Redpanda-часть подменена fakeProducer (см. notifier_test.go), НЕ реальным
// брокером: в песочнице этого прогона образ redpandadata/redpanda:v24.2.7
// недоступен по сети (см. docker pull … → 403 Forbidden от
// production.cloudfront.docker.com — прокси-ограничение окружения, а не
// проблема кода, см. также internal/bus/integration_test.go, который
// действительно поднимает Redpanda и по той же причине не может быть
// прогнан здесь). Поэтому здесь проверяется именно то, что реально требует
// БД — резолв channel_links/tasks (владелец тикета 7.3, "простой путь"
// резолва привязки на стороне оркестратора, см. годок пакета), — а не
// Kafka-транспорт (его протокольные гарантии уже покрыты
// internal/bus/integration_test.go).
package telegram

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"crypto/rand"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/yarabey/agentify/internal/bus"
	"github.com/yarabey/agentify/orchestrator/internal/db"
	"github.com/yarabey/agentify/orchestrator/internal/notify"
	"github.com/yarabey/agentify/orchestrator/migrations"
)

const (
	// pgImage тянется через настроенный daemon registry-mirror (тикет 0.3,
	// mirror.gcr.io) — прокси не обходим (тот же образ, что и у остальных
	// Postgres-integration-тестов оркестратора).
	pgImage = "postgres:16-alpine"
	pgUser  = "telegram_notify_test"
	pgPass  = "telegram_notify_test"
	pgDB    = "telegram_notify_test"
)

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

// integrationIDString конвертирует pgtype.UUID в каноническую строку — та же
// форма, что кладёт Notifier.Notify в env.IntegrationID (см. notifier.go).
func integrationIDString(t *testing.T, id pgtype.UUID) string {
	t.Helper()
	return uuid.UUID(id.Bytes).String()
}

func randomCode(t *testing.T) string {
	t.Helper()
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("crypto/rand.Read: %v", err)
	}
	return hex.EncodeToString(b)
}

// seedUserIntegrationTask создаёт владельца, интеграцию и задачу (стартовый
// статус 'created') — тот же приём, что и seedTask в
// orchestrator/internal/task/transition_integration_test.go, но возвращает
// ещё и userID/integrationID отдельно (нужны тесту для сборки
// notify.Notification и проверки env.IntegrationID).
func seedUserIntegrationTask(ctx context.Context, t *testing.T, pool *pgxpool.Pool, q *db.Queries, username string) (userID, integrationID, taskID pgtype.UUID) {
	t.Helper()
	owner, err := q.CreateUser(ctx, db.CreateUserParams{
		Username:     username,
		PasswordHash: "argon2id$stub",
		IsAdmin:      false,
	})
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	err = pool.QueryRow(ctx, `
		INSERT INTO integrations (user_id, name, uuid_hmac, uuid_enc)
		VALUES ($1, $2, $3, $4)
		RETURNING id`,
		owner.ID, username+"-machine", username+"-hmac", []byte("ciphertext-stub")).Scan(&integrationID)
	if err != nil {
		t.Fatalf("вставка integrations: %v", err)
	}

	err = pool.QueryRow(ctx, `
		INSERT INTO tasks (user_id, integration_id, text_enc)
		VALUES ($1, $2, $3)
		RETURNING id`,
		owner.ID, integrationID, []byte("text-ciphertext-stub")).Scan(&taskID)
	if err != nil {
		t.Fatalf("вставка tasks: %v", err)
	}
	return owner.ID, integrationID, taskID
}

// TestIntegration_Notify_WithTelegramLink_Publishes — на реальном Postgres:
// пользователь с активной привязкой Telegram (channel_links) получает
// уведомление, опубликованное с telegram_chat_id из привязки и
// integration_id из его задачи (приёмка тикета 7.3: "вопрос → сообщение в
// чат", здесь — до границы Kafka, см. годок файла).
func TestIntegration_Notify_WithTelegramLink_Publishes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	pool, cleanup := setupPool(ctx, t)
	defer cleanup()
	q := db.New(pool)

	userID, integrationID, taskID := seedUserIntegrationTask(ctx, t, pool, q, "tguser-linked")

	const telegramUserID = "987654321"
	if _, err := q.CreateChannelLink(ctx, db.CreateChannelLinkParams{
		UserID:     userID,
		Channel:    telegramChannel,
		ExternalID: telegramUserID,
	}); err != nil {
		t.Fatalf("CreateChannelLink: %v", err)
	}

	fp := &fakeProducer{}
	n := &Notifier{producer: fp, channels: q, tasks: q}

	questionPayload, err := json.Marshal(bus.AgentQuestionPayload{QuestionID: "q-int-1", Text: "Продолжить установку зависимостей?"})
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	notification := notify.Notification{
		TaskID:    taskID,
		UserID:    userID,
		Kind:      notify.KindAgentQuestion,
		Payload:   questionPayload,
		CreatedAt: time.Now().UTC(),
	}
	if err := n.Notify(ctx, notification); err != nil {
		t.Fatalf("Notify: неожиданная ошибка: %v", err)
	}

	if len(fp.calls) != 1 {
		t.Fatalf("Publish вызван %d раз(а), ожидался 1", len(fp.calls))
	}
	call := fp.calls[0]
	if call.topic != bus.TopicNotificationsTelegram {
		t.Fatalf("topic = %q, ожидался %q", call.topic, bus.TopicNotificationsTelegram)
	}
	wantIntegrationID := integrationIDString(t, integrationID)
	if call.env.IntegrationID != wantIntegrationID {
		t.Fatalf("env.IntegrationID = %q, ожидался %q (integration_id задачи)", call.env.IntegrationID, wantIntegrationID)
	}

	var payload bus.TelegramNotificationPayload
	if err := json.Unmarshal(call.env.Payload, &payload); err != nil {
		t.Fatalf("демаршалинг payload: %v", err)
	}
	if payload.TelegramChatID != 987654321 {
		t.Fatalf("TelegramChatID = %d, ожидался 987654321", payload.TelegramChatID)
	}
	if payload.Text == "" {
		t.Fatal("Text пуст")
	}
	t.Logf("OK: уведомление опубликовано для telegram_chat_id=%d, text=%q", payload.TelegramChatID, payload.Text)
}

// TestIntegration_Notify_WithoutTelegramLink_NoPublish — на реальном
// Postgres: пользователь БЕЗ привязки Telegram — Notify не публикует ничего
// и не возвращает ошибку (best-effort канал).
func TestIntegration_Notify_WithoutTelegramLink_NoPublish(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	pool, cleanup := setupPool(ctx, t)
	defer cleanup()
	q := db.New(pool)

	userID, _, taskID := seedUserIntegrationTask(ctx, t, pool, q, "tguser-unlinked")

	fp := &fakeProducer{}
	n := &Notifier{producer: fp, channels: q, tasks: q}

	notification := notify.Notification{
		TaskID:    taskID,
		UserID:    userID,
		Kind:      notify.KindAgentQuestion,
		Payload:   []byte(`{"question_id":"q-1","text":"?"}`),
		CreatedAt: time.Now().UTC(),
	}
	if err := n.Notify(ctx, notification); err != nil {
		t.Fatalf("Notify: неожиданная ошибка: %v", err)
	}
	if len(fp.calls) != 0 {
		t.Fatalf("Publish вызван %d раз(а) без привязки Telegram, ожидалось 0", len(fp.calls))
	}
}

// TestIntegration_Notify_MultipleLinks_UsesMostRecent — если у пользователя
// (в теории — нет UNIQUE(user_id, channel), см. миграцию 00004) оказалось
// НЕСКОЛЬКО привязок Telegram, GetChannelLinkByUserAndChannel (и, тем самым,
// Notify) берёт САМУЮ СВЕЖУЮ (см. годок SQL-запроса).
func TestIntegration_Notify_MultipleLinks_UsesMostRecent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	pool, cleanup := setupPool(ctx, t)
	defer cleanup()
	q := db.New(pool)

	userID, _, taskID := seedUserIntegrationTask(ctx, t, pool, q, "tguser-multi")

	if _, err := q.CreateChannelLink(ctx, db.CreateChannelLinkParams{UserID: userID, Channel: telegramChannel, ExternalID: "111111111"}); err != nil {
		t.Fatalf("CreateChannelLink (первая): %v", err)
	}
	// created_at — DEFAULT now(); гарантируем различимый порядок между
	// вставками без флаки-теста на точности часов.
	time.Sleep(10 * time.Millisecond)
	if _, err := q.CreateChannelLink(ctx, db.CreateChannelLinkParams{UserID: userID, Channel: telegramChannel, ExternalID: "222222222"}); err != nil {
		t.Fatalf("CreateChannelLink (вторая): %v", err)
	}

	fp := &fakeProducer{}
	n := &Notifier{producer: fp, channels: q, tasks: q}

	notification := notify.Notification{
		TaskID:    taskID,
		UserID:    userID,
		Kind:      notify.KindAnswerReminder,
		CreatedAt: time.Now().UTC(),
	}
	if err := n.Notify(ctx, notification); err != nil {
		t.Fatalf("Notify: неожиданная ошибка: %v", err)
	}
	if len(fp.calls) != 1 {
		t.Fatalf("Publish вызван %d раз(а), ожидался 1", len(fp.calls))
	}
	var payload bus.TelegramNotificationPayload
	if err := json.Unmarshal(fp.calls[0].env.Payload, &payload); err != nil {
		t.Fatalf("демаршалинг payload: %v", err)
	}
	if payload.TelegramChatID != 222222222 {
		t.Fatalf("TelegramChatID = %d, ожидался 222222222 (самая свежая привязка)", payload.TelegramChatID)
	}
}
