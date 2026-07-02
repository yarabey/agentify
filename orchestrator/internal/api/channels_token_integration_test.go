//go:build integration

// Integration-тесты POST /channels/telegram/token и полного цикла «действие
// из Telegram доходит до оркестратора» (тикет 10.3, FR D1, §4 «Постановка
// задачи из канала», Примеры: канал=telegram) на РЕАЛЬНОМ Postgres через
// testcontainers-go. setupDB/testJWTSigningKey/testEncryptionKey32
// (register_integration_test.go), createTestIntegration/doPostTasksRequest/
// noopCommandPublisher (tasks_integration_test.go) переиспользуются — тот же
// пакет api_test, все файлы компилируются вместе.
//
// Покрывает приёмку тикета 10.3 («Тесты: задача из Telegram доходит до
// машины, т.е. до оркестратора создаётся запись в tasks от правильного
// user_id»):
//   - happy path: telegram_user_id привязан (channel_links, тикет 10.2) →
//     POST /channels/telegram/token выдаёт acting-токен → POST /tasks с этим
//     токеном как Bearer (ОБЫЧНЫЙ путь единого API, БЕЗ какой-либо
//     Telegram-специфичной логики в tasks.go) создаёт РЕАЛЬНУЮ строку tasks с
//     user_id ИМЕННО владельца привязки;
//   - edge: telegram_user_id НЕ привязан ни к одному аккаунту →
//     POST /channels/telegram/token отвечает 404 not_linked, задача не
//     создаётся вовсе (в БД нет ни одной строки tasks) — бот в этом случае
//     отвечает пользователю подсказкой /start <code> (bot/start.go,
//     bot/internal/orchestrator.ErrNotLinked), сюда HTTP-запрос POST /tasks
//     даже не доходит.
package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/yarabey/agentify/orchestrator/internal/api"
	"github.com/yarabey/agentify/orchestrator/internal/db"
	"github.com/yarabey/agentify/orchestrator/internal/task"
)

// testBotServiceSecretIntegration — сервисный секрет бота для этого файла
// (тикет 10.3). Не секрет — используется только в тестовом процессе, как и
// testJWTSigningKey/testEncryptionKey32 (register_integration_test.go).
const testBotServiceSecretIntegration = "integration-test-bot-service-secret"

// newTelegramActionsTestRouter собирает роутер тем же способом, что
// orchestrator/main.go для действий из Telegram (тикет 10.3): реальный
// Transitioner + noop-паблишер (доставка в Redpanda уже отдельно покрыта
// tasks_integration_test.go — предмет проверки здесь именно путь
// telegram_user_id → user_id → строка tasks, не сама публикация конверта) +
// сервисный секрет бота (SetBotServiceSecret).
func newTelegramActionsTestRouter(pool *pgxpool.Pool) http.Handler {
	server := api.NewServer(db.New(pool), nil, []byte(testJWTSigningKey), []byte(testEncryptionKey32))
	server.SetTransitioner(task.NewTransitioner(pool, task.WithMasterKey([]byte(testEncryptionKey32))))
	server.SetCommandPublisher(noopCommandPublisher{})
	server.SetBotServiceSecret([]byte(testBotServiceSecretIntegration))
	return api.NewRouter(server)
}

// postChannelsTelegramToken шлёт POST /channels/telegram/token через httptest
// поверх router с валидным X-Bot-Service-Secret.
func postChannelsTelegramToken(t *testing.T, router http.Handler, telegramUserID string) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(api.TelegramActingTokenRequest{TelegramUserId: telegramUserID})
	if err != nil {
		t.Fatalf("marshal тела: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/channels/telegram/token", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Bot-Service-Secret", testBotServiceSecretIntegration)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

// TestIntegration_TelegramActions_LinkedUserCanCreateTask — happy path
// приёмки тикета 10.3: telegram_user_id привязан к аккаунту → бот получает
// acting-токен → POST /tasks с этим токеном (как ОБЫЧНЫЙ клиент единого API,
// принцип «единый API») создаёт РЕАЛЬНУЮ строку tasks с user_id владельца
// привязки — «задача из Telegram доходит до машины» в терминах приёмки.
func TestIntegration_TelegramActions_LinkedUserCanCreateTask(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, doneDB := setupDB(ctx, t)
	defer doneDB()
	q := db.New(pool)

	user, err := q.CreateUser(ctx, db.CreateUserParams{Username: "telegram-grace", PasswordHash: "argon2id$stub"})
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	// Предусловие: пользователь уже выполнил /start <code> (тикет 10.2) —
	// channel_links связывает telegram_user_id="tg-500" с этим аккаунтом.
	if _, err := q.CreateChannelLink(ctx, db.CreateChannelLinkParams{
		UserID:     user.ID,
		Channel:    "telegram",
		ExternalID: "tg-500",
	}); err != nil {
		t.Fatalf("CreateChannelLink: %v", err)
	}
	integration := createTestIntegration(ctx, t, q, user.ID, "grace-machine")

	router := newTelegramActionsTestRouter(pool)

	tokenRec := postChannelsTelegramToken(t, router, "tg-500")
	if tokenRec.Code != http.StatusOK {
		t.Fatalf("POST /channels/telegram/token: статус = %d (%s), ожидался 200", tokenRec.Code, tokenRec.Body.String())
	}
	var tokenResp api.TelegramActingToken
	if uerr := json.Unmarshal(tokenRec.Body.Bytes(), &tokenResp); uerr != nil {
		t.Fatalf("тело ответа не JSON TelegramActingToken: %v", uerr)
	}
	if tokenResp.AccessToken == nil || *tokenResp.AccessToken == "" {
		t.Fatal("access_token пуст")
	}

	// Бот дальше действует как ОБЫЧНЫЙ клиент контракта — POST /tasks с
	// полученным acting-токеном как Bearer, тем же путём (doPostTasksRequest,
	// tasks_integration_test.go), что и web; tasks.go этим тикетом НЕ
	// изменяется вообще.
	taskRec := doPostTasksRequest(t, router, *tokenResp.AccessToken, "tg-idem-500", api.TaskCreate{
		IntegrationId: uuid.UUID(integration.ID.Bytes),
		Text:          "Собери проект и прогони тесты",
	})
	if taskRec.Code != http.StatusCreated {
		t.Fatalf("POST /tasks: статус = %d (%s), ожидался 201", taskRec.Code, taskRec.Body.String())
	}

	var gotTask api.Task
	if uerr := json.Unmarshal(taskRec.Body.Bytes(), &gotTask); uerr != nil {
		t.Fatalf("тело ответа не JSON Task: %v", uerr)
	}
	if gotTask.Id == nil {
		t.Fatal("id задачи пуст")
	}

	var dbUserID pgtype.UUID
	if qerr := pool.QueryRow(ctx, `SELECT user_id FROM tasks WHERE id = $1`, *gotTask.Id).Scan(&dbUserID); qerr != nil {
		t.Fatalf("SELECT user_id FROM tasks: %v", qerr)
	}
	if dbUserID.Bytes != user.ID.Bytes {
		t.Fatalf("tasks.user_id = %x, ожидался %x (владелец telegram-привязки, а НЕ произвольный/нулевой)", dbUserID.Bytes, user.ID.Bytes)
	}
	t.Logf("OK: задача из Telegram создана от правильного user_id (%s)", user.ID)
}

// TestIntegration_TelegramActions_UnlinkedUserCannotCreateTask — edge из
// брифа тикета 10.3: telegram_user_id, который НИКОГДА не выполнял
// /start <code> (нет строки channel_links), → POST /channels/telegram/token
// отвечает 404 not_linked, и ни одной строки tasks не появляется — действие
// не доходит даже до POST /tasks (бот не получает acting-токен, которым мог
// бы его вызвать).
func TestIntegration_TelegramActions_UnlinkedUserCannotCreateTask(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, doneDB := setupDB(ctx, t)
	defer doneDB()

	router := newTelegramActionsTestRouter(pool)

	rec := postChannelsTelegramToken(t, router, "tg-unlinked-999")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("статус = %d (%s), ожидался 404", rec.Code, rec.Body.String())
	}
	var got api.Error
	if uerr := json.Unmarshal(rec.Body.Bytes(), &got); uerr != nil {
		t.Fatalf("тело ответа не JSON Error: %v", uerr)
	}
	if got.Code != "not_linked" {
		t.Fatalf("code = %q, ожидался not_linked", got.Code)
	}

	var n int
	if qerr := pool.QueryRow(ctx, `SELECT count(*) FROM tasks`).Scan(&n); qerr != nil {
		t.Fatalf("count tasks: %v", qerr)
	}
	if n != 0 {
		t.Fatalf("tasks = %d строк, ожидалось 0 (непривязанный пользователь не может поставить задачу)", n)
	}
	t.Logf("OK: непривязанный telegram_user_id → 404 not_linked, задача не создана")
}
