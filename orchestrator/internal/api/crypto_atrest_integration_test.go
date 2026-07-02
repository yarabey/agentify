//go:build integration

// Integration-тесты at-rest шифрования (тикет 11.1, FR I1) на РЕАЛЬНОМ
// Postgres (testcontainers): приёмочный тест «в БД после записи в *_enc
// колонках НЕТ плейнтекста». Проверяем оба зашифрованных поля тикета, которые
// пишет оркестратор в рабочем пути:
//   - tasks.text_enc — текст задачи (пишется POST /tasks, шифруется в
//     обработчике, tasks.go);
//   - task_events.payload_enc — содержимое события (пишется единственным
//     писателем task.Transitioner при смене статуса, transition.go).
//
// Для каждого поля: сырые байты в колонке (SELECT ... минуя расшифровку)
// НЕ содержат plaintext и НЕ равны ему, но расшифровываются тем же подключом
// обратно в исходное значение; а неверный ключ расшифровку проваливает
// (край «неверный ключ → ошибка» из приёмки; round-trip и «пустой/nil» на
// уровне примитива покрыты internal/crypto/crypto_test.go). uuid_enc покрыт
// отдельно integrations_integration_test.go (тикеты 2.2/2.3).
package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/yarabey/agentify/orchestrator/internal/api"
	"github.com/yarabey/agentify/orchestrator/internal/db"
	"github.com/yarabey/agentify/orchestrator/internal/task"
)

// wrongMasterKey32 — заведомо ДРУГОЙ мастер-ключ ровно 32 байта для проверки
// «неверный ключ → ошибка расшифровки» (отличается от testEncryptionKey32).
var wrongMasterKey32 = []byte("ffffffffffffffffffffffffffffffff")

// TestIntegration_AtRest_TaskTextEncryptedInDB — приёмка FR I1 (тикет 11.1)
// для tasks.text_enc: после POST /tasks сырой text_enc в БД не содержит
// открытого текста, но расшифровывается обратно; неверный ключ — ошибка.
func TestIntegration_AtRest_TaskTextEncryptedInDB(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, doneDB := setupDB(ctx, t)
	defer doneDB()
	q := db.New(pool)

	user, token := createTestUserWithToken(ctx, t, q, "alice-atrest-text")
	integration := createTestIntegration(ctx, t, q, user.ID, "alice-machine-atrest-text")

	server := api.NewServer(q, nil, []byte(testJWTSigningKey), []byte(testEncryptionKey32))
	server.SetTransitioner(task.NewTransitioner(pool, task.WithMasterKey([]byte(testEncryptionKey32))))
	server.SetCommandPublisher(noopCommandPublisher{})
	router := api.NewRouter(server)

	const plaintext = "совершенно секретный текст задачи 42"
	rec := doPostTasksRequest(t, router, token, "atrest-text-key-1", api.TaskCreate{
		IntegrationId: uuid.UUID(integration.ID.Bytes),
		Text:          plaintext,
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("POST /tasks: статус = %d (%s), ожидался 201", rec.Code, rec.Body.String())
	}
	var created api.Task
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("unmarshal POST /tasks: %v", err)
	}
	if created.Id == nil {
		t.Fatal("POST /tasks: Task.Id пуст")
	}
	// API отдаёт plaintext как раньше (внешнее поведение сохранено).
	if created.Text == nil || *created.Text != plaintext {
		t.Fatalf("POST /tasks: Task.Text = %v, ожидался %q (API отдаёт открытый текст)", created.Text, plaintext)
	}

	// Сырые байты колонки text_enc — минуя любую расшифровку.
	var raw []byte
	if err := pool.QueryRow(ctx, `SELECT text_enc FROM tasks WHERE id = $1`,
		pgtype.UUID{Bytes: *created.Id, Valid: true}).Scan(&raw); err != nil {
		t.Fatalf("SELECT text_enc: %v", err)
	}
	if len(raw) == 0 {
		t.Fatal("text_enc пуст — ожидался шифротекст")
	}
	if bytes.Contains(raw, []byte(plaintext)) {
		t.Fatalf("text_enc содержит открытый текст задачи — шифрования at-rest не произошло")
	}
	if bytes.Equal(raw, []byte(plaintext)) {
		t.Fatalf("text_enc равен открытому тексту — шифрования at-rest не произошло")
	}

	// Расшифровка тем же подключом возвращает исходный текст.
	got, err := api.DecryptTaskTextForTest([]byte(testEncryptionKey32), raw)
	if err != nil {
		t.Fatalf("DecryptTaskTextForTest: %v", err)
	}
	if string(got) != plaintext {
		t.Fatalf("расшифрованный text_enc = %q, ожидался %q", got, plaintext)
	}

	// Неверный ключ — ошибка (край приёмки).
	if _, err := api.DecryptTaskTextForTest(wrongMasterKey32, raw); err == nil {
		t.Fatal("расшифровка text_enc неверным ключом не вернула ошибку")
	}
}

// TestIntegration_AtRest_EventPayloadEncryptedInDB — приёмка FR I1 (тикет 11.1)
// для task_events.payload_enc: смена статуса задачи через POST /tasks пишет
// status_change, чей payload_enc в БД не содержит открытого JSON, но
// расшифровывается обратно; неверный ключ — ошибка.
func TestIntegration_AtRest_EventPayloadEncryptedInDB(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, doneDB := setupDB(ctx, t)
	defer doneDB()
	q := db.New(pool)

	user, token := createTestUserWithToken(ctx, t, q, "alice-atrest-payload")
	integration := createTestIntegration(ctx, t, q, user.ID, "alice-machine-atrest-payload")

	server := api.NewServer(q, nil, []byte(testJWTSigningKey), []byte(testEncryptionKey32))
	server.SetTransitioner(task.NewTransitioner(pool, task.WithMasterKey([]byte(testEncryptionKey32))))
	server.SetCommandPublisher(noopCommandPublisher{})
	router := api.NewRouter(server)

	rec := doPostTasksRequest(t, router, token, "atrest-payload-key-1", api.TaskCreate{
		IntegrationId: uuid.UUID(integration.ID.Bytes),
		Text:          "любой текст",
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("POST /tasks: статус = %d (%s), ожидался 201", rec.Code, rec.Body.String())
	}
	var created api.Task
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("unmarshal POST /tasks: %v", err)
	}
	if created.Id == nil {
		t.Fatal("POST /tasks: Task.Id пуст")
	}

	// POST /tasks перевёл задачу created→queued через Transitioner — есть хотя бы
	// одно событие status_change с зашифрованным payload_enc.
	var raw []byte
	if err := pool.QueryRow(ctx, `
		SELECT payload_enc FROM task_events
		WHERE task_id = $1 AND type = 'status_change'
		ORDER BY seq ASC LIMIT 1`,
		pgtype.UUID{Bytes: *created.Id, Valid: true}).Scan(&raw); err != nil {
		t.Fatalf("SELECT payload_enc: %v", err)
	}
	if len(raw) == 0 {
		t.Fatal("payload_enc пуст — ожидался шифротекст")
	}
	// Открытый JSON status_change содержал бы имена статусов/полей — их не должно
	// быть видно в сырых байтах.
	for _, marker := range [][]byte{[]byte("queued"), []byte("created"), []byte("trigger"), []byte("from")} {
		if bytes.Contains(raw, marker) {
			t.Fatalf("payload_enc содержит фрагмент открытого JSON %q — шифрования at-rest не произошло", marker)
		}
	}

	// Расшифровка тем же подключом даёт валидный JSON перехода.
	plaintext, err := api.DecryptEventPayloadForTest([]byte(testEncryptionKey32), raw)
	if err != nil {
		t.Fatalf("DecryptEventPayloadForTest: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(plaintext, &payload); err != nil {
		t.Fatalf("расшифрованный payload_enc не JSON: %v (%q)", err, plaintext)
	}
	if payload["to"] != string(task.StatusQueued) {
		t.Fatalf("payload.to = %v, ожидался %q", payload["to"], task.StatusQueued)
	}

	// Неверный ключ — ошибка (край приёмки).
	if _, err := api.DecryptEventPayloadForTest(wrongMasterKey32, raw); err == nil {
		t.Fatal("расшифровка payload_enc неверным ключом не вернула ошибку")
	}
}
