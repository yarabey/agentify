//go:build integration

// Integration-тесты CRUD интеграций + выдачи UUID на РЕАЛЬНОМ Postgres через
// testcontainers-go (тикет 2.2, FR B1-B6, Gherkin §2 «Управление
// интеграциями»). Тег integration, setupDB/testJWTSigningKey/
// testEncryptionKey32 переиспользуются из register_integration_test.go (тот
// же пакет api_test).
//
// Покрываем приёмочные сценарии тикета 2.2:
//   - POST /integrations с валидным телом → 201, выдан непустой UUID,
//     name совпадает, status == "offline" (FR B1, B2, B4);
//   - список GET /integrations показывает только свои интеграции — owner
//     isolation между двумя разными пользователями (FR A4, I3);
//   - GET /integrations/{id} чужой интеграции → 404 (не 403, единый ответ);
//   - GET /integrations/{id} своей интеграции → 200, uuid совпадает с тем,
//     что вернул POST (расшифровка через реальный Postgres BYTEA проходит
//     по кругу);
//   - PATCH /integrations/{id} меняет name → 200, последующий GET видит
//     новое имя, uuid (и, соответственно, uuid_hmac) не меняются;
//   - POST /integrations с пустым name → 400.
//
// Access-токены выпускаются напрямую через auth.IssueAccessToken (как в
// admin_integration_test.go) — без прохождения через /auth/login.
//
// Отдельно (TestIntegration_Integrations_PatchDuringRunningTaskDoesNotBreakTask,
// тикет 2.5, FR B5, Gherkin §2 «Редактирование не рвёт активные задачи»)
// покрываем пересечение PATCH с активной задачей: createTestIntegration/
// getTaskRowStatus переиспользованы из tasks_integration_test.go (тот же
// пакет api_test, оба файла компилируются вместе), доводим задачу до
// running через task.Transitioner — тем же приёмом, что и
// TestIntegration_PostTasksIdAnswer_TwoQuestionsCorrectBinding.
package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/yarabey/agentify/internal/auth"
	"github.com/yarabey/agentify/orchestrator/internal/api"
	"github.com/yarabey/agentify/orchestrator/internal/db"
	"github.com/yarabey/agentify/orchestrator/internal/task"
)

// createTestUserWithToken создаёт пользователя и выпускает ему access-токен —
// общий шаг предусловия для тестов интеграций.
func createTestUserWithToken(ctx context.Context, t *testing.T, q *db.Queries, username string) (db.User, string) {
	t.Helper()
	user, err := q.CreateUser(ctx, db.CreateUserParams{
		Username:     username,
		PasswordHash: "$argon2id$v=19$m=1,t=1,p=1$c2FsdA$aGFzaA",
		IsAdmin:      false,
	})
	if err != nil {
		t.Fatalf("CreateUser (%s): %v", username, err)
	}
	accessToken, err := auth.IssueAccessToken(user.ID.String(), []byte(testJWTSigningKey), time.Now())
	if err != nil {
		t.Fatalf("IssueAccessToken (%s): %v", username, err)
	}
	return user, accessToken
}

// doIntegrationsRequest шлёт HTTP-запрос через httptest поверх router с
// Bearer-токеном и (опционально) JSON-телом.
func doIntegrationsRequest(t *testing.T, router http.Handler, method, path, bearerToken string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var reqBody *bytes.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal тела: %v", err)
		}
		reqBody = bytes.NewReader(raw)
	} else {
		reqBody = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, reqBody)
	req.Header.Set("Content-Type", "application/json")
	if bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+bearerToken)
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

// TestIntegration_Integrations_CreateReturnsUUIDAndOfflineStatus —
// POST /integrations с валидным телом → 201, непустой uuid, имя совпадает,
// status == "offline" (FR B1, B2, B4, Gherkin §2 «Создание интеграции выдаёт
// UUID»).
func TestIntegration_Integrations_CreateReturnsUUIDAndOfflineStatus(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupDB(ctx, t)
	defer done()
	q := db.New(pool)

	_, token := createTestUserWithToken(ctx, t, q, "alice-create")

	router := api.NewRouter(api.NewServer(q, nil, []byte(testJWTSigningKey), []byte(testEncryptionKey32)))

	ipHint := "192.0.2.10"
	rec := doIntegrationsRequest(t, router, http.MethodPost, "/integrations", token, api.IntegrationCreate{
		Name:   "my-laptop",
		IpHint: &ipHint,
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("статус = %d (%s), ожидался 201", rec.Code, rec.Body.String())
	}

	var created api.IntegrationWithSecret
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("unmarshal тела: %v", err)
	}
	if created.Uuid == nil || created.Uuid.String() == "00000000-0000-0000-0000-000000000000" {
		t.Fatalf("uuid пуст/нулевой: %+v", created.Uuid)
	}
	if created.Name == nil || *created.Name != "my-laptop" {
		t.Fatalf("name = %v, ожидалось my-laptop", created.Name)
	}
	if created.Status == nil || *created.Status != api.IntegrationWithSecretStatus("offline") {
		t.Fatalf("status = %v, ожидался offline", created.Status)
	}
	t.Logf("OK: POST /integrations → 201, uuid=%s, status=offline", created.Uuid)
}

// TestIntegration_Integrations_CreateRejectsEmptyName — POST /integrations с
// пустым name → 400 (FR B1).
func TestIntegration_Integrations_CreateRejectsEmptyName(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupDB(ctx, t)
	defer done()
	q := db.New(pool)

	_, token := createTestUserWithToken(ctx, t, q, "alice-empty-name")

	router := api.NewRouter(api.NewServer(q, nil, []byte(testJWTSigningKey), []byte(testEncryptionKey32)))

	rec := doIntegrationsRequest(t, router, http.MethodPost, "/integrations", token, api.IntegrationCreate{Name: "   "})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("статус = %d (%s), ожидался 400", rec.Code, rec.Body.String())
	}
	t.Logf("OK: пустой name → 400")
}

// TestIntegration_Integrations_ListIsOwnerScoped — список GET /integrations
// показывает только свои интеграции (FR A4, I3, owner isolation между двумя
// разными пользователями).
func TestIntegration_Integrations_ListIsOwnerScoped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupDB(ctx, t)
	defer done()
	q := db.New(pool)

	_, aliceToken := createTestUserWithToken(ctx, t, q, "alice-list")
	_, bobToken := createTestUserWithToken(ctx, t, q, "bob-list")

	router := api.NewRouter(api.NewServer(q, nil, []byte(testJWTSigningKey), []byte(testEncryptionKey32)))

	// Алиса создаёт две интеграции, Боб — одну.
	for _, name := range []string{"alice-1", "alice-2"} {
		rec := doIntegrationsRequest(t, router, http.MethodPost, "/integrations", aliceToken, api.IntegrationCreate{Name: name})
		if rec.Code != http.StatusCreated {
			t.Fatalf("создание %s: статус = %d (%s)", name, rec.Code, rec.Body.String())
		}
	}
	rec := doIntegrationsRequest(t, router, http.MethodPost, "/integrations", bobToken, api.IntegrationCreate{Name: "bob-1"})
	if rec.Code != http.StatusCreated {
		t.Fatalf("создание bob-1: статус = %d (%s)", rec.Code, rec.Body.String())
	}

	// Алиса видит только свои две.
	aliceList := doIntegrationsRequest(t, router, http.MethodGet, "/integrations", aliceToken, nil)
	if aliceList.Code != http.StatusOK {
		t.Fatalf("GET /integrations (alice): статус = %d (%s)", aliceList.Code, aliceList.Body.String())
	}
	var aliceIntegrations []api.Integration
	if err := json.Unmarshal(aliceList.Body.Bytes(), &aliceIntegrations); err != nil {
		t.Fatalf("unmarshal (alice): %v", err)
	}
	if len(aliceIntegrations) != 2 {
		t.Fatalf("alice видит %d интеграций, ожидалось 2: %+v", len(aliceIntegrations), aliceIntegrations)
	}
	for _, it := range aliceIntegrations {
		if it.Name == nil || (*it.Name != "alice-1" && *it.Name != "alice-2") {
			t.Fatalf("alice видит чужую интеграцию: %+v", it)
		}
	}

	// Боб видит только свою одну.
	bobList := doIntegrationsRequest(t, router, http.MethodGet, "/integrations", bobToken, nil)
	if bobList.Code != http.StatusOK {
		t.Fatalf("GET /integrations (bob): статус = %d (%s)", bobList.Code, bobList.Body.String())
	}
	var bobIntegrations []api.Integration
	if err := json.Unmarshal(bobList.Body.Bytes(), &bobIntegrations); err != nil {
		t.Fatalf("unmarshal (bob): %v", err)
	}
	if len(bobIntegrations) != 1 || bobIntegrations[0].Name == nil || *bobIntegrations[0].Name != "bob-1" {
		t.Fatalf("bob видит %+v, ожидалась только bob-1", bobIntegrations)
	}
	t.Logf("OK: список интеграций owner-scoped — alice 2, bob 1, без пересечения")
}

// TestIntegration_Integrations_GetByIdNotOwnerReturns404 — GET
// /integrations/{id} чужой интеграции → 404, не 403 (единый ответ, owner
// isolation).
func TestIntegration_Integrations_GetByIdNotOwnerReturns404(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupDB(ctx, t)
	defer done()
	q := db.New(pool)

	_, aliceToken := createTestUserWithToken(ctx, t, q, "alice-getid-owner")
	_, bobToken := createTestUserWithToken(ctx, t, q, "bob-getid-other")

	router := api.NewRouter(api.NewServer(q, nil, []byte(testJWTSigningKey), []byte(testEncryptionKey32)))

	createRec := doIntegrationsRequest(t, router, http.MethodPost, "/integrations", aliceToken, api.IntegrationCreate{Name: "alice-secret-machine"})
	if createRec.Code != http.StatusCreated {
		t.Fatalf("создание: статус = %d (%s)", createRec.Code, createRec.Body.String())
	}
	var created api.IntegrationWithSecret
	if err := json.Unmarshal(createRec.Body.Bytes(), &created); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Боб пытается прочитать интеграцию Алисы по её id.
	bobGet := doIntegrationsRequest(t, router, http.MethodGet, "/integrations/"+created.Id.String(), bobToken, nil)
	if bobGet.Code != http.StatusNotFound {
		t.Fatalf("GET чужой интеграции: статус = %d (%s), ожидался 404", bobGet.Code, bobGet.Body.String())
	}
	t.Logf("OK: GET чужой интеграции → 404 (не 403)")
}

// TestIntegration_Integrations_GetByIdOwnReturnsMatchingUUID — GET
// /integrations/{id} своей интеграции → 200, uuid совпадает с тем, что вернул
// POST (расшифровка работает через реальный Postgres BYTEA, FR B2).
func TestIntegration_Integrations_GetByIdOwnReturnsMatchingUUID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupDB(ctx, t)
	defer done()
	q := db.New(pool)

	_, token := createTestUserWithToken(ctx, t, q, "alice-getid-own")

	router := api.NewRouter(api.NewServer(q, nil, []byte(testJWTSigningKey), []byte(testEncryptionKey32)))

	createRec := doIntegrationsRequest(t, router, http.MethodPost, "/integrations", token, api.IntegrationCreate{Name: "alice-own-machine"})
	if createRec.Code != http.StatusCreated {
		t.Fatalf("создание: статус = %d (%s)", createRec.Code, createRec.Body.String())
	}
	var created api.IntegrationWithSecret
	if err := json.Unmarshal(createRec.Body.Bytes(), &created); err != nil {
		t.Fatalf("unmarshal create: %v", err)
	}

	getRec := doIntegrationsRequest(t, router, http.MethodGet, "/integrations/"+created.Id.String(), token, nil)
	if getRec.Code != http.StatusOK {
		t.Fatalf("GET своей интеграции: статус = %d (%s), ожидался 200", getRec.Code, getRec.Body.String())
	}
	var fetched api.IntegrationWithSecret
	if err := json.Unmarshal(getRec.Body.Bytes(), &fetched); err != nil {
		t.Fatalf("unmarshal get: %v", err)
	}
	if fetched.Uuid == nil || created.Uuid == nil || *fetched.Uuid != *created.Uuid {
		t.Fatalf("uuid при GET (%v) не совпадает с uuid из POST (%v)", fetched.Uuid, created.Uuid)
	}
	t.Logf("OK: GET своей интеграции → 200, uuid совпадает с выданным при создании")
}

// TestIntegration_Integrations_PatchUpdatesNameKeepsUUID — PATCH
// /integrations/{id} меняет name → 200, последующий GET видит новое имя,
// uuid (а значит и uuid_hmac, raw-секрет не пересоздаётся) не меняется
// (FR B5).
func TestIntegration_Integrations_PatchUpdatesNameKeepsUUID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupDB(ctx, t)
	defer done()
	q := db.New(pool)

	_, token := createTestUserWithToken(ctx, t, q, "alice-patch")

	router := api.NewRouter(api.NewServer(q, nil, []byte(testJWTSigningKey), []byte(testEncryptionKey32)))

	createRec := doIntegrationsRequest(t, router, http.MethodPost, "/integrations", token, api.IntegrationCreate{Name: "old-name"})
	if createRec.Code != http.StatusCreated {
		t.Fatalf("создание: статус = %d (%s)", createRec.Code, createRec.Body.String())
	}
	var created api.IntegrationWithSecret
	if err := json.Unmarshal(createRec.Body.Bytes(), &created); err != nil {
		t.Fatalf("unmarshal create: %v", err)
	}

	newName := "new-name"
	patchRec := doIntegrationsRequest(t, router, http.MethodPatch, "/integrations/"+created.Id.String(), token, api.IntegrationUpdate{Name: &newName})
	if patchRec.Code != http.StatusOK {
		t.Fatalf("PATCH: статус = %d (%s), ожидался 200", patchRec.Code, patchRec.Body.String())
	}
	var patched api.Integration
	if err := json.Unmarshal(patchRec.Body.Bytes(), &patched); err != nil {
		t.Fatalf("unmarshal patch: %v", err)
	}
	if patched.Name == nil || *patched.Name != newName {
		t.Fatalf("PATCH ответ name = %v, ожидалось %q", patched.Name, newName)
	}

	getRec := doIntegrationsRequest(t, router, http.MethodGet, "/integrations/"+created.Id.String(), token, nil)
	if getRec.Code != http.StatusOK {
		t.Fatalf("GET после PATCH: статус = %d (%s), ожидался 200", getRec.Code, getRec.Body.String())
	}
	var fetched api.IntegrationWithSecret
	if err := json.Unmarshal(getRec.Body.Bytes(), &fetched); err != nil {
		t.Fatalf("unmarshal get после patch: %v", err)
	}
	if fetched.Name == nil || *fetched.Name != newName {
		t.Fatalf("GET после PATCH видит name = %v, ожидалось %q", fetched.Name, newName)
	}
	if fetched.Uuid == nil || created.Uuid == nil || *fetched.Uuid != *created.Uuid {
		t.Fatalf("uuid после PATCH (%v) изменился относительно исходного (%v)", fetched.Uuid, created.Uuid)
	}
	t.Logf("OK: PATCH меняет name (видно при последующем GET), uuid не меняется")
}

// TestIntegration_Integrations_PatchDuringRunningTaskDoesNotBreakTask —
// приёмка тикета 2.5 (FR B5, Gherkin §2 «Редактирование не рвёт активные
// задачи»): реальный HTTP PATCH /integrations/{id} во время running-задачи
// этой интеграции не меняет tasks.status, не добавляет лишних task_events, и
// FSM задачи по-прежнему способна штатно перейти дальше (running→
// waiting_user) ПОСЛЕ patch'а — это и есть эмпирическое доказательство «не
// рвёт», а не просто «поле не тронуто». Дополняет
// TestIntegration_Integrations_PatchUpdatesNameKeepsUUID (тот проверяет
// name/uuid без участия задач вообще).
func TestIntegration_Integrations_PatchDuringRunningTaskDoesNotBreakTask(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupDB(ctx, t)
	defer done()
	q := db.New(pool)

	user, token := createTestUserWithToken(ctx, t, q, "alice-patch-running-task")
	integration := createTestIntegration(ctx, t, q, user.ID, "alice-machine-running")

	// Предусловие: задача доведена до running (created→queued→running), тем
	// же приёмом, что и TestIntegration_PostTasksIdAnswer_TwoQuestionsCorrectBinding
	// (tasks_integration_test.go).
	idempotencyKey := "patch-during-running-key-1"
	taskRow, err := q.CreateTask(ctx, db.CreateTaskParams{
		UserID:         user.ID,
		IntegrationID:  integration.ID,
		TextEnc:        []byte("длинная задача, идущая во время редактирования интеграции"),
		IdempotencyKey: &idempotencyKey,
	})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	taskID := taskRow.ID

	tr := task.NewTransitioner(pool)
	for _, trigger := range []task.Trigger{task.TriggerEnqueued, task.TriggerTaskAccepted} {
		if _, _, terr := tr.Transition(ctx, taskID, trigger); terr != nil {
			t.Fatalf("подготовка (%s): %v", trigger, terr)
		}
	}
	if status := getTaskRowStatus(ctx, t, pool, taskID); status != string(task.StatusRunning) {
		t.Fatalf("подготовка: tasks.status = %s, ожидался running", status)
	}

	var eventsBefore int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM task_events WHERE task_id = $1`, taskID).Scan(&eventsBefore); err != nil {
		t.Fatalf("SELECT count(task_events) до PATCH: %v", err)
	}

	router := api.NewRouter(api.NewServer(q, nil, []byte(testJWTSigningKey), []byte(testEncryptionKey32)))

	// Реальный HTTP PATCH меняет ОБА поля (name И ip_hint) одновременно —
	// тикет прямо перечисляет оба поля.
	newName := "renamed-during-running"
	newIPHint := "203.0.113.7"
	integrationID := uuid.UUID(integration.ID.Bytes)
	patchRec := doIntegrationsRequest(t, router, http.MethodPatch, "/integrations/"+integrationID.String(), token, api.IntegrationUpdate{
		Name:   &newName,
		IpHint: &newIPHint,
	})
	if patchRec.Code != http.StatusOK {
		t.Fatalf("PATCH: статус = %d (%s), ожидался 200", patchRec.Code, patchRec.Body.String())
	}
	var patched api.Integration
	if err := json.Unmarshal(patchRec.Body.Bytes(), &patched); err != nil {
		t.Fatalf("unmarshal patch: %v", err)
	}
	if patched.Name == nil || *patched.Name != newName {
		t.Fatalf("PATCH ответ name = %v, ожидалось %q", patched.Name, newName)
	}
	if patched.IpHint == nil || *patched.IpHint != newIPHint {
		t.Fatalf("PATCH ответ ip_hint = %v, ожидалось %q", patched.IpHint, newIPHint)
	}

	// tasks.status всё ещё running — PATCH интеграции не тронул задачу.
	if status := getTaskRowStatus(ctx, t, pool, taskID); status != string(task.StatusRunning) {
		t.Fatalf("tasks.status после PATCH = %s, ожидался running (PATCH не должен трогать задачи)", status)
	}

	// task_events не выросли — PATCH не породил никаких лишних записей.
	var eventsAfter int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM task_events WHERE task_id = $1`, taskID).Scan(&eventsAfter); err != nil {
		t.Fatalf("SELECT count(task_events) после PATCH: %v", err)
	}
	if eventsAfter != eventsBefore {
		t.Fatalf("task_events выросли с %d до %d после PATCH интеграции — PATCH не должен писать события задачи", eventsBefore, eventsAfter)
	}

	// Эмпирическое доказательство «не рвёт»: FSM задачи по-прежнему штатно
	// продолжается ПОСЛЕ PATCH (running→waiting_user через тот же реальный
	// Transitioner, что использует продакшн-код).
	if _, _, terr := tr.Transition(ctx, taskID, task.TriggerAgentQuestion); terr != nil {
		t.Fatalf("Transition(agent_question) после PATCH интеграции: %v (задача не должна быть сломана правкой интеграции)", terr)
	}
	if status := getTaskRowStatus(ctx, t, pool, taskID); status != string(task.StatusWaitingUser) {
		t.Fatalf("tasks.status после Transition = %s, ожидался waiting_user", status)
	}

	t.Logf("OK: PATCH интеграции во время running-задачи меняет name+ip_hint (200), не трогает tasks.status/task_events, задача продолжает штатно переходить по FSM")
}

// createTaskInStatus создаёт задачу интеграции и доводит её реальным
// task.Transitioner до статуса status — общий шаг предусловия для тестов
// DeleteIntegrationsId (тикет 2.6). idempotencyKey ДОЛЖЕН быть уникален
// внутри теста (uq_tasks_idempotency, тикет 5.5) — вызывающий передаёт
// заведомо разные значения для нескольких задач одного пользователя.
func createTaskInStatus(ctx context.Context, t *testing.T, q *db.Queries, tr *task.Transitioner, userID, integrationID pgtype.UUID, idempotencyKey string, status task.Status) db.Task {
	t.Helper()
	taskRow, err := q.CreateTask(ctx, db.CreateTaskParams{
		UserID:         userID,
		IntegrationID:  integrationID,
		TextEnc:        []byte("задача для теста удаления интеграции"),
		IdempotencyKey: &idempotencyKey,
	})
	if err != nil {
		t.Fatalf("CreateTask (%s): %v", idempotencyKey, err)
	}

	// Путь created→queued→running→(waiting_user|stale) — общими Trigger'ами,
	// без прохождения через HTTP (это только подготовка состояния, не предмет
	// проверки самих тестов DeleteIntegrationsId).
	triggers := []task.Trigger{task.TriggerEnqueued}
	switch status {
	case task.StatusQueued:
		// уже достаточно TriggerEnqueued выше.
	case task.StatusRunning:
		triggers = append(triggers, task.TriggerTaskAccepted)
	case task.StatusWaitingUser:
		triggers = append(triggers, task.TriggerTaskAccepted, task.TriggerAgentQuestion)
	case task.StatusAwaitingConfirm:
		triggers = append(triggers, task.TriggerTaskAccepted, task.TriggerAgentCompleted)
	case task.StatusStale:
		triggers = append(triggers, task.TriggerTaskAccepted, task.TriggerTimeout)
	default:
		t.Fatalf("createTaskInStatus: не поддерживаемый целевой статус %s", status)
	}
	for _, trigger := range triggers {
		if _, _, terr := tr.Transition(ctx, taskRow.ID, trigger); terr != nil {
			t.Fatalf("подготовка (%s → %s): %v", idempotencyKey, trigger, terr)
		}
	}
	return taskRow
}

// TestIntegration_DeleteIntegrationsId_NoActiveTasks_DeletesImmediately —
// приёмка тикета 2.6 (FR B5): DELETE /integrations/{id} БЕЗ активных задач
// удаляет сразу (204), confirm не требуется. После удаления интеграция
// неотличима от несуществующей — GET/PATCH тоже отвечают 404 (ADR 0004,
// deleted_at IS NULL во всех owner-scoped запросах).
func TestIntegration_DeleteIntegrationsId_NoActiveTasks_DeletesImmediately(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupDB(ctx, t)
	defer done()
	q := db.New(pool)

	_, token := createTestUserWithToken(ctx, t, q, "alice-delete-no-tasks")

	router := api.NewRouter(api.NewServer(q, nil, []byte(testJWTSigningKey), []byte(testEncryptionKey32)))

	createRec := doIntegrationsRequest(t, router, http.MethodPost, "/integrations", token, api.IntegrationCreate{Name: "no-tasks-machine"})
	if createRec.Code != http.StatusCreated {
		t.Fatalf("создание: статус = %d (%s)", createRec.Code, createRec.Body.String())
	}
	var created api.IntegrationWithSecret
	if err := json.Unmarshal(createRec.Body.Bytes(), &created); err != nil {
		t.Fatalf("unmarshal create: %v", err)
	}

	// БЕЗ ?confirm=true — активных задач нет, подтверждение не требуется.
	deleteRec := doIntegrationsRequest(t, router, http.MethodDelete, "/integrations/"+created.Id.String(), token, nil)
	if deleteRec.Code != http.StatusNoContent {
		t.Fatalf("DELETE без активных задач: статус = %d (%s), ожидался 204", deleteRec.Code, deleteRec.Body.String())
	}

	getRec := doIntegrationsRequest(t, router, http.MethodGet, "/integrations/"+created.Id.String(), token, nil)
	if getRec.Code != http.StatusNotFound {
		t.Fatalf("GET после удаления: статус = %d (%s), ожидался 404", getRec.Code, getRec.Body.String())
	}

	listRec := doIntegrationsRequest(t, router, http.MethodGet, "/integrations", token, nil)
	if listRec.Code != http.StatusOK {
		t.Fatalf("GET /integrations после удаления: статус = %d (%s)", listRec.Code, listRec.Body.String())
	}
	var list []api.Integration
	if err := json.Unmarshal(listRec.Body.Bytes(), &list); err != nil {
		t.Fatalf("unmarshal list: %v", err)
	}
	if len(list) != 0 {
		t.Fatalf("список после удаления = %+v, ожидался пустой", list)
	}
	t.Logf("OK: без активных задач DELETE удаляет сразу (204), интеграция после этого неотличима от несуществующей (404, не в списке)")
}

// TestIntegration_DeleteIntegrationsId_ActiveTaskWithoutConfirm_Returns409 —
// приёмка тикета 2.6, Gherkin §2 «Удаление интеграции с активной задачей
// требует подтверждения»: с активной задачей и БЕЗ confirm=true DELETE
// отвечает 409 и НЕ трогает ни интеграцию, ни задачу.
func TestIntegration_DeleteIntegrationsId_ActiveTaskWithoutConfirm_Returns409(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupDB(ctx, t)
	defer done()
	q := db.New(pool)

	user, token := createTestUserWithToken(ctx, t, q, "alice-delete-409")
	integration := createTestIntegration(ctx, t, q, user.ID, "alice-machine-409")

	tr := task.NewTransitioner(pool)
	taskRow := createTaskInStatus(ctx, t, q, tr, user.ID, integration.ID, "delete-409-key-1", task.StatusRunning)

	router := api.NewRouter(api.NewServer(q, nil, []byte(testJWTSigningKey), []byte(testEncryptionKey32)))

	integrationID := uuid.UUID(integration.ID.Bytes)
	deleteRec := doIntegrationsRequest(t, router, http.MethodDelete, "/integrations/"+integrationID.String(), token, nil)
	if deleteRec.Code != http.StatusConflict {
		t.Fatalf("DELETE без confirm при активной задаче: статус = %d (%s), ожидался 409", deleteRec.Code, deleteRec.Body.String())
	}

	// Интеграция НЕ удалена — прямой SQL-чек deleted_at IS NULL. Намеренно не
	// через GET /integrations/{id}: тот расшифровывает uuid_enc
	// (crypto.Decrypt), а createTestIntegration (tasks_integration_test.go)
	// кладёт туда сырые байты секрета, а не настоящий AEAD-шифротекст — GET
	// упал бы 500 по причине, не относящейся к тому, что здесь проверяется
	// (тому, что реализует и проверяет TestIntegration_Integrations_Get*).
	var deletedAt pgtype.Timestamptz
	if err := pool.QueryRow(ctx, `SELECT deleted_at FROM integrations WHERE id = $1`, integration.ID).Scan(&deletedAt); err != nil {
		t.Fatalf("SELECT deleted_at после 409: %v", err)
	}
	if deletedAt.Valid {
		t.Fatalf("integrations.deleted_at после 409 = %v, ожидался NULL (интеграция не должна быть удалена)", deletedAt)
	}

	// Задача НЕ тронута — по-прежнему running, не cancelled.
	if status := getTaskRowStatus(ctx, t, pool, taskRow.ID); status != string(task.StatusRunning) {
		t.Fatalf("tasks.status после 409 = %s, ожидался running (задача не должна быть отменена без confirm)", status)
	}
	t.Logf("OK: активная задача без confirm=true → 409, интеграция и задача не тронуты")
}

// TestIntegration_DeleteIntegrationsId_ActiveTaskWithConfirm_CancelsAndDeletes —
// приёмка тикета 2.6 (FR B5, Gherkin §2): с confirm=true задача корректно
// отменяется через FSM (Transition → cancelled), затем интеграция мягко
// удаляется (ADR 0004) — единственный физический эффект: deleted_at,
// tasks/task_events не удаляются (FR I2).
func TestIntegration_DeleteIntegrationsId_ActiveTaskWithConfirm_CancelsAndDeletes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupDB(ctx, t)
	defer done()
	q := db.New(pool)

	user, token := createTestUserWithToken(ctx, t, q, "alice-delete-confirm")
	integration := createTestIntegration(ctx, t, q, user.ID, "alice-machine-confirm")

	tr := task.NewTransitioner(pool)
	taskRow := createTaskInStatus(ctx, t, q, tr, user.ID, integration.ID, "delete-confirm-key-1", task.StatusRunning)

	server := api.NewServer(q, nil, []byte(testJWTSigningKey), []byte(testEncryptionKey32))
	server.SetTransitioner(tr)
	server.SetCommandPublisher(noopCommandPublisher{})
	router := api.NewRouter(server)

	integrationID := uuid.UUID(integration.ID.Bytes)
	deleteRec := doIntegrationsRequest(t, router, http.MethodDelete, "/integrations/"+integrationID.String()+"?confirm=true", token, nil)
	if deleteRec.Code != http.StatusNoContent {
		t.Fatalf("DELETE с confirm=true: статус = %d (%s), ожидался 204", deleteRec.Code, deleteRec.Body.String())
	}

	if status := getTaskRowStatus(ctx, t, pool, taskRow.ID); status != string(task.StatusCancelled) {
		t.Fatalf("tasks.status после DELETE?confirm=true = %s, ожидался cancelled", status)
	}

	// tasks-строка сохранена (FR I2, история бессрочна) — не удалена, не
	// осиротела (integration_id по-прежнему указывает на исходную интеграцию).
	var stillIntegrationID pgtype.UUID
	if err := pool.QueryRow(ctx, `SELECT integration_id FROM tasks WHERE id = $1`, taskRow.ID).Scan(&stillIntegrationID); err != nil {
		t.Fatalf("SELECT tasks.integration_id после удаления интеграции: %v", err)
	}
	if stillIntegrationID != integration.ID {
		t.Fatalf("tasks.integration_id после удаления интеграции = %v, ожидался неизменным %v (FK ON DELETE RESTRICT/ADR 0004 — строка не должна осиротеть)", stillIntegrationID, integration.ID)
	}

	getRec := doIntegrationsRequest(t, router, http.MethodGet, "/integrations/"+integrationID.String(), token, nil)
	if getRec.Code != http.StatusNotFound {
		t.Fatalf("GET после удаления: статус = %d (%s), ожидался 404", getRec.Code, getRec.Body.String())
	}
	t.Logf("OK: confirm=true отменяет активную задачу (cancelled) и мягко удаляет интеграцию (204, затем 404); история задачи (tasks-строка) сохранена")
}

// TestIntegration_DeleteIntegrationsId_MultipleActiveTasks_AllCancelled —
// приёмка тикета 2.6: несколько активных задач интеграции в РАЗНЫХ активных
// статусах (queued/running/waiting_user) — confirm=true отменяет КАЖДУЮ из
// них, ни одна не остаётся в промежуточном статусе.
func TestIntegration_DeleteIntegrationsId_MultipleActiveTasks_AllCancelled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupDB(ctx, t)
	defer done()
	q := db.New(pool)

	user, token := createTestUserWithToken(ctx, t, q, "alice-delete-multi")
	integration := createTestIntegration(ctx, t, q, user.ID, "alice-machine-multi")

	tr := task.NewTransitioner(pool)
	queuedTask := createTaskInStatus(ctx, t, q, tr, user.ID, integration.ID, "delete-multi-key-queued", task.StatusQueued)
	runningTask := createTaskInStatus(ctx, t, q, tr, user.ID, integration.ID, "delete-multi-key-running", task.StatusRunning)
	waitingTask := createTaskInStatus(ctx, t, q, tr, user.ID, integration.ID, "delete-multi-key-waiting", task.StatusWaitingUser)

	server := api.NewServer(q, nil, []byte(testJWTSigningKey), []byte(testEncryptionKey32))
	server.SetTransitioner(tr)
	server.SetCommandPublisher(noopCommandPublisher{})
	router := api.NewRouter(server)

	integrationID := uuid.UUID(integration.ID.Bytes)
	deleteRec := doIntegrationsRequest(t, router, http.MethodDelete, "/integrations/"+integrationID.String()+"?confirm=true", token, nil)
	if deleteRec.Code != http.StatusNoContent {
		t.Fatalf("DELETE с confirm=true (3 активные задачи): статус = %d (%s), ожидался 204", deleteRec.Code, deleteRec.Body.String())
	}

	for name, taskRow := range map[string]db.Task{"queued": queuedTask, "running": runningTask, "waiting_user": waitingTask} {
		if status := getTaskRowStatus(ctx, t, pool, taskRow.ID); status != string(task.StatusCancelled) {
			t.Fatalf("tasks.status задачи %s после DELETE?confirm=true = %s, ожидался cancelled", name, status)
		}
	}
	t.Logf("OK: DELETE?confirm=true отменяет ВСЕ активные задачи интеграции (queued, running, waiting_user → cancelled)")
}

// TestIntegration_DeleteIntegrationsId_OtherUsersIntegration_404 — DELETE
// чужой интеграции → 404 (не 403, owner isolation, FR A4, I3, тот же приём,
// что и у GET/PATCH). Чужая интеграция не удаляется.
func TestIntegration_DeleteIntegrationsId_OtherUsersIntegration_404(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupDB(ctx, t)
	defer done()
	q := db.New(pool)

	_, aliceToken := createTestUserWithToken(ctx, t, q, "alice-delete-owner")
	_, bobToken := createTestUserWithToken(ctx, t, q, "bob-delete-other")

	router := api.NewRouter(api.NewServer(q, nil, []byte(testJWTSigningKey), []byte(testEncryptionKey32)))

	createRec := doIntegrationsRequest(t, router, http.MethodPost, "/integrations", aliceToken, api.IntegrationCreate{Name: "alice-protected-machine"})
	if createRec.Code != http.StatusCreated {
		t.Fatalf("создание: статус = %d (%s)", createRec.Code, createRec.Body.String())
	}
	var created api.IntegrationWithSecret
	if err := json.Unmarshal(createRec.Body.Bytes(), &created); err != nil {
		t.Fatalf("unmarshal create: %v", err)
	}

	// Боб пытается удалить интеграцию Алисы.
	bobDelete := doIntegrationsRequest(t, router, http.MethodDelete, "/integrations/"+created.Id.String()+"?confirm=true", bobToken, nil)
	if bobDelete.Code != http.StatusNotFound {
		t.Fatalf("DELETE чужой интеграции: статус = %d (%s), ожидался 404", bobDelete.Code, bobDelete.Body.String())
	}

	// Интеграция Алисы по-прежнему существует.
	aliceGet := doIntegrationsRequest(t, router, http.MethodGet, "/integrations/"+created.Id.String(), aliceToken, nil)
	if aliceGet.Code != http.StatusOK {
		t.Fatalf("GET владельцем после чужой попытки DELETE: статус = %d (%s), ожидался 200", aliceGet.Code, aliceGet.Body.String())
	}
	t.Logf("OK: DELETE чужой интеграции → 404, сама интеграция не удаляется")
}

// TestIntegration_DeleteIntegrationsId_NotFound_404 — DELETE несуществующего
// id → 404 (тот же единый ответ, что и для чужой интеграции).
func TestIntegration_DeleteIntegrationsId_NotFound_404(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupDB(ctx, t)
	defer done()
	q := db.New(pool)

	_, token := createTestUserWithToken(ctx, t, q, "alice-delete-notfound")

	router := api.NewRouter(api.NewServer(q, nil, []byte(testJWTSigningKey), []byte(testEncryptionKey32)))

	deleteRec := doIntegrationsRequest(t, router, http.MethodDelete, "/integrations/"+uuid.New().String(), token, nil)
	if deleteRec.Code != http.StatusNotFound {
		t.Fatalf("DELETE несуществующей интеграции: статус = %d (%s), ожидался 404", deleteRec.Code, deleteRec.Body.String())
	}
	t.Logf("OK: DELETE несуществующего id → 404")
}

// TestIntegration_DeleteIntegrationsId_DeletedIntegrationCannotAuthenticateMachine —
// критичный для безопасности сценарий ADR 0004
// (docs/adr/0004-integration-soft-delete.md): после мягкого удаления
// интеграция не может пройти WS-аутентификацию машины повторным
// предъявлением своего UUID-секрета — GetIntegrationByUUIDHMAC (путь
// machine_ws.go, тикет 2.3) тоже фильтрует deleted_at IS NULL и после
// удаления не находит строку (pgx.ErrNoRows), как для несуществующей
// интеграции.
func TestIntegration_DeleteIntegrationsId_DeletedIntegrationCannotAuthenticateMachine(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupDB(ctx, t)
	defer done()
	q := db.New(pool)

	user, token := createTestUserWithToken(ctx, t, q, "alice-delete-ws-auth")
	integration := createTestIntegration(ctx, t, q, user.ID, "alice-machine-ws-auth")

	// Предусловие: ДО удаления интеграция аутентифицируется по своему
	// uuid_hmac (тот же путь, что GetMachineWs на WS-handshake).
	if _, err := q.GetIntegrationByUUIDHMAC(ctx, integration.UuidHmac); err != nil {
		t.Fatalf("GetIntegrationByUUIDHMAC ДО удаления: %v, ожидался успех", err)
	}

	router := api.NewRouter(api.NewServer(q, nil, []byte(testJWTSigningKey), []byte(testEncryptionKey32)))

	integrationID := uuid.UUID(integration.ID.Bytes)
	deleteRec := doIntegrationsRequest(t, router, http.MethodDelete, "/integrations/"+integrationID.String(), token, nil)
	if deleteRec.Code != http.StatusNoContent {
		t.Fatalf("DELETE: статус = %d (%s), ожидался 204", deleteRec.Code, deleteRec.Body.String())
	}

	// ПОСЛЕ удаления тот же uuid_hmac больше не находится — машина не может
	// «воскресить» удалённую интеграцию повторным hello.
	if _, err := q.GetIntegrationByUUIDHMAC(ctx, integration.UuidHmac); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("GetIntegrationByUUIDHMAC ПОСЛЕ удаления: err=%v, ожидался pgx.ErrNoRows (удалённая интеграция не должна аутентифицировать машину)", err)
	}
	t.Logf("OK: удалённая интеграция не проходит GetIntegrationByUUIDHMAC — не может повторно аутентифицировать машину на WS-handshake")
}
