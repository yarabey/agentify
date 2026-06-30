//go:build integration

// Integration-тесты защиты от повторного UUID на WS-handshake /machine/ws
// (тикет 2.4, FR B6, ADR 0002 "machine-ws duplicate uuid policy"). Тег
// integration, тот же пакет api_test, что и machine_ws_integration_test.go —
// dialMachineWS/sendHello/wsAccepted/wsCloseUnauthorized и
// setupDB/createTestUserWithToken/doIntegrationsRequest/testJWTSigningKey/
// testEncryptionKey32 переиспользуются оттуда без дублирования.
//
// Политика (ADR 0002): второй успешно аутентифицированный коннект с тем же
// integration_id вытесняет первый — сервер закрывает СТАРОЕ соединение
// кодом wsCloseSuperseded (4409) и принимает НОВОЕ. Реестр активных
// соединений ведётся per-integration_id (orchestrator/internal/api/server.go,
// Server.machineConns), поэтому разные интеграции друг друга не вытесняют —
// второй тест это и проверяет.
//
// Важно про порядок чтений на одном соединении: coder/websocket закрывает
// соединение НАВСЕГДА (Conn.close()), если переданный в Read контекст
// реально истекает, пока чтение блокировано (см. context.AfterFunc в
// conn.go: setupReadTimeout) — это относится и к wsAccepted, чья ветка
// errors.Is(err, context.DeadlineExceeded) возвращает true именно так. Поэтому
// ниже КАЖДОЕ соединение читается через Read/wsAccepted не более одного раза:
// повторное чтение того же conn после уже сработавшего по таймауту Read
// получает «use of closed network connection», а не реальный close-кадр от
// сервера — это особенность библиотеки, а не баг вытеснения.
package api_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/yarabey/agentify/orchestrator/internal/api"
	"github.com/yarabey/agentify/orchestrator/internal/db"
)

// wsCloseSuperseded — close-код, которым registerMachineConn закрывает
// вытесненное соединение (см. orchestrator/internal/api/machine_ws.go).
// Дублируется здесь как константа теста по той же причине, что и
// wsCloseUnauthorized в machine_ws_integration_test.go: символ в пакете api
// неэкспортирован намеренно (деталь реализации).
const wsCloseSuperseded websocket.StatusCode = 4409

// TestIntegration_MachineWS_DuplicateUUID_SecondSupersedesFirst — второй
// коннект с ОДНИМ И ТЕМ ЖЕ валидным UUID-секретом, пока первый ещё активен:
// второй принимается, а первый получает server-initiated close 4409
// ("close old, accept new" — ADR 0002, FR B6).
func TestIntegration_MachineWS_DuplicateUUID_SecondSupersedesFirst(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupDB(ctx, t)
	defer done()
	q := db.New(pool)

	_, token := createTestUserWithToken(ctx, t, q, "machinews-duplicate-uuid")
	router := api.NewRouter(api.NewServer(q, nil, []byte(testJWTSigningKey), []byte(testEncryptionKey32)))

	createRec := doIntegrationsRequest(t, router, http.MethodPost, "/integrations", token, api.IntegrationCreate{Name: "laptop-duplicate"})
	if createRec.Code != http.StatusCreated {
		t.Fatalf("создание интеграции: статус = %d (%s)", createRec.Code, createRec.Body.String())
	}
	var created api.IntegrationWithSecret
	if err := json.Unmarshal(createRec.Body.Bytes(), &created); err != nil {
		t.Fatalf("unmarshal create: %v", err)
	}

	ts := httptest.NewServer(router)
	defer ts.Close()

	// Первый коннект устанавливается и проходит hello, но СОЗНАТЕЛЬНО не
	// читается здесь (см. комментарий к файлу про DeadlineExceeded): первое
	// чтение first будет ниже, один раз, и совмещает в себе проверку и
	// «был принят», и «вытеснен вторым» — если бы hello первого не прошло
	// аутентификацию, ниже мы увидели бы close 4401, а не 4409, и тест
	// упал бы с понятным сообщением.
	first := dialMachineWS(ctx, t, ts)
	defer first.CloseNow()
	sendHello(ctx, t, first, created.Uuid.String())

	// Второй коннект с тем же UUID-секретом — тоже принят (та же интеграция,
	// тот же валидный секрет; политика не про отказ второму, см. ADR 0002).
	second := dialMachineWS(ctx, t, ts)
	defer second.CloseNow()
	sendHello(ctx, t, second, created.Uuid.String())
	if !wsAccepted(t, second) {
		t.Fatalf("второй коннект должен быть принят (вытесняющая политика, не отвергающая)")
	}

	// Первое соединение должно было получить server-initiated close 4409 в
	// момент регистрации второго (registerMachineConn закрывает старое сразу
	// после успешной аутентификации нового, см. machine_ws.go). Единственное
	// чтение first — здесь.
	readCtx, readCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer readCancel()
	_, _, err := first.Read(readCtx)
	if err == nil {
		t.Fatalf("ожидалось закрытие первого соединения (вытеснено вторым), но чтение не вернуло ошибку")
	}
	if status := websocket.CloseStatus(err); status != wsCloseSuperseded {
		t.Fatalf("первое соединение: ожидался close-код %d (superseded), получено: %v (close status %d)", wsCloseSuperseded, err, status)
	}
}

// TestIntegration_MachineWS_DifferentIntegrations_DoNotSupersedeEachOther —
// две РАЗНЫЕ интеграции (разные валидные UUID-секреты) подключаются
// одновременно: ни одна не вытесняет другую (реестр вытеснения — per
// integration_id, не глобальный лок на одно соединение вообще).
func TestIntegration_MachineWS_DifferentIntegrations_DoNotSupersedeEachOther(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupDB(ctx, t)
	defer done()
	q := db.New(pool)

	_, token := createTestUserWithToken(ctx, t, q, "machinews-distinct-integrations")
	router := api.NewRouter(api.NewServer(q, nil, []byte(testJWTSigningKey), []byte(testEncryptionKey32)))

	createA := doIntegrationsRequest(t, router, http.MethodPost, "/integrations", token, api.IntegrationCreate{Name: "laptop-a"})
	if createA.Code != http.StatusCreated {
		t.Fatalf("создание интеграции A: статус = %d (%s)", createA.Code, createA.Body.String())
	}
	var integrationA api.IntegrationWithSecret
	if err := json.Unmarshal(createA.Body.Bytes(), &integrationA); err != nil {
		t.Fatalf("unmarshal create A: %v", err)
	}

	createB := doIntegrationsRequest(t, router, http.MethodPost, "/integrations", token, api.IntegrationCreate{Name: "laptop-b"})
	if createB.Code != http.StatusCreated {
		t.Fatalf("создание интеграции B: статус = %d (%s)", createB.Code, createB.Body.String())
	}
	var integrationB api.IntegrationWithSecret
	if err := json.Unmarshal(createB.Body.Bytes(), &integrationB); err != nil {
		t.Fatalf("unmarshal create B: %v", err)
	}

	ts := httptest.NewServer(router)
	defer ts.Close()

	// A устанавливается и проходит hello, но СОЗНАТЕЛЬНО не читается здесь —
	// единственное чтение connA ниже, ПОСЛЕ того как подключится B, чтобы
	// один и тот же Read одновременно подтверждал и «A был принят», и «A не
	// вытеснен подключением B» (см. комментарий к файлу про
	// DeadlineExceeded: повторное чтение того же conn после сработавшего по
	// таймауту Read для library — уже закрытое соединение, а не реальная
	// проверка).
	connA := dialMachineWS(ctx, t, ts)
	defer connA.CloseNow()
	sendHello(ctx, t, connA, integrationA.Uuid.String())

	connB := dialMachineWS(ctx, t, ts)
	defer connB.CloseNow()
	sendHello(ctx, t, connB, integrationB.Uuid.String())
	if !wsAccepted(t, connB) {
		t.Fatalf("интеграция B должна быть принята")
	}

	// Обе интеграции — разные integration_id в реестре сервера, поэтому
	// подключение B не должно было закрыть A: A всё ещё открыто и принимает.
	if !wsAccepted(t, connA) {
		t.Fatalf("интеграция A не должна быть вытеснена подключением другой интеграции B")
	}
}
