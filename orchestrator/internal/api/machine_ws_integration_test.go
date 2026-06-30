//go:build integration

// Integration-тесты WS-handshake аутентификации машины по UUID на РЕАЛЬНОМ
// Postgres через testcontainers-go (тикет 2.3, FR B3, B6, Gherkin §2
// «Аутентификация машины по UUID», «IP — необязательная проверка»). Тег
// integration, setupDB/testJWTSigningKey/testEncryptionKey32 и
// createTestUserWithToken/doIntegrationsRequest переиспользуются из
// register_integration_test.go/integrations_integration_test.go (тот же
// пакет api_test).
//
// В отличие от остальных интеграционных тестов пакета (httptest.Recorder
// поверх router), WS-handshake требует реального TCP/HTTP-сервера — Hijack,
// нужный для апгрейда до WebSocket, httptest.ResponseRecorder не
// поддерживает. Поэтому здесь поднимается httptest.NewServer(router): тот же
// собранный router используется и для предварительных REST-вызовов (создание
// интеграции, PATCH ip_hint через doIntegrationsRequest), и для самого
// WS-подключения (httptest.NewServer.URL со схемой http:// заменяется на
// ws://).
//
// Подлинный secret UUID и соответствующий ему uuid_hmac в БД получаются НЕ
// ручным пересчётом DeriveKey/HMAC в тесте (задублировало бы внутреннюю
// логику пакета api и было бы хрупко против рефакторинга), а через реальный
// POST /integrations (тикет 2.2, уже реализован): IntegrationWithSecret.Uuid
// из ответа — это и есть plaintext UUID, который агент предъявит в hello.
// Для ip_hint-тестов используется PATCH /integrations/{id} (тоже уже
// реализован тикетом 2.2), чтобы выставить нужный ip_hint перед
// WS-подключением.
//
// Покрываем приёмочные сценарии тикета 2.3:
//   - валидный UUID + пустой ip_hint → соединение принято (не закрыто 4401);
//   - валидный UUID + ip_hint совпадает с реальным IP клиента (127.0.0.1 для
//     httptest.NewServer) → принято;
//   - валидный UUID + ip_hint, заведомо НЕ совпадающий с реальным IP клиента
//     → отклонено close-кодом 4401;
//   - случайный/несуществующий UUID → отклонено close-кодом 4401;
//   - первый кадр, который не парсится как валидный hello → отклонено
//     close-кодом 4401.
package api_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"

	"github.com/yarabey/agentify/orchestrator/internal/api"
	"github.com/yarabey/agentify/orchestrator/internal/db"
)

// wsCloseUnauthorized — close-код, которым GetMachineWs закрывает соединение
// при провале WS-аутентификации машины (см.
// orchestrator/internal/api/machine_ws.go). Дублируется здесь как константа
// теста, а не импортируется: символ в пакете api неэкспортирован намеренно
// (деталь реализации, не часть публичного контракта пакета).
const wsCloseUnauthorized websocket.StatusCode = 4401

// wsHelloEnvelope/wsHelloPayload — JSON-конверт и payload сообщения
// type == "hello" (docs/protocol.md §2/§4), собираемые тестом со стороны
// "агента" для отправки на /machine/ws. Поля, не участвующие в проверке
// тикета 2.3 (message_id/integration_id/seq/ts/protocol_version),
// заполняются произвольными, но валидными по форме значениями — сервер их в
// этом тикете не валидирует (см. machine_ws.go), однако конверт должен быть
// синтаксически полным, как того требует протокол.
type wsHelloEnvelope struct {
	MessageID       string         `json:"message_id"`
	TaskID          *string        `json:"task_id"`
	IntegrationID   string         `json:"integration_id"`
	Type            string         `json:"type"`
	Seq             int64          `json:"seq"`
	Ts              string         `json:"ts"`
	ProtocolVersion string         `json:"protocol_version"`
	Payload         wsHelloPayload `json:"payload"`
}

// wsHelloPayload — payload сообщения hello (docs/protocol.md §4):
// {uuid, agent_version, providers[]}.
type wsHelloPayload struct {
	UUID         string   `json:"uuid"`
	AgentVersion string   `json:"agent_version"`
	Providers    []string `json:"providers"`
}

// dialMachineWS открывает WS-соединение к /machine/ws тестового HTTP-сервера
// (схема ws:// — httptest.NewServer всегда отдаёт http://).
func dialMachineWS(ctx context.Context, t *testing.T, ts *httptest.Server) *websocket.Conn {
	t.Helper()
	wsURL := strings.Replace(ts.URL, "http://", "ws://", 1) + "/machine/ws"
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("websocket.Dial: %v", err)
	}
	return conn
}

// sendHello отправляет hello-кадр с предъявленным uuidSecret как первый кадр
// WS-соединения (docs/protocol.md §4, аутентификация машины по UUID).
func sendHello(ctx context.Context, t *testing.T, conn *websocket.Conn, uuidSecret string) {
	t.Helper()
	env := wsHelloEnvelope{
		MessageID:       uuid.NewString(),
		TaskID:          nil,
		IntegrationID:   uuid.Nil.String(),
		Type:            "hello",
		Seq:             1,
		Ts:              time.Now().UTC().Format(time.RFC3339),
		ProtocolVersion: "1",
		Payload: wsHelloPayload{
			UUID:         uuidSecret,
			AgentVersion: "test-agent/0.0.0",
			Providers:    []string{"claude"},
		},
	}
	raw, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal hello: %v", err)
	}
	if err := conn.Write(ctx, websocket.MessageText, raw); err != nil {
		t.Fatalf("write hello: %v", err)
	}
}

// wsAccepted определяет, осталось ли соединение открытым после hello
// (приёмка тикета 2.3): пытается прочитать следующий кадр с коротким
// таймаутом. Отказ сервер сигнализирует немедленным close 4401 (см.
// machine_ws.go); успешный путь сервера ничего не пишет в ответ и просто
// блокируется на чтении (минимальный read-loop тикета 2.3), поэтому попытка
// чтения с таймаутом в этом случае упирается в истечение контекста
// (context.DeadlineExceeded), а не в close-фрейм.
func wsAccepted(t *testing.T, conn *websocket.Conn) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _, err := conn.Read(ctx)
	switch {
	case err == nil:
		// Сервер тикета 2.3 ничего не пишет на успешном пути — неожиданно,
		// но раз кадр пришёл без ошибки, соединение точно не закрыто 4401.
		return true
	case websocket.CloseStatus(err) == wsCloseUnauthorized:
		return false
	case errors.Is(err, context.DeadlineExceeded):
		return true
	default:
		t.Fatalf("неожиданная ошибка чтения после hello: %v", err)
		return false
	}
}

// TestIntegration_MachineWS_ValidUUIDEmptyIPHintAccepted — валидный UUID +
// пустой ip_hint → соединение принято (FR B3, §2 «IP — необязательная
// проверка»: пустой ip_hint пропускает любой IP).
func TestIntegration_MachineWS_ValidUUIDEmptyIPHintAccepted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupDB(ctx, t)
	defer done()
	q := db.New(pool)

	_, token := createTestUserWithToken(ctx, t, q, "machinews-valid-empty-hint")
	router := api.NewRouter(api.NewServer(q, nil, []byte(testJWTSigningKey), []byte(testEncryptionKey32)))

	createRec := doIntegrationsRequest(t, router, http.MethodPost, "/integrations", token, api.IntegrationCreate{Name: "laptop-empty-hint"})
	if createRec.Code != http.StatusCreated {
		t.Fatalf("создание интеграции: статус = %d (%s)", createRec.Code, createRec.Body.String())
	}
	var created api.IntegrationWithSecret
	if err := json.Unmarshal(createRec.Body.Bytes(), &created); err != nil {
		t.Fatalf("unmarshal create: %v", err)
	}

	ts := httptest.NewServer(router)
	defer ts.Close()

	conn := dialMachineWS(ctx, t, ts)
	defer conn.CloseNow()

	sendHello(ctx, t, conn, created.Uuid.String())
	if !wsAccepted(t, conn) {
		t.Fatalf("ожидалось, что соединение принято (валидный UUID, пустой ip_hint), но оно закрыто")
	}
	t.Logf("OK: валидный UUID + пустой ip_hint → принято")
}

// TestIntegration_MachineWS_ValidUUIDMatchingIPHintAccepted — валидный UUID +
// ip_hint совпадает с реальным IP клиента (127.0.0.1 для httptest.NewServer)
// → принято (FR B3, §2 «IP — необязательная проверка»: указанный ip_hint
// сверяется и, при совпадении, не блокирует подключение).
func TestIntegration_MachineWS_ValidUUIDMatchingIPHintAccepted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupDB(ctx, t)
	defer done()
	q := db.New(pool)

	_, token := createTestUserWithToken(ctx, t, q, "machinews-valid-matching-hint")
	router := api.NewRouter(api.NewServer(q, nil, []byte(testJWTSigningKey), []byte(testEncryptionKey32)))

	createRec := doIntegrationsRequest(t, router, http.MethodPost, "/integrations", token, api.IntegrationCreate{Name: "laptop-matching-hint"})
	if createRec.Code != http.StatusCreated {
		t.Fatalf("создание интеграции: статус = %d (%s)", createRec.Code, createRec.Body.String())
	}
	var created api.IntegrationWithSecret
	if err := json.Unmarshal(createRec.Body.Bytes(), &created); err != nil {
		t.Fatalf("unmarshal create: %v", err)
	}

	matchingIP := "127.0.0.1"
	patchRec := doIntegrationsRequest(t, router, http.MethodPatch, "/integrations/"+created.Id.String(), token, api.IntegrationUpdate{IpHint: &matchingIP})
	if patchRec.Code != http.StatusOK {
		t.Fatalf("PATCH ip_hint: статус = %d (%s)", patchRec.Code, patchRec.Body.String())
	}

	ts := httptest.NewServer(router)
	defer ts.Close()

	conn := dialMachineWS(ctx, t, ts)
	defer conn.CloseNow()

	sendHello(ctx, t, conn, created.Uuid.String())
	if !wsAccepted(t, conn) {
		t.Fatalf("ожидалось, что соединение принято (валидный UUID, ip_hint совпадает), но оно закрыто")
	}
	t.Logf("OK: валидный UUID + совпадающий ip_hint → принято")
}

// TestIntegration_MachineWS_MismatchedIPHintRejected — валидный UUID +
// ip_hint, заведомо НЕ совпадающий с реальным IP клиента → отклонено
// close-кодом 4401 (FR B3, §2 «IP — необязательная проверка»: указанный
// ip_hint, если не совпал, блокирует подключение несмотря на верный UUID).
func TestIntegration_MachineWS_MismatchedIPHintRejected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupDB(ctx, t)
	defer done()
	q := db.New(pool)

	_, token := createTestUserWithToken(ctx, t, q, "machinews-mismatched-hint")
	router := api.NewRouter(api.NewServer(q, nil, []byte(testJWTSigningKey), []byte(testEncryptionKey32)))

	createRec := doIntegrationsRequest(t, router, http.MethodPost, "/integrations", token, api.IntegrationCreate{Name: "laptop-mismatched-hint"})
	if createRec.Code != http.StatusCreated {
		t.Fatalf("создание интеграции: статус = %d (%s)", createRec.Code, createRec.Body.String())
	}
	var created api.IntegrationWithSecret
	if err := json.Unmarshal(createRec.Body.Bytes(), &created); err != nil {
		t.Fatalf("unmarshal create: %v", err)
	}

	// TEST-NET-3 (RFC 5737) — заведомо не адрес httptest.NewServer.
	otherIP := "203.0.113.99"
	patchRec := doIntegrationsRequest(t, router, http.MethodPatch, "/integrations/"+created.Id.String(), token, api.IntegrationUpdate{IpHint: &otherIP})
	if patchRec.Code != http.StatusOK {
		t.Fatalf("PATCH ip_hint: статус = %d (%s)", patchRec.Code, patchRec.Body.String())
	}

	ts := httptest.NewServer(router)
	defer ts.Close()

	conn := dialMachineWS(ctx, t, ts)
	defer conn.CloseNow()

	sendHello(ctx, t, conn, created.Uuid.String())
	if wsAccepted(t, conn) {
		t.Fatalf("ожидалось отклонение (валидный UUID, ip_hint не совпадает), но соединение принято")
	}
	t.Logf("OK: валидный UUID + несовпадающий ip_hint → отклонено (4401)")
}

// TestIntegration_MachineWS_UnknownUUIDRejected — случайный/несуществующий
// UUID → отклонено close-кодом 4401 (FR B6: основной механизм
// аутентификации — поиск по HMAC; не найден → единый отказ).
func TestIntegration_MachineWS_UnknownUUIDRejected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupDB(ctx, t)
	defer done()
	q := db.New(pool)

	router := api.NewRouter(api.NewServer(q, nil, []byte(testJWTSigningKey), []byte(testEncryptionKey32)))

	ts := httptest.NewServer(router)
	defer ts.Close()

	conn := dialMachineWS(ctx, t, ts)
	defer conn.CloseNow()

	// uuid.NewString() — валидный по форме UUID, но заведомо отсутствующий в
	// integrations.uuid_hmac (никогда не создавался через POST /integrations).
	sendHello(ctx, t, conn, uuid.NewString())
	if wsAccepted(t, conn) {
		t.Fatalf("ожидалось отклонение (несуществующий UUID), но соединение принято")
	}
	t.Logf("OK: несуществующий UUID → отклонено (4401)")
}

// TestIntegration_MachineWS_MalformedFrameRejected — первый кадр, который не
// парсится как валидный hello → отклонено close-кодом 4401 (тот же единый
// сигнал отказа, что и у остальных причин — без утечки, что именно не так).
func TestIntegration_MachineWS_MalformedFrameRejected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, done := setupDB(ctx, t)
	defer done()
	q := db.New(pool)

	router := api.NewRouter(api.NewServer(q, nil, []byte(testJWTSigningKey), []byte(testEncryptionKey32)))

	ts := httptest.NewServer(router)
	defer ts.Close()

	conn := dialMachineWS(ctx, t, ts)
	defer conn.CloseNow()

	if err := conn.Write(ctx, websocket.MessageText, []byte(`{"this is": "not a hello envelope"}`)); err != nil {
		t.Fatalf("write malformed frame: %v", err)
	}
	if wsAccepted(t, conn) {
		t.Fatalf("ожидалось отклонение (некорректный первый кадр), но соединение принято")
	}
	t.Logf("OK: некорректный первый кадр (не hello) → отклонено (4401)")
}
