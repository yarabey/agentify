// Unit/router-тесты клиентского WS-эндпоинта /ws (тикет 7.2, FR G1, §6
// «Уведомление в web по WebSocket»).
//
// В отличие от machine_ws_integration_test.go (тег integration, реальный
// Postgres) — /ws не обращается к БД вовсе (аутентификация целиком по
// access-JWT, см. authenticateClientWS), поэтому все сценарии, включая
// сквозной ("агент задаёт вопрос → уведомление приходит в уже открытую
// вкладку"), гоняются здесь как обычные unit-тесты пакета api поверх
// httptest.NewServer(NewRouter(...)) с fakeQuerier — без build-тега
// integration.
//
// Покрываем приёмочные сценарии тикета 7.2:
//   - валидный auth-кадр → соединение принято (не закрыто 4401);
//   - невалидный/просроченный access_token → отклонено 4401;
//   - первый кадр не type=="auth" (в т.ч. попытка прислать конверт машинного
//     протокола) → отклонено 4401;
//   - первый кадр не JSON → отклонено 4401;
//   - ДВЕ одновременные вкладки одного пользователя → ОБЕ регистрируются и
//     ОБЕ получают одно и то же уведомление (ADR 0005, в отличие от ADR 0002
//     для /machine/ws);
//   - ClientConnHub.Notify для пользователя без открытых вкладок — не ошибка;
//   - сквозной сценарий: agent_question через handleMachineFrame (тикет 7.1)
//     с реальным *ClientConnHub, зарегистрированным как Notifier (тот же
//     wiring, что и orchestrator/main.go), доставляет уведомление в УЖЕ
//     открытое WS-соединение /ws того же user_id, без переподключения — это
//     Go-эквивалент требования тикета «e2e: вопрос → уведомление в UI без
//     reload».
package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/yarabey/agentify/internal/bus"
	"github.com/yarabey/agentify/orchestrator/internal/db"
	"github.com/yarabey/agentify/orchestrator/internal/notify"
	"github.com/yarabey/agentify/orchestrator/internal/task"
)

// dialClientWS открывает WS-соединение к /ws тестового HTTP-сервера (схема
// ws:// — httptest.NewServer всегда отдаёт http://).
func dialClientWS(ctx context.Context, t *testing.T, ts *httptest.Server) *websocket.Conn {
	t.Helper()
	wsURL := strings.Replace(ts.URL, "http://", "ws://", 1) + "/ws"
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("websocket.Dial: %v", err)
	}
	return conn
}

// sendClientAuthFrame отправляет auth-кадр {"type":"auth","access_token":...}
// как первый кадр WS-соединения (см. authenticateClientWS в client_ws.go).
func sendClientAuthFrame(ctx context.Context, t *testing.T, conn *websocket.Conn, accessToken string) {
	t.Helper()
	raw, err := json.Marshal(clientAuthFrame{Type: "auth", AccessToken: accessToken})
	if err != nil {
		t.Fatalf("marshal clientAuthFrame: %v", err)
	}
	if err := conn.Write(ctx, websocket.MessageText, raw); err != nil {
		t.Fatalf("write auth-кадра: %v", err)
	}
}

// clientWSAccepted определяет, осталось ли соединение открытым после
// auth-кадра (приёмка тикета 7.2): пытается прочитать следующий кадр с
// коротким таймаутом. Отказ сервер сигнализирует немедленным close 4401 (см.
// client_ws.go); успешный путь ничего не пишет в ответ (строго
// однонаправленный канал сервер→браузер до первого реального уведомления) и
// просто блокируется на чтении, поэтому попытка чтения с таймаутом в этом
// случае упирается в истечение контекста (context.DeadlineExceeded), а не в
// close-фрейм — тот же приём, что и wsAccepted в
// machine_ws_integration_test.go.
func clientWSAccepted(t *testing.T, conn *websocket.Conn) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _, err := conn.Read(ctx)
	switch {
	case err == nil:
		return true
	case websocket.CloseStatus(err) == wsCloseUnauthorized:
		return false
	case errors.Is(err, context.DeadlineExceeded):
		return true
	default:
		t.Fatalf("неожиданная ошибка чтения после auth-кадра: %v", err)
		return false
	}
}

// TestGetWs_ValidAuthFrame_ConnectionAccepted — валидный access-токен в
// auth-кадре → соединение принято (не закрыто 4401).
func TestGetWs_ValidAuthFrame_ConnectionAccepted(t *testing.T) {
	s := newTestServer(fakeQuerier{})
	ts := httptest.NewServer(NewRouter(s))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn := dialClientWS(ctx, t, ts)
	defer func() { _ = conn.CloseNow() }()

	token := issueTestAccessToken(t, uuid.New(), time.Now())
	sendClientAuthFrame(ctx, t, conn, token)

	if !clientWSAccepted(t, conn) {
		t.Fatal("ожидалось, что соединение принято (валидный access_token), но оно закрыто")
	}
}

// TestGetWs_InvalidToken_Rejected — битый/просроченный access_token →
// отклонено close-кодом 4401.
func TestGetWs_InvalidToken_Rejected(t *testing.T) {
	cases := map[string]string{
		"мусорный токен": "not-a-jwt-at-all",
		"пустой токен":   "",
	}

	s := newTestServer(fakeQuerier{})
	ts := httptest.NewServer(NewRouter(s))
	defer ts.Close()

	for name, token := range cases {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			conn := dialClientWS(ctx, t, ts)
			defer func() { _ = conn.CloseNow() }()

			sendClientAuthFrame(ctx, t, conn, token)
			if clientWSAccepted(t, conn) {
				t.Fatalf("%s: ожидалось отклонение, но соединение принято", name)
			}
		})
	}

	t.Run("просроченный токен", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		conn := dialClientWS(ctx, t, ts)
		defer func() { _ = conn.CloseNow() }()

		expired := issueTestAccessToken(t, uuid.New(), time.Now().Add(-1*time.Hour))
		sendClientAuthFrame(ctx, t, conn, expired)
		if clientWSAccepted(t, conn) {
			t.Fatal("ожидалось отклонение (просроченный токен), но соединение принято")
		}
	})
}

// TestGetWs_WrongFrameType_Rejected — первый кадр — валидный JSON, но не
// {"type":"auth",...} (в т.ч. попытка прислать конверт машинного протокола
// bus.Envelope) → отклонено 4401.
func TestGetWs_WrongFrameType_Rejected(t *testing.T) {
	s := newTestServer(fakeQuerier{})
	ts := httptest.NewServer(NewRouter(s))
	defer ts.Close()

	token := issueTestAccessToken(t, uuid.New(), time.Now())

	cases := map[string][]byte{
		"type=hello (машинный протокол)": func() []byte {
			raw, _ := json.Marshal(struct {
				Type        string `json:"type"`
				AccessToken string `json:"access_token"`
			}{Type: "hello", AccessToken: token})
			return raw
		}(),
		"без поля type вовсе": func() []byte {
			raw, _ := json.Marshal(struct {
				AccessToken string `json:"access_token"`
			}{AccessToken: token})
			return raw
		}(),
	}

	for name, frame := range cases {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			conn := dialClientWS(ctx, t, ts)
			defer func() { _ = conn.CloseNow() }()

			if err := conn.Write(ctx, websocket.MessageText, frame); err != nil {
				t.Fatalf("write frame: %v", err)
			}
			if clientWSAccepted(t, conn) {
				t.Fatalf("%s: ожидалось отклонение, но соединение принято", name)
			}
		})
	}
}

// TestGetWs_MalformedFirstFrame_Rejected — первый кадр не JSON вовсе →
// отклонено 4401 (тот же единый сигнал отказа, без утечки причины).
func TestGetWs_MalformedFirstFrame_Rejected(t *testing.T) {
	s := newTestServer(fakeQuerier{})
	ts := httptest.NewServer(NewRouter(s))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn := dialClientWS(ctx, t, ts)
	defer func() { _ = conn.CloseNow() }()

	if err := conn.Write(ctx, websocket.MessageText, []byte("это не json")); err != nil {
		t.Fatalf("write malformed frame: %v", err)
	}
	if clientWSAccepted(t, conn) {
		t.Fatal("ожидалось отклонение (не-JSON первый кадр), но соединение принято")
	}
}

// readNotificationFrame читает один текстовый кадр и разбирает его как
// clientNotificationFrame — общий помощник для тестов доставки уведомления.
func readNotificationFrame(ctx context.Context, t *testing.T, conn *websocket.Conn) clientNotificationFrame {
	t.Helper()
	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("не получено уведомление: %v", err)
	}
	var frame clientNotificationFrame
	if err := json.Unmarshal(data, &frame); err != nil {
		t.Fatalf("уведомление не парсится как clientNotificationFrame: %v (%s)", err, data)
	}
	return frame
}

// TestClientConnHub_MultipleTabsSameUser_BothReceiveNotification —
// ключевое архитектурное отличие от /machine/ws (ADR 0002): ДВЕ одновременные
// вкладки (WS-соединения) ОДНОГО user_id ОБЕ остаются зарегистрированными
// (вторая НЕ вытесняет первую) и ОБЕ получают одно и то же уведомление
// (docs/adr/0005-client-ws-multi-connection-per-user.md).
func TestClientConnHub_MultipleTabsSameUser_BothReceiveNotification(t *testing.T) {
	s := newTestServer(fakeQuerier{})
	ts := httptest.NewServer(NewRouter(s))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	userID := uuid.New()
	token := issueTestAccessToken(t, userID, time.Now())

	tab1 := dialClientWS(ctx, t, ts)
	defer func() { _ = tab1.CloseNow() }()
	sendClientAuthFrame(ctx, t, tab1, token)

	tab2 := dialClientWS(ctx, t, ts)
	defer func() { _ = tab2.CloseNow() }()
	sendClientAuthFrame(ctx, t, tab2, token)

	// Дать серверу время зарегистрировать оба соединения (register()
	// выполняется в GetWs сразу после успешной authenticateClientWS, до
	// входа в read-loop).
	time.Sleep(200 * time.Millisecond)

	taskID := uuid.New()
	createdAt := time.Now().UTC()
	if err := s.ClientConnHub().Notify(context.Background(), notify.Notification{
		TaskID:    pgtype.UUID{Bytes: taskID, Valid: true},
		UserID:    pgtype.UUID{Bytes: userID, Valid: true},
		Kind:      notify.KindAgentQuestion,
		Payload:   []byte(`{}`),
		CreatedAt: createdAt,
	}); err != nil {
		t.Fatalf("Notify: %v", err)
	}

	for name, conn := range map[string]*websocket.Conn{"вкладка 1": tab1, "вкладка 2": tab2} {
		got := readNotificationFrame(ctx, t, conn)
		if got.Kind != notify.KindAgentQuestion {
			t.Fatalf("%s: kind = %q, ожидался %q", name, got.Kind, notify.KindAgentQuestion)
		}
		if got.TaskID != taskID.String() {
			t.Fatalf("%s: task_id = %q, ожидался %q", name, got.TaskID, taskID.String())
		}
	}
}

// TestClientConnHub_Notify_NoOpenConnections_ReturnsNil — Notify для
// пользователя без открытых WS-соединений — НЕ ошибка (best-effort доставка,
// docs/adr/0005, «Последствия»).
func TestClientConnHub_Notify_NoOpenConnections_ReturnsNil(t *testing.T) {
	s := newTestServer(fakeQuerier{})

	err := s.ClientConnHub().Notify(context.Background(), notify.Notification{
		TaskID:    pgtype.UUID{Bytes: uuid.New(), Valid: true},
		UserID:    pgtype.UUID{Bytes: uuid.New(), Valid: true},
		Kind:      notify.KindAgentQuestion,
		Payload:   []byte(`{}`),
		CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("Notify без открытых соединений вернул ошибку: %v", err)
	}
}

// TestFullChain_AgentQuestionDeliversToOpenWebClient — сквозной сценарий
// тикета 7.2 (Gherkin «Уведомление в web по WebSocket»): агент присылает
// agent_question по /machine/ws-протоколу (здесь — напрямую через
// handleMachineFrame, тот же приём, что и у остальных unit-тестов этого
// пакета, см. machine_ws_test.go) → handleAgentQuestion (тикет 7.1)
// формирует notify.Notification и передаёт зарегистрированному Notifier →
// РЕАЛЬНЫЙ *ClientConnHub (тот же экземпляр, что orchestrator/main.go
// регистрирует через server.SetNotifier(server.ClientConnHub())) доставляет
// его в УЖЕ ОТКРЫТОЕ WS-соединение /ws того же пользователя — без
// переподключения клиента, что и есть Go-эквивалент требования «e2e: вопрос
// → уведомление в UI без reload».
func TestFullChain_AgentQuestionDeliversToOpenWebClient(t *testing.T) {
	dbIntegrationID := uuid.New()
	taskID := uuid.New()
	userID := uuid.New()
	questionID := uuid.New().String()

	q := fakeQuerier{
		getTaskByIDAndIntegrationResult: db.Task{
			ID:            pgtype.UUID{Bytes: taskID, Valid: true},
			IntegrationID: pgtype.UUID{Bytes: dbIntegrationID, Valid: true},
			UserID:        pgtype.UUID{Bytes: userID, Valid: true},
		},
	}
	s := newTestServer(q)
	s.SetTransitioner(&fakeTransitioner{to: task.StatusWaitingUser})
	// Тот же wiring, что и orchestrator/main.go: реестр WS-соединений браузера
	// сам является Notifier'ом.
	s.SetNotifier(s.ClientConnHub())

	ts := httptest.NewServer(NewRouter(s))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// "Открытая вкладка" пользователя — подключается и аутентифицируется
	// ДО того, как агент задаст вопрос, и остаётся открытой на протяжении
	// всего сценария (то самое "без перезагрузки страницы").
	webConn := dialClientWS(ctx, t, ts)
	defer func() { _ = webConn.CloseNow() }()
	sendClientAuthFrame(ctx, t, webConn, issueTestAccessToken(t, userID, time.Now()))
	time.Sleep(200 * time.Millisecond) // дать GetWs время зарегистрировать соединение

	// "Агент" присылает вопрос по машинному протоколу (тикет 6.1/7.1) — тот
	// же вызов, что использует TestHandleMachineFrame_AgentQuestion_HappyPath_PublishesNotification
	// в machine_ws_test.go, но здесь Notifier настоящий (*ClientConnHub), а
	// не fakeNotifier.
	messageID := bus.NewMessageID()
	env := agentQuestionEnvelope(t, messageID, taskID.String(), questionID, "какую версию Go использовать?")
	// serverConn для handleMachineFrame не нужен для доставки ack в этом
	// тесте (он не проверяется здесь) — используем отдельную WS-пару, как и
	// другие тесты пакета.
	machineServerConn, machineClientConn, cleanupMachine := newWSPair(t)
	defer cleanupMachine()
	s.handleMachineFrame(ctx, machineServerConn, dbIntegrationID, marshalEnvelope(t, env))

	// Ack агенту (не предмет этого теста, но осушаем, чтобы не блокировать
	// запись на стороне сервера).
	go func() {
		_, _, _ = machineClientConn.Read(ctx)
	}()

	got := readNotificationFrame(ctx, t, webConn)
	if got.Kind != notify.KindAgentQuestion {
		t.Fatalf("уведомление: kind = %q, ожидался %q", got.Kind, notify.KindAgentQuestion)
	}
	if got.TaskID != taskID.String() {
		t.Fatalf("уведомление: task_id = %q, ожидался %q", got.TaskID, taskID.String())
	}
	if got.CreatedAt == "" {
		t.Fatal("уведомление: created_at пуст")
	}
}
