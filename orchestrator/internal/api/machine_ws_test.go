// Unit-тесты разбора пост-hello кадров WS машины (тикеты 3.4/3.6,
// protocol.md §5/§6): parseAckFrame и handleMachineFrame — без сети/БД/Redpanda
// (в отличие от machine_ws_integration_test.go, который гоняет полный
// handshake через httptest.NewServer + реальный WS-клиент).
//
// Покрывает приёмочное требование тикета 3.4 «прочие типы кадров безопасно
// игнорируются, ack — пересылается в AckSink»:
//   - валидный ack-конверт → (ack_message_id, true);
//   - конверт другого известного типа (hello/task_assigned) → ("", false);
//   - кадр не JSON / не конверт → ("", false);
//   - конверт type==ack, но payload без ack_message_id/с пустым значением →
//     ("", false);
//   - handleMachineFrame пересылает ack в зарегистрированный AckSink ровно с
//     тем ack_message_id, что был в payload;
//   - handleMachineFrame на НЕ-ack кадре не трогает AckSink вовсе;
//   - handleMachineFrame не паникует, если AckSink не зарегистрирован (nil).
//
// А также приёмочное требование тикета 3.6 (FR B4, protocol.md §6):
//   - heartbeat-кадр публикуется зарегистрированным EventSink с
//     IntegrationID, ПЕРЕЗАПИСАННЫМ на аутентифицированный DB id соединения
//     (а не на значение, присланное агентом в конверте);
//   - успешная публикация → агенту приходит ack с ack_message_id исходного
//     heartbeat-конверта;
//   - ошибка EventSink.HandleEvent → ack агенту НЕ отправляется;
//   - EventSink не зарегистрирован (nil) → кадр молча игнорируется, без паники.
//
// А также приёмочное требование тикета 4.7 (FR C5, ADR 0003 "machine-ws
// protocol version compat") — authenticateMachineHello:
//   - hello с несовместимым protocol_version → соединение закрывается кодом
//     wsCloseIncompatibleProtocolVersion (4426) с содержательной причиной
//     (got/want), authenticateMachineHello возвращает (uuid.Nil, false);
//   - hello с совместимым protocol_version и валидным UUID/HMAC продолжает
//     работать как раньше (регрессия — без завязки на ADR 0003).
//
// А также приёмочное требование тикета 5.4 (FR E1, «Доставка на машину»,
// handleTaskAccepted):
//   - task_accepted для задачи в queued → Transition(queued→running) вызван
//     с правильными taskID/trigger, агенту приходит ack;
//   - задача не найдена/принадлежит другой интеграции, transitioner не
//     настроен, Transition вернул ошибку, конверт без task_id → ack НЕ
//     отправляется, паники нет.
//
// А также приёмочное требование тикета 5.8 (FR E1, «Ошибки агента/машины»,
// handleAgentError):
//   - error для задачи в running → TransitionWithEvent(running→failed)
//     вызван с правильными taskID/trigger/eventType/eventPayload, агенту
//     приходит ack;
//   - задача не найдена/принадлежит другой интеграции, transitioner не
//     настроен, TransitionWithEvent вернул ошибку, конверт без task_id,
//     payload с пустым message → ack НЕ отправляется, паники нет.
//
// А также приёмочное требование тикета 6.4 (FR F3, Gherkin §5 «Команда вне
// allowlist требует согласования», handleCommandApprovalRequest):
//   - command_approval_request для задачи в running →
//     TransitionWithEvent(running→waiting_user) вызван с правильными
//     taskID/trigger/eventType, агенту приходит ack;
//   - задача не найдена/принадлежит другой интеграции, transitioner не
//     настроен, TransitionWithEvent вернул ошибку, конверт без task_id,
//     payload без request_id → ack НЕ отправляется, паники нет.
package api

import (
	"bytes"
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
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/yarabey/agentify/internal/bus"
	"github.com/yarabey/agentify/orchestrator/internal/db"
	"github.com/yarabey/agentify/orchestrator/internal/notify"
	"github.com/yarabey/agentify/orchestrator/internal/task"
)

// fakeNotifier — подменный Notifier для unit-тестов handleAgentQuestion
// (тикет 7.1): фиксирует все вызовы Notify для проверки в тестах.
type fakeNotifier struct {
	calls []notify.Notification
	err   error
}

func (f *fakeNotifier) Notify(_ context.Context, n notify.Notification) error {
	f.calls = append(f.calls, n)
	return f.err
}

// marshalEnvelope собирает и маршалит конверт для тестов parseAckFrame/
// handleMachineFrame — обёртка над bus.Envelope.Marshal с t.Fatalf на ошибку.
func marshalEnvelope(t *testing.T, env bus.Envelope) []byte {
	t.Helper()
	data, err := env.Marshal()
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	return data
}

// ackEnvelope собирает валидный конверт type==ack с заданным ack_message_id
// (protocol.md §4/§5, bus.AckPayload).
func ackEnvelope(t *testing.T, ackMessageID string) bus.Envelope {
	t.Helper()
	payload, err := json.Marshal(bus.AckPayload{AckMessageID: ackMessageID})
	if err != nil {
		t.Fatalf("marshal AckPayload: %v", err)
	}
	return bus.Envelope{
		MessageID:       bus.NewMessageID(),
		IntegrationID:   "11111111-1111-1111-1111-111111111111",
		Type:            bus.MessageTypeAck,
		Ts:              "2026-06-30T00:00:00Z",
		ProtocolVersion: bus.ProtocolVersion,
		Payload:         payload,
	}
}

// TestParseAckFrame_ValidAck — валидный ack-конверт → (ack_message_id, true).
func TestParseAckFrame_ValidAck(t *testing.T) {
	wantID := bus.NewMessageID()
	data := marshalEnvelope(t, ackEnvelope(t, wantID))

	gotID, ok := parseAckFrame(data)
	if !ok {
		t.Fatal("parseAckFrame: ok=false для валидного ack-конверта")
	}
	if gotID != wantID {
		t.Fatalf("parseAckFrame: ack_message_id = %q, ожидался %q", gotID, wantID)
	}
}

// TestParseAckFrame_RejectsNonAckAndMalformed — все случаи, когда parseAckFrame
// должен вернуть ("", false): неверный тип, не-JSON, payload без/с пустым
// ack_message_id (приёмка тикета 3.4 — «прочие типы безопасно игнорируются»).
func TestParseAckFrame_RejectsNonAckAndMalformed(t *testing.T) {
	helloPayload, err := json.Marshal(bus.HelloPayload{UUID: "11111111-1111-1111-1111-111111111111", AgentVersion: "1.0.0"})
	if err != nil {
		t.Fatalf("marshal HelloPayload: %v", err)
	}
	helloEnv := bus.Envelope{
		MessageID:       bus.NewMessageID(),
		IntegrationID:   "11111111-1111-1111-1111-111111111111",
		Type:            bus.MessageTypeHello,
		Ts:              "2026-06-30T00:00:00Z",
		ProtocolVersion: bus.ProtocolVersion,
		Payload:         helloPayload,
	}

	emptyAckIDEnv := ackEnvelope(t, "")

	ackTypeBadPayloadEnv := bus.Envelope{
		MessageID:       bus.NewMessageID(),
		IntegrationID:   "11111111-1111-1111-1111-111111111111",
		Type:            bus.MessageTypeAck,
		Ts:              "2026-06-30T00:00:00Z",
		ProtocolVersion: bus.ProtocolVersion,
		Payload:         json.RawMessage(`["not", "an", "object"]`),
	}

	cases := map[string][]byte{
		"кадр не JSON вовсе":                  []byte("это не json"),
		"JSON, но не конверт (нет полей)":     []byte(`{"foo":"bar"}`),
		"конверт другого типа (hello)":        marshalEnvelope(t, helloEnv),
		"конверт ack, но ack_message_id пуст": marshalEnvelope(t, emptyAckIDEnv),
		"конверт ack, payload не объект":      marshalEnvelope(t, ackTypeBadPayloadEnv),
	}

	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			gotID, ok := parseAckFrame(data)
			if ok {
				t.Fatalf("%s: ok=true (ack_message_id=%q), ожидалось false", name, gotID)
			}
			if gotID != "" {
				t.Fatalf("%s: ack_message_id = %q, ожидалась пустая строка при ok=false", name, gotID)
			}
		})
	}
}

// fakeAckSink — фейковая реализация AckSink для unit-тестов handleMachineFrame:
// запоминает все полученные ack_message_id.
type fakeAckSink struct {
	received []string
}

func (f *fakeAckSink) HandleAck(ackMessageID string) {
	f.received = append(f.received, ackMessageID)
}

// TestHandleMachineFrame_AckDispatchedToSink — ack-кадр пересылается
// зарегистрированному AckSink ровно с тем ack_message_id, что был в payload.
func TestHandleMachineFrame_AckDispatchedToSink(t *testing.T) {
	s := newTestServer(fakeQuerier{})
	sink := &fakeAckSink{}
	s.SetAckSink(sink)

	wantID := bus.NewMessageID()
	s.handleMachineFrame(context.Background(), nil, uuid.New(), marshalEnvelope(t, ackEnvelope(t, wantID)))

	if len(sink.received) != 1 {
		t.Fatalf("AckSink.HandleAck вызван %d раз(а), ожидался 1: %v", len(sink.received), sink.received)
	}
	if sink.received[0] != wantID {
		t.Fatalf("AckSink.HandleAck получил %q, ожидался %q", sink.received[0], wantID)
	}
}

// TestHandleMachineFrame_NonAckFrameIgnoredBySink — кадр НЕ типа ack (в т.ч.
// битый/нераспознанный) не должен вызывать AckSink вовсе — приёмка тикета 3.4
// «прочие типы кадров безопасно игнорируются».
func TestHandleMachineFrame_NonAckFrameIgnoredBySink(t *testing.T) {
	s := newTestServer(fakeQuerier{})
	sink := &fakeAckSink{}
	s.SetAckSink(sink)

	frames := [][]byte{
		[]byte("мусор, не json"),
		[]byte(`{"type":"task_accepted"}`),
	}
	for _, data := range frames {
		s.handleMachineFrame(context.Background(), nil, uuid.New(), data)
	}

	if len(sink.received) != 0 {
		t.Fatalf("AckSink.HandleAck вызван для не-ack кадра(ов): %v", sink.received)
	}
}

// TestHandleMachineFrame_NoSinkRegistered_DoesNotPanic — AckSink не
// зарегистрирован (nil, как до старта моста тикета 3.4 / в тестах без него) →
// handleMachineFrame на ack-кадре не паникует, просто игнорирует.
func TestHandleMachineFrame_NoSinkRegistered_DoesNotPanic(t *testing.T) {
	s := newTestServer(fakeQuerier{})
	s.handleMachineFrame(context.Background(), nil, uuid.New(), marshalEnvelope(t, ackEnvelope(t, bus.NewMessageID())))
}

// heartbeatEnvelope собирает валидный конверт type==heartbeat (protocol.md
// §4/§6) с заданным integration_id (эмулирует то, что реально присылает
// агент, — секрет интеграции, НЕ DB id).
func heartbeatEnvelope(t *testing.T, messageID, integrationID string) bus.Envelope {
	t.Helper()
	return bus.Envelope{
		MessageID:       messageID,
		IntegrationID:   integrationID,
		Type:            bus.MessageTypeHeartbeat,
		Ts:              time.Now().UTC().Format(time.RFC3339),
		ProtocolVersion: bus.ProtocolVersion,
		Payload:         json.RawMessage(`{}`),
	}
}

// fakeEventSink — фейковая реализация EventSink для unit-тестов
// handleMachineFrame/handleMachineEvent (тикет 3.6): запоминает все
// полученные конверты; err (если задан) возвращается из HandleEvent для
// проверки ветки «публикация не удалась → ack не отправляется».
type fakeEventSink struct {
	received []bus.Envelope
	err      error
}

func (f *fakeEventSink) HandleEvent(_ context.Context, env bus.Envelope) error {
	f.received = append(f.received, env)
	return f.err
}

// newWSPair поднимает настоящую пару WS-соединений (сервер/клиент) через
// httptest.NewServer — без Redpanda/Docker, тот же приём, что и
// orchestrator/internal/bridge.newWSPair (неэкспортированный помощник
// другого пакета напрямую не переиспользовать — здесь отдельная копия).
// serverConn — сторона, которую handleMachineEvent использует для записи
// ack-кадра; clientConn — сторона теста, играющая роль агента (читает ack).
func newWSPair(t *testing.T) (serverConn, clientConn *websocket.Conn, cleanup func()) {
	t.Helper()
	accepted := make(chan *websocket.Conn, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		accepted <- c
	}))

	wsURL := strings.Replace(ts.URL, "http://", "ws://", 1)
	clientConn, _, err := websocket.Dial(context.Background(), wsURL, nil)
	if err != nil {
		ts.Close()
		t.Fatalf("websocket.Dial: %v", err)
	}

	select {
	case serverConn = <-accepted:
	case <-time.After(5 * time.Second):
		ts.Close()
		t.Fatal("сервер не принял WS-соединение за отведённое время")
	}

	cleanup = func() {
		_ = clientConn.CloseNow()
		_ = serverConn.CloseNow()
		ts.Close()
	}
	return serverConn, clientConn, cleanup
}

// TestHandleMachineFrame_HeartbeatRewritesIntegrationIDAndAcks — heartbeat-кадр
// публикуется зарегистрированным EventSink с IntegrationID, ПЕРЕЗАПИСАННЫМ на
// аутентифицированный DB id соединения (а не на значение из кадра, которое
// эмулирует секрет интеграции), и агенту приходит ack с ack_message_id
// исходного heartbeat-конверта (тикет 3.6, FR B4, protocol.md §6).
func TestHandleMachineFrame_HeartbeatRewritesIntegrationIDAndAcks(t *testing.T) {
	s := newTestServer(fakeQuerier{})
	sink := &fakeEventSink{}
	s.SetEventSink(sink)

	serverConn, clientConn, cleanup := newWSPair(t)
	defer cleanup()

	dbID := uuid.New()
	agentSentSecret := uuid.New().String()
	messageID := bus.NewMessageID()

	s.handleMachineFrame(context.Background(), serverConn, dbID,
		marshalEnvelope(t, heartbeatEnvelope(t, messageID, agentSentSecret)))

	if len(sink.received) != 1 {
		t.Fatalf("EventSink.HandleEvent вызван %d раз(а), ожидался 1", len(sink.received))
	}
	got := sink.received[0]
	if got.IntegrationID != dbID.String() {
		t.Fatalf("EventSink получил IntegrationID=%q, ожидался DB id %q (НЕ секрет агента %q)",
			got.IntegrationID, dbID.String(), agentSentSecret)
	}
	if got.Type != bus.MessageTypeHeartbeat {
		t.Fatalf("EventSink получил type=%q, ожидался %q", got.Type, bus.MessageTypeHeartbeat)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, data, err := clientConn.Read(ctx)
	if err != nil {
		t.Fatalf("клиент не получил ack-кадр: %v", err)
	}
	var ackEnv bus.Envelope
	if err := json.Unmarshal(data, &ackEnv); err != nil {
		t.Fatalf("ack-кадр не парсится: %v", err)
	}
	if ackEnv.Type != bus.MessageTypeAck {
		t.Fatalf("ack-кадр type=%q, ожидался %q", ackEnv.Type, bus.MessageTypeAck)
	}
	var ackPayload bus.AckPayload
	if err := json.Unmarshal(ackEnv.Payload, &ackPayload); err != nil {
		t.Fatalf("ack-payload не парсится: %v", err)
	}
	if ackPayload.AckMessageID != messageID {
		t.Fatalf("ack_message_id=%q, ожидался %q", ackPayload.AckMessageID, messageID)
	}
}

// TestHandleMachineFrame_HeartbeatSinkError_NoAckSent — EventSink.HandleEvent
// возвращает ошибку (публикация не удалась) → агенту НЕ должен прийти ack:
// он повторит heartbeat через свой durable outbox (тикет 3.5, protocol.md §5).
func TestHandleMachineFrame_HeartbeatSinkError_NoAckSent(t *testing.T) {
	s := newTestServer(fakeQuerier{})
	sink := &fakeEventSink{err: errors.New("publish failed")}
	s.SetEventSink(sink)

	serverConn, clientConn, cleanup := newWSPair(t)
	defer cleanup()

	s.handleMachineFrame(context.Background(), serverConn, uuid.New(),
		marshalEnvelope(t, heartbeatEnvelope(t, bus.NewMessageID(), uuid.New().String())))

	if len(sink.received) != 1 {
		t.Fatalf("EventSink.HandleEvent вызван %d раз(а), ожидался 1", len(sink.received))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, _, err := clientConn.Read(ctx)
	if err == nil {
		t.Fatal("клиент получил кадр, хотя EventSink вернул ошибку — ack не должен отправляться")
	}
}

// TestHandleMachineFrame_HeartbeatNoSinkRegistered_DoesNotPanic — EventSink не
// зарегистрирован (nil) → heartbeat-кадр молча игнорируется, соединение не
// падает (тот же принцип, что и у AckSink).
func TestHandleMachineFrame_HeartbeatNoSinkRegistered_DoesNotPanic(t *testing.T) {
	s := newTestServer(fakeQuerier{})

	serverConn, clientConn, cleanup := newWSPair(t)
	defer cleanup()

	s.handleMachineFrame(context.Background(), serverConn, uuid.New(),
		marshalEnvelope(t, heartbeatEnvelope(t, bus.NewMessageID(), uuid.New().String())))

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, _, err := clientConn.Read(ctx)
	if err == nil {
		t.Fatal("клиент получил кадр, хотя EventSink не зарегистрирован")
	}
}

// agentQuestionEnvelope собирает валидный конверт type==agent_question
// (тикет 6.1, FR F1, protocol.md §4) для заданной задачи с
// bus.AgentQuestionPayload{QuestionID, Text}.
func agentQuestionEnvelope(t *testing.T, messageID, taskID, questionID, text string) bus.Envelope {
	t.Helper()
	payload, err := json.Marshal(bus.AgentQuestionPayload{QuestionID: questionID, Text: text})
	if err != nil {
		t.Fatalf("marshal AgentQuestionPayload: %v", err)
	}
	return bus.Envelope{
		MessageID:       messageID,
		TaskID:          &taskID,
		IntegrationID:   uuid.New().String(), // перезаписывается на DB id в handleAgentQuestion не требуется — используется параметр integrationID
		Type:            bus.MessageTypeAgentQuestion,
		Ts:              time.Now().UTC().Format(time.RFC3339),
		ProtocolVersion: bus.ProtocolVersion,
		Payload:         payload,
	}
}

// TestHandleMachineFrame_AgentQuestion_HappyPath — agent_question успешно
// переводит задачу running→waiting_user (fakeTransitioner) и агенту приходит
// ack с ack_message_id исходного конверта (тикет 6.1, FR F1).
func TestHandleMachineFrame_AgentQuestion_HappyPath(t *testing.T) {
	dbIntegrationID := uuid.New()
	taskID := uuid.New()
	questionID := uuid.New().String()

	q := fakeQuerier{
		getTaskByIDAndIntegrationResult: db.Task{
			ID:            pgtype.UUID{Bytes: taskID, Valid: true},
			IntegrationID: pgtype.UUID{Bytes: dbIntegrationID, Valid: true},
		},
	}
	s := newTestServer(q)
	transitioner := &fakeTransitioner{to: task.StatusWaitingUser}
	s.SetTransitioner(transitioner)

	serverConn, clientConn, cleanup := newWSPair(t)
	defer cleanup()

	messageID := bus.NewMessageID()
	env := agentQuestionEnvelope(t, messageID, taskID.String(), questionID, "какую версию Go использовать?")
	s.handleMachineFrame(context.Background(), serverConn, dbIntegrationID, marshalEnvelope(t, env))

	if transitioner.lastTaskID.Bytes != taskID {
		t.Fatalf("TransitionWithEvent вызван с taskID = %s, ожидался %s", uuid.UUID(transitioner.lastTaskID.Bytes), taskID)
	}
	if transitioner.lastTrigger != task.TriggerAgentQuestion {
		t.Fatalf("TransitionWithEvent вызван с trigger = %q, ожидался %q", transitioner.lastTrigger, task.TriggerAgentQuestion)
	}
	if transitioner.lastEventType != "agent_question" {
		t.Fatalf("TransitionWithEvent вызван с eventType = %q, ожидался %q", transitioner.lastEventType, "agent_question")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, data, err := clientConn.Read(ctx)
	if err != nil {
		t.Fatalf("клиент не получил ack-кадр: %v", err)
	}
	var ackEnv bus.Envelope
	if err := json.Unmarshal(data, &ackEnv); err != nil {
		t.Fatalf("ack-кадр не парсится: %v", err)
	}
	if ackEnv.Type != bus.MessageTypeAck {
		t.Fatalf("ack-кадр type=%q, ожидался %q", ackEnv.Type, bus.MessageTypeAck)
	}
	var ackPayload bus.AckPayload
	if err := json.Unmarshal(ackEnv.Payload, &ackPayload); err != nil {
		t.Fatalf("ack-payload не парсится: %v", err)
	}
	if ackPayload.AckMessageID != messageID {
		t.Fatalf("ack_message_id=%q, ожидался %q", ackPayload.AckMessageID, messageID)
	}
}

// TestHandleMachineFrame_AgentQuestion_HappyPath_PublishesNotification —
// успешный переход running→waiting_user, если Notifier зарегистрирован,
// публикует ровно одно доменное уведомление notify.KindAgentQuestion с
// правильными TaskID/UserID/Payload/CreatedAt (тикет 7.1, FR G1).
func TestHandleMachineFrame_AgentQuestion_HappyPath_PublishesNotification(t *testing.T) {
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
	notifier := &fakeNotifier{}
	s.SetNotifier(notifier)

	serverConn, clientConn, cleanup := newWSPair(t)
	defer cleanup()

	messageID := bus.NewMessageID()
	env := agentQuestionEnvelope(t, messageID, taskID.String(), questionID, "какую версию Go использовать?")
	s.handleMachineFrame(context.Background(), serverConn, dbIntegrationID, marshalEnvelope(t, env))

	if len(notifier.calls) != 1 {
		t.Fatalf("Notify вызван %d раз(а), ожидался 1", len(notifier.calls))
	}
	got := notifier.calls[0]
	if got.TaskID.Bytes != taskID {
		t.Fatalf("Notify получил TaskID = %s, ожидался %s", uuid.UUID(got.TaskID.Bytes), taskID)
	}
	if got.UserID.Bytes != userID {
		t.Fatalf("Notify получил UserID = %s, ожидался %s", uuid.UUID(got.UserID.Bytes), userID)
	}
	if got.Kind != notify.KindAgentQuestion {
		t.Fatalf("Notify получил Kind = %q, ожидался %q", got.Kind, notify.KindAgentQuestion)
	}
	if len(got.Payload) == 0 {
		t.Fatal("Notify получил пустой Payload")
	}
	if got.CreatedAt.IsZero() {
		t.Fatal("Notify получил нулевой CreatedAt")
	}

	// Ack агенту всё равно должен прийти — уведомление не блокирует основной поток.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, _, err := clientConn.Read(ctx); err != nil {
		t.Fatalf("клиент не получил ack-кадр: %v", err)
	}
}

// TestHandleMachineFrame_AgentQuestion_NoNotifierConfigured_StillAcks —
// Notifier не зарегистрирован (nil, по умолчанию) → handleAgentQuestion
// молча не формирует уведомление, но ack агенту приходит как обычно (тикет
// 7.1: отсутствие notify-подсистемы не должно ломать основной поток
// вопрос/ответ).
func TestHandleMachineFrame_AgentQuestion_NoNotifierConfigured_StillAcks(t *testing.T) {
	dbIntegrationID := uuid.New()
	taskID := uuid.New()

	q := fakeQuerier{
		getTaskByIDAndIntegrationResult: db.Task{
			ID:            pgtype.UUID{Bytes: taskID, Valid: true},
			IntegrationID: pgtype.UUID{Bytes: dbIntegrationID, Valid: true},
		},
	}
	s := newTestServer(q)
	s.SetTransitioner(&fakeTransitioner{to: task.StatusWaitingUser})
	// notifier намеренно не зарегистрирован.

	serverConn, clientConn, cleanup := newWSPair(t)
	defer cleanup()

	messageID := bus.NewMessageID()
	env := agentQuestionEnvelope(t, messageID, taskID.String(), uuid.New().String(), "вопрос")
	s.handleMachineFrame(context.Background(), serverConn, dbIntegrationID, marshalEnvelope(t, env))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, data, err := clientConn.Read(ctx)
	if err != nil {
		t.Fatalf("клиент не получил ack-кадр: %v", err)
	}
	var ackEnv bus.Envelope
	if err := json.Unmarshal(data, &ackEnv); err != nil {
		t.Fatalf("ack-кадр не парсится: %v", err)
	}
	var ackPayload bus.AckPayload
	if err := json.Unmarshal(ackEnv.Payload, &ackPayload); err != nil {
		t.Fatalf("ack-payload не парсится: %v", err)
	}
	if ackPayload.AckMessageID != messageID {
		t.Fatalf("ack_message_id=%q, ожидался %q", ackPayload.AckMessageID, messageID)
	}
}

// TestHandleMachineFrame_AgentQuestion_NotifierError_StillAcks — Notify
// вернул ошибку (публикация уведомления не удалась) → ошибка только
// логируется, ack агенту ВСЁ РАВНО приходит (тикет 7.1: уведомление вторично
// относительно перехода FSM и не должно блокировать/дублировать основной
// поток вопрос/ответ).
func TestHandleMachineFrame_AgentQuestion_NotifierError_StillAcks(t *testing.T) {
	dbIntegrationID := uuid.New()
	taskID := uuid.New()

	q := fakeQuerier{
		getTaskByIDAndIntegrationResult: db.Task{
			ID:            pgtype.UUID{Bytes: taskID, Valid: true},
			IntegrationID: pgtype.UUID{Bytes: dbIntegrationID, Valid: true},
		},
	}
	s := newTestServer(q)
	s.SetTransitioner(&fakeTransitioner{to: task.StatusWaitingUser})
	s.SetNotifier(&fakeNotifier{err: errors.New("канал недоступен")})

	serverConn, clientConn, cleanup := newWSPair(t)
	defer cleanup()

	messageID := bus.NewMessageID()
	env := agentQuestionEnvelope(t, messageID, taskID.String(), uuid.New().String(), "вопрос")
	s.handleMachineFrame(context.Background(), serverConn, dbIntegrationID, marshalEnvelope(t, env))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, data, err := clientConn.Read(ctx)
	if err != nil {
		t.Fatalf("клиент не получил ack-кадр: %v", err)
	}
	var ackEnv bus.Envelope
	if err := json.Unmarshal(data, &ackEnv); err != nil {
		t.Fatalf("ack-кадр не парсится: %v", err)
	}
	var ackPayload bus.AckPayload
	if err := json.Unmarshal(ackEnv.Payload, &ackPayload); err != nil {
		t.Fatalf("ack-payload не парсится: %v", err)
	}
	if ackPayload.AckMessageID != messageID {
		t.Fatalf("ack_message_id=%q, ожидался %q", ackPayload.AckMessageID, messageID)
	}
}

// TestHandleMachineFrame_AgentQuestion_TaskNotFoundOrWrongIntegration —
// задача не найдена для этой интеграции (GetTaskByIDAndIntegration →
// pgx.ErrNoRows, в т.ч. подделанный task_id чужой задачи) → ack НЕ
// отправляется, соединение не падает (тикет 6.1, FR A4/I3 — та же логика,
// что и owner-scoped проверки в HTTP-обработчиках).
func TestHandleMachineFrame_AgentQuestion_TaskNotFoundOrWrongIntegration(t *testing.T) {
	q := fakeQuerier{getTaskByIDAndIntegrationErr: pgx.ErrNoRows}
	s := newTestServer(q)
	s.SetTransitioner(&fakeTransitioner{to: task.StatusWaitingUser})

	serverConn, clientConn, cleanup := newWSPair(t)
	defer cleanup()

	env := agentQuestionEnvelope(t, bus.NewMessageID(), uuid.New().String(), uuid.New().String(), "вопрос")
	s.handleMachineFrame(context.Background(), serverConn, uuid.New(), marshalEnvelope(t, env))

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, _, err := clientConn.Read(ctx)
	if err == nil {
		t.Fatal("клиент получил ack, хотя задача не найдена для этой интеграции")
	}
}

// TestHandleMachineFrame_AgentQuestion_NoTransitionerConfigured —
// transitioner не зарегистрирован (nil) → ack НЕ отправляется, паники нет.
func TestHandleMachineFrame_AgentQuestion_NoTransitionerConfigured(t *testing.T) {
	dbIntegrationID := uuid.New()
	taskID := uuid.New()
	q := fakeQuerier{
		getTaskByIDAndIntegrationResult: db.Task{ID: pgtype.UUID{Bytes: taskID, Valid: true}},
	}
	s := newTestServer(q)
	// transitioner намеренно не зарегистрирован.

	serverConn, clientConn, cleanup := newWSPair(t)
	defer cleanup()

	env := agentQuestionEnvelope(t, bus.NewMessageID(), taskID.String(), uuid.New().String(), "вопрос")
	s.handleMachineFrame(context.Background(), serverConn, dbIntegrationID, marshalEnvelope(t, env))

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, _, err := clientConn.Read(ctx)
	if err == nil {
		t.Fatal("клиент получил ack, хотя transitioner не настроен")
	}
}

// TestHandleMachineFrame_AgentQuestion_TransitionError — TransitionWithEvent
// вернул ошибку (недопустимый переход FSM/сбой БД) → ack НЕ отправляется.
func TestHandleMachineFrame_AgentQuestion_TransitionError(t *testing.T) {
	dbIntegrationID := uuid.New()
	taskID := uuid.New()
	q := fakeQuerier{
		getTaskByIDAndIntegrationResult: db.Task{ID: pgtype.UUID{Bytes: taskID, Valid: true}},
	}
	s := newTestServer(q)
	s.SetTransitioner(&fakeTransitioner{err: errors.New("недопустимый переход")})

	serverConn, clientConn, cleanup := newWSPair(t)
	defer cleanup()

	env := agentQuestionEnvelope(t, bus.NewMessageID(), taskID.String(), uuid.New().String(), "вопрос")
	s.handleMachineFrame(context.Background(), serverConn, dbIntegrationID, marshalEnvelope(t, env))

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, _, err := clientConn.Read(ctx)
	if err == nil {
		t.Fatal("клиент получил ack, хотя TransitionWithEvent вернул ошибку")
	}
}

// TestHandleMachineFrame_AgentQuestion_InvalidPayload_DoesNotPanic — payload
// без question_id (или отсутствующий task_id) → ack НЕ отправляется,
// handleMachineFrame не паникует и не закрывает соединение (тот же принцип
// «безопасно игнорировать», что и у ack/heartbeat).
func TestHandleMachineFrame_AgentQuestion_InvalidPayload_DoesNotPanic(t *testing.T) {
	taskID := uuid.New().String()
	envNoQuestionID := bus.Envelope{
		MessageID:       bus.NewMessageID(),
		TaskID:          &taskID,
		IntegrationID:   uuid.New().String(),
		Type:            bus.MessageTypeAgentQuestion,
		Ts:              time.Now().UTC().Format(time.RFC3339),
		ProtocolVersion: bus.ProtocolVersion,
		Payload:         json.RawMessage(`{"text":"вопрос без question_id"}`),
	}
	envNoTaskID := bus.Envelope{
		MessageID:       bus.NewMessageID(),
		IntegrationID:   uuid.New().String(),
		Type:            bus.MessageTypeAgentQuestion,
		Ts:              time.Now().UTC().Format(time.RFC3339),
		ProtocolVersion: bus.ProtocolVersion,
		Payload:         json.RawMessage(`{"question_id":"q1","text":"вопрос без task_id"}`),
	}

	q := fakeQuerier{
		getTaskByIDAndIntegrationResult: db.Task{},
	}
	s := newTestServer(q)
	s.SetTransitioner(&fakeTransitioner{to: task.StatusWaitingUser})

	serverConn, clientConn, cleanup := newWSPair(t)
	defer cleanup()

	for name, env := range map[string]bus.Envelope{
		"без question_id": envNoQuestionID,
		"без task_id":     envNoTaskID,
	} {
		t.Run(name, func(t *testing.T) {
			s.handleMachineFrame(context.Background(), serverConn, uuid.New(), marshalEnvelope(t, env))

			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			_, _, err := clientConn.Read(ctx)
			if err == nil {
				t.Fatalf("%s: клиент получил ack, хотя payload/конверт невалиден", name)
			}
		})
	}
}

// commandApprovalRequestEnvelope собирает валидный конверт
// type==command_approval_request (тикет 6.4, FR F3, Gherkin §5) для заданной
// задачи с bus.CommandApprovalRequestPayload{RequestID, Command, Reason}.
func commandApprovalRequestEnvelope(t *testing.T, messageID, taskID, requestID, command, reason string) bus.Envelope {
	t.Helper()
	payload, err := json.Marshal(bus.CommandApprovalRequestPayload{RequestID: requestID, Command: command, Reason: reason})
	if err != nil {
		t.Fatalf("marshal CommandApprovalRequestPayload: %v", err)
	}
	return bus.Envelope{
		MessageID:       messageID,
		TaskID:          &taskID,
		IntegrationID:   uuid.New().String(), // перезаписывается на DB id в handleCommandApprovalRequest не требуется — используется параметр integrationID
		Type:            bus.MessageTypeCommandApprovalRequest,
		Ts:              time.Now().UTC().Format(time.RFC3339),
		ProtocolVersion: bus.ProtocolVersion,
		Payload:         payload,
	}
}

// TestHandleMachineFrame_CommandApprovalRequest_HappyPath —
// command_approval_request успешно переводит задачу running→waiting_user
// (fakeTransitioner) и агенту приходит ack с ack_message_id исходного
// конверта (тикет 6.4, FR F3, Gherkin §5).
func TestHandleMachineFrame_CommandApprovalRequest_HappyPath(t *testing.T) {
	dbIntegrationID := uuid.New()
	taskID := uuid.New()
	requestID := uuid.New().String()

	q := fakeQuerier{
		getTaskByIDAndIntegrationResult: db.Task{
			ID:            pgtype.UUID{Bytes: taskID, Valid: true},
			IntegrationID: pgtype.UUID{Bytes: dbIntegrationID, Valid: true},
		},
	}
	s := newTestServer(q)
	transitioner := &fakeTransitioner{to: task.StatusWaitingUser}
	s.SetTransitioner(transitioner)

	serverConn, clientConn, cleanup := newWSPair(t)
	defer cleanup()

	messageID := bus.NewMessageID()
	env := commandApprovalRequestEnvelope(t, messageID, taskID.String(), requestID, "rm -rf /tmp/build", "нужно очистить директорию сборки")
	s.handleMachineFrame(context.Background(), serverConn, dbIntegrationID, marshalEnvelope(t, env))

	if transitioner.lastTaskID.Bytes != taskID {
		t.Fatalf("TransitionWithEvent вызван с taskID = %s, ожидался %s", uuid.UUID(transitioner.lastTaskID.Bytes), taskID)
	}
	if transitioner.lastTrigger != task.TriggerApprovalRequested {
		t.Fatalf("TransitionWithEvent вызван с trigger = %q, ожидался %q", transitioner.lastTrigger, task.TriggerApprovalRequested)
	}
	if transitioner.lastEventType != "command_approval_request" {
		t.Fatalf("TransitionWithEvent вызван с eventType = %q, ожидался %q", transitioner.lastEventType, "command_approval_request")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, data, err := clientConn.Read(ctx)
	if err != nil {
		t.Fatalf("клиент не получил ack-кадр: %v", err)
	}
	var ackEnv bus.Envelope
	if err := json.Unmarshal(data, &ackEnv); err != nil {
		t.Fatalf("ack-кадр не парсится: %v", err)
	}
	if ackEnv.Type != bus.MessageTypeAck {
		t.Fatalf("ack-кадр type=%q, ожидался %q", ackEnv.Type, bus.MessageTypeAck)
	}
	var ackPayload bus.AckPayload
	if err := json.Unmarshal(ackEnv.Payload, &ackPayload); err != nil {
		t.Fatalf("ack-payload не парсится: %v", err)
	}
	if ackPayload.AckMessageID != messageID {
		t.Fatalf("ack_message_id=%q, ожидался %q", ackPayload.AckMessageID, messageID)
	}
}

// TestHandleMachineFrame_CommandApprovalRequest_TaskNotFoundOrWrongIntegration
// — задача не найдена для этой интеграции (GetTaskByIDAndIntegration →
// pgx.ErrNoRows, в т.ч. подделанный task_id чужой задачи) → ack НЕ
// отправляется, соединение не падает (тикет 6.4, FR A4/I3).
func TestHandleMachineFrame_CommandApprovalRequest_TaskNotFoundOrWrongIntegration(t *testing.T) {
	q := fakeQuerier{getTaskByIDAndIntegrationErr: pgx.ErrNoRows}
	s := newTestServer(q)
	s.SetTransitioner(&fakeTransitioner{to: task.StatusWaitingUser})

	serverConn, clientConn, cleanup := newWSPair(t)
	defer cleanup()

	env := commandApprovalRequestEnvelope(t, bus.NewMessageID(), uuid.New().String(), uuid.New().String(), "rm -rf /", "опасная команда")
	s.handleMachineFrame(context.Background(), serverConn, uuid.New(), marshalEnvelope(t, env))

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, _, err := clientConn.Read(ctx)
	if err == nil {
		t.Fatal("клиент получил ack, хотя задача не найдена для этой интеграции")
	}
}

// TestHandleMachineFrame_CommandApprovalRequest_NoTransitionerConfigured —
// transitioner не зарегистрирован (nil) → ack НЕ отправляется, паники нет.
func TestHandleMachineFrame_CommandApprovalRequest_NoTransitionerConfigured(t *testing.T) {
	dbIntegrationID := uuid.New()
	taskID := uuid.New()
	q := fakeQuerier{
		getTaskByIDAndIntegrationResult: db.Task{ID: pgtype.UUID{Bytes: taskID, Valid: true}},
	}
	s := newTestServer(q)
	// transitioner намеренно не зарегистрирован.

	serverConn, clientConn, cleanup := newWSPair(t)
	defer cleanup()

	env := commandApprovalRequestEnvelope(t, bus.NewMessageID(), taskID.String(), uuid.New().String(), "make deploy", "деплой прод")
	s.handleMachineFrame(context.Background(), serverConn, dbIntegrationID, marshalEnvelope(t, env))

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, _, err := clientConn.Read(ctx)
	if err == nil {
		t.Fatal("клиент получил ack, хотя transitioner не настроен")
	}
}

// TestHandleMachineFrame_CommandApprovalRequest_TransitionError —
// TransitionWithEvent вернул ошибку (недопустимый переход FSM/сбой БД) →
// ack НЕ отправляется.
func TestHandleMachineFrame_CommandApprovalRequest_TransitionError(t *testing.T) {
	dbIntegrationID := uuid.New()
	taskID := uuid.New()
	q := fakeQuerier{
		getTaskByIDAndIntegrationResult: db.Task{ID: pgtype.UUID{Bytes: taskID, Valid: true}},
	}
	s := newTestServer(q)
	s.SetTransitioner(&fakeTransitioner{err: errors.New("недопустимый переход")})

	serverConn, clientConn, cleanup := newWSPair(t)
	defer cleanup()

	env := commandApprovalRequestEnvelope(t, bus.NewMessageID(), taskID.String(), uuid.New().String(), "make deploy", "деплой прод")
	s.handleMachineFrame(context.Background(), serverConn, dbIntegrationID, marshalEnvelope(t, env))

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, _, err := clientConn.Read(ctx)
	if err == nil {
		t.Fatal("клиент получил ack, хотя TransitionWithEvent вернул ошибку")
	}
}

// TestHandleMachineFrame_CommandApprovalRequest_InvalidPayload_DoesNotPanic —
// payload без request_id (или отсутствующий task_id) → ack НЕ отправляется,
// handleMachineFrame не паникует и не закрывает соединение (тот же принцип
// «безопасно игнорировать», что и у agent_question).
func TestHandleMachineFrame_CommandApprovalRequest_InvalidPayload_DoesNotPanic(t *testing.T) {
	taskID := uuid.New().String()
	envNoRequestID := bus.Envelope{
		MessageID:       bus.NewMessageID(),
		TaskID:          &taskID,
		IntegrationID:   uuid.New().String(),
		Type:            bus.MessageTypeCommandApprovalRequest,
		Ts:              time.Now().UTC().Format(time.RFC3339),
		ProtocolVersion: bus.ProtocolVersion,
		Payload:         json.RawMessage(`{"command":"rm -rf /","reason":"нет request_id"}`),
	}
	envNoTaskID := bus.Envelope{
		MessageID:       bus.NewMessageID(),
		IntegrationID:   uuid.New().String(),
		Type:            bus.MessageTypeCommandApprovalRequest,
		Ts:              time.Now().UTC().Format(time.RFC3339),
		ProtocolVersion: bus.ProtocolVersion,
		Payload:         json.RawMessage(`{"request_id":"r1","command":"rm -rf /","reason":"нет task_id"}`),
	}

	q := fakeQuerier{
		getTaskByIDAndIntegrationResult: db.Task{},
	}
	s := newTestServer(q)
	s.SetTransitioner(&fakeTransitioner{to: task.StatusWaitingUser})

	serverConn, clientConn, cleanup := newWSPair(t)
	defer cleanup()

	for name, env := range map[string]bus.Envelope{
		"без request_id": envNoRequestID,
		"без task_id":    envNoTaskID,
	} {
		t.Run(name, func(t *testing.T) {
			s.handleMachineFrame(context.Background(), serverConn, uuid.New(), marshalEnvelope(t, env))

			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			_, _, err := clientConn.Read(ctx)
			if err == nil {
				t.Fatalf("%s: клиент получил ack, хотя payload/конверт невалиден", name)
			}
		})
	}
}

// taskAcceptedEnvelope собирает валидный конверт type==task_accepted (тикет
// 5.4, FR E1, protocol.md §4) для заданной задачи. Payload — {} (пустой
// объект, см. bus.MessageTypeTaskAccepted): в отличие от agent_question,
// у task_accepted нет дополнительного бизнес-payload.
func taskAcceptedEnvelope(t *testing.T, messageID, taskID string) bus.Envelope {
	t.Helper()
	return bus.Envelope{
		MessageID:       messageID,
		TaskID:          &taskID,
		IntegrationID:   uuid.New().String(), // не используется handleTaskAccepted — см. агрумент integrationID
		Type:            bus.MessageTypeTaskAccepted,
		Ts:              time.Now().UTC().Format(time.RFC3339),
		ProtocolVersion: bus.ProtocolVersion,
		Payload:         json.RawMessage(`{}`),
	}
}

// TestHandleMachineFrame_TaskAccepted_HappyPath — task_accepted успешно
// переводит задачу queued→running (fakeTransitioner.Transition) и агенту
// приходит ack с ack_message_id исходного конверта (тикет 5.4, FR E1).
func TestHandleMachineFrame_TaskAccepted_HappyPath(t *testing.T) {
	dbIntegrationID := uuid.New()
	taskID := uuid.New()

	q := fakeQuerier{
		getTaskByIDAndIntegrationResult: db.Task{
			ID:            pgtype.UUID{Bytes: taskID, Valid: true},
			IntegrationID: pgtype.UUID{Bytes: dbIntegrationID, Valid: true},
		},
	}
	s := newTestServer(q)
	transitioner := &fakeTransitioner{to: task.StatusRunning}
	s.SetTransitioner(transitioner)

	serverConn, clientConn, cleanup := newWSPair(t)
	defer cleanup()

	messageID := bus.NewMessageID()
	env := taskAcceptedEnvelope(t, messageID, taskID.String())
	s.handleMachineFrame(context.Background(), serverConn, dbIntegrationID, marshalEnvelope(t, env))

	if transitioner.lastTaskID.Bytes != taskID {
		t.Fatalf("Transition вызван с taskID = %s, ожидался %s", uuid.UUID(transitioner.lastTaskID.Bytes), taskID)
	}
	if transitioner.lastTrigger != task.TriggerTaskAccepted {
		t.Fatalf("Transition вызван с trigger = %q, ожидался %q", transitioner.lastTrigger, task.TriggerTaskAccepted)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, data, err := clientConn.Read(ctx)
	if err != nil {
		t.Fatalf("клиент не получил ack-кадр: %v", err)
	}
	var ackEnv bus.Envelope
	if err := json.Unmarshal(data, &ackEnv); err != nil {
		t.Fatalf("ack-кадр не парсится: %v", err)
	}
	if ackEnv.Type != bus.MessageTypeAck {
		t.Fatalf("ack-кадр type=%q, ожидался %q", ackEnv.Type, bus.MessageTypeAck)
	}
	var ackPayload bus.AckPayload
	if err := json.Unmarshal(ackEnv.Payload, &ackPayload); err != nil {
		t.Fatalf("ack-payload не парсится: %v", err)
	}
	if ackPayload.AckMessageID != messageID {
		t.Fatalf("ack_message_id=%q, ожидался %q", ackPayload.AckMessageID, messageID)
	}
}

// TestHandleMachineFrame_TaskAccepted_TaskNotFoundOrWrongIntegration — задача
// не найдена для этой интеграции (GetTaskByIDAndIntegration → pgx.ErrNoRows,
// в т.ч. подделанный task_id чужой задачи) → ack НЕ отправляется, соединение
// не падает (тикет 5.4, та же owner-scoped проверка, что и в
// handleAgentQuestion).
func TestHandleMachineFrame_TaskAccepted_TaskNotFoundOrWrongIntegration(t *testing.T) {
	q := fakeQuerier{getTaskByIDAndIntegrationErr: pgx.ErrNoRows}
	s := newTestServer(q)
	s.SetTransitioner(&fakeTransitioner{to: task.StatusRunning})

	serverConn, clientConn, cleanup := newWSPair(t)
	defer cleanup()

	env := taskAcceptedEnvelope(t, bus.NewMessageID(), uuid.New().String())
	s.handleMachineFrame(context.Background(), serverConn, uuid.New(), marshalEnvelope(t, env))

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, _, err := clientConn.Read(ctx)
	if err == nil {
		t.Fatal("клиент получил ack, хотя задача не найдена для этой интеграции")
	}
}

// TestHandleMachineFrame_TaskAccepted_NoTransitionerConfigured — transitioner
// не зарегистрирован (nil) → ack НЕ отправляется, паники нет.
func TestHandleMachineFrame_TaskAccepted_NoTransitionerConfigured(t *testing.T) {
	taskID := uuid.New()
	q := fakeQuerier{
		getTaskByIDAndIntegrationResult: db.Task{ID: pgtype.UUID{Bytes: taskID, Valid: true}},
	}
	s := newTestServer(q)
	// transitioner намеренно не зарегистрирован.

	serverConn, clientConn, cleanup := newWSPair(t)
	defer cleanup()

	env := taskAcceptedEnvelope(t, bus.NewMessageID(), taskID.String())
	s.handleMachineFrame(context.Background(), serverConn, uuid.New(), marshalEnvelope(t, env))

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, _, err := clientConn.Read(ctx)
	if err == nil {
		t.Fatal("клиент получил ack, хотя transitioner не настроен")
	}
}

// TestHandleMachineFrame_TaskAccepted_TransitionError — Transition вернул
// ошибку (например, задача уже не в queued — повторная доставка того же
// task_assigned после потери ack) → ack НЕ отправляется.
func TestHandleMachineFrame_TaskAccepted_TransitionError(t *testing.T) {
	taskID := uuid.New()
	q := fakeQuerier{
		getTaskByIDAndIntegrationResult: db.Task{ID: pgtype.UUID{Bytes: taskID, Valid: true}},
	}
	s := newTestServer(q)
	s.SetTransitioner(&fakeTransitioner{err: errors.New("недопустимый переход")})

	serverConn, clientConn, cleanup := newWSPair(t)
	defer cleanup()

	env := taskAcceptedEnvelope(t, bus.NewMessageID(), taskID.String())
	s.handleMachineFrame(context.Background(), serverConn, uuid.New(), marshalEnvelope(t, env))

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, _, err := clientConn.Read(ctx)
	if err == nil {
		t.Fatal("клиент получил ack, хотя Transition вернул ошибку")
	}
}

// TestHandleMachineFrame_TaskAccepted_NoTaskID_DoesNotPanic — конверт
// task_accepted без task_id → ack НЕ отправляется, handleMachineFrame не
// паникует и не закрывает соединение (тот же принцип «безопасно
// игнорировать», что и у agent_question/ack/heartbeat).
func TestHandleMachineFrame_TaskAccepted_NoTaskID_DoesNotPanic(t *testing.T) {
	env := bus.Envelope{
		MessageID:       bus.NewMessageID(),
		IntegrationID:   uuid.New().String(),
		Type:            bus.MessageTypeTaskAccepted,
		Ts:              time.Now().UTC().Format(time.RFC3339),
		ProtocolVersion: bus.ProtocolVersion,
		Payload:         json.RawMessage(`{}`),
	}

	q := fakeQuerier{}
	s := newTestServer(q)
	s.SetTransitioner(&fakeTransitioner{to: task.StatusRunning})

	serverConn, clientConn, cleanup := newWSPair(t)
	defer cleanup()

	s.handleMachineFrame(context.Background(), serverConn, uuid.New(), marshalEnvelope(t, env))

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, _, err := clientConn.Read(ctx)
	if err == nil {
		t.Fatal("клиент получил ack, хотя конверт task_accepted без task_id")
	}
}

// agentErrorEnvelope собирает валидный конверт type==error (тикет 5.8, FR E1,
// protocol.md §4) для заданной задачи. Payload — bus.ErrorPayload{code,
// message}.
func agentErrorEnvelope(t *testing.T, messageID, taskID, code, message string) bus.Envelope {
	t.Helper()
	payload, err := json.Marshal(bus.ErrorPayload{Code: code, Message: message})
	if err != nil {
		t.Fatalf("marshal ErrorPayload: %v", err)
	}
	return bus.Envelope{
		MessageID:       messageID,
		TaskID:          &taskID,
		IntegrationID:   uuid.New().String(), // не используется handleAgentError — см. аргумент integrationID
		Type:            bus.MessageTypeError,
		Ts:              time.Now().UTC().Format(time.RFC3339),
		ProtocolVersion: bus.ProtocolVersion,
		Payload:         payload,
	}
}

// TestHandleMachineFrame_AgentError_HappyPath — error успешно переводит
// задачу running→failed (fakeTransitioner) и агенту приходит ack с
// ack_message_id исходного конверта (тикет 5.8, FR E1).
func TestHandleMachineFrame_AgentError_HappyPath(t *testing.T) {
	dbIntegrationID := uuid.New()
	taskID := uuid.New()

	q := fakeQuerier{
		getTaskByIDAndIntegrationResult: db.Task{
			ID:            pgtype.UUID{Bytes: taskID, Valid: true},
			IntegrationID: pgtype.UUID{Bytes: dbIntegrationID, Valid: true},
		},
	}
	s := newTestServer(q)
	transitioner := &fakeTransitioner{to: task.StatusFailed}
	s.SetTransitioner(transitioner)

	serverConn, clientConn, cleanup := newWSPair(t)
	defer cleanup()

	messageID := bus.NewMessageID()
	env := agentErrorEnvelope(t, messageID, taskID.String(), "provider_error", "агент упал с паникой")
	s.handleMachineFrame(context.Background(), serverConn, dbIntegrationID, marshalEnvelope(t, env))

	if transitioner.lastTaskID.Bytes != taskID {
		t.Fatalf("TransitionWithEvent вызван с taskID = %s, ожидался %s", uuid.UUID(transitioner.lastTaskID.Bytes), taskID)
	}
	if transitioner.lastTrigger != task.TriggerAgentError {
		t.Fatalf("TransitionWithEvent вызван с trigger = %q, ожидался %q", transitioner.lastTrigger, task.TriggerAgentError)
	}
	if transitioner.lastEventType != "error" {
		t.Fatalf("TransitionWithEvent вызван с eventType = %q, ожидался %q", transitioner.lastEventType, "error")
	}
	if !bytes.Equal(transitioner.lastEventPayload, env.Payload) {
		t.Fatalf("TransitionWithEvent вызван с eventPayload = %s, ожидался %s", transitioner.lastEventPayload, env.Payload)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, data, err := clientConn.Read(ctx)
	if err != nil {
		t.Fatalf("клиент не получил ack-кадр: %v", err)
	}
	var ackEnv bus.Envelope
	if err := json.Unmarshal(data, &ackEnv); err != nil {
		t.Fatalf("ack-кадр не парсится: %v", err)
	}
	if ackEnv.Type != bus.MessageTypeAck {
		t.Fatalf("ack-кадр type=%q, ожидался %q", ackEnv.Type, bus.MessageTypeAck)
	}
	var ackPayload bus.AckPayload
	if err := json.Unmarshal(ackEnv.Payload, &ackPayload); err != nil {
		t.Fatalf("ack-payload не парсится: %v", err)
	}
	if ackPayload.AckMessageID != messageID {
		t.Fatalf("ack_message_id=%q, ожидался %q", ackPayload.AckMessageID, messageID)
	}
}

// TestHandleMachineFrame_AgentError_TaskNotFoundOrWrongIntegration — задача
// не найдена для этой интеграции (GetTaskByIDAndIntegration → pgx.ErrNoRows,
// в т.ч. подделанный task_id чужой задачи) → ack НЕ отправляется, соединение
// не падает (тикет 5.8, та же owner-scoped проверка, что и в
// handleAgentQuestion/handleTaskAccepted).
func TestHandleMachineFrame_AgentError_TaskNotFoundOrWrongIntegration(t *testing.T) {
	q := fakeQuerier{getTaskByIDAndIntegrationErr: pgx.ErrNoRows}
	s := newTestServer(q)
	s.SetTransitioner(&fakeTransitioner{to: task.StatusFailed})

	serverConn, clientConn, cleanup := newWSPair(t)
	defer cleanup()

	env := agentErrorEnvelope(t, bus.NewMessageID(), uuid.New().String(), "provider_error", "ошибка")
	s.handleMachineFrame(context.Background(), serverConn, uuid.New(), marshalEnvelope(t, env))

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, _, err := clientConn.Read(ctx)
	if err == nil {
		t.Fatal("клиент получил ack, хотя задача не найдена для этой интеграции")
	}
}

// TestHandleMachineFrame_AgentError_NoTransitionerConfigured — transitioner
// не зарегистрирован (nil) → ack НЕ отправляется, паники нет.
func TestHandleMachineFrame_AgentError_NoTransitionerConfigured(t *testing.T) {
	dbIntegrationID := uuid.New()
	taskID := uuid.New()
	q := fakeQuerier{
		getTaskByIDAndIntegrationResult: db.Task{ID: pgtype.UUID{Bytes: taskID, Valid: true}},
	}
	s := newTestServer(q)
	// transitioner намеренно не зарегистрирован.

	serverConn, clientConn, cleanup := newWSPair(t)
	defer cleanup()

	env := agentErrorEnvelope(t, bus.NewMessageID(), taskID.String(), "provider_error", "ошибка")
	s.handleMachineFrame(context.Background(), serverConn, dbIntegrationID, marshalEnvelope(t, env))

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, _, err := clientConn.Read(ctx)
	if err == nil {
		t.Fatal("клиент получил ack, хотя transitioner не настроен")
	}
}

// TestHandleMachineFrame_AgentError_TransitionError — TransitionWithEvent
// вернул ошибку (например, задача уже не в running) → ack НЕ отправляется.
func TestHandleMachineFrame_AgentError_TransitionError(t *testing.T) {
	dbIntegrationID := uuid.New()
	taskID := uuid.New()
	q := fakeQuerier{
		getTaskByIDAndIntegrationResult: db.Task{ID: pgtype.UUID{Bytes: taskID, Valid: true}},
	}
	s := newTestServer(q)
	s.SetTransitioner(&fakeTransitioner{err: errors.New("недопустимый переход")})

	serverConn, clientConn, cleanup := newWSPair(t)
	defer cleanup()

	env := agentErrorEnvelope(t, bus.NewMessageID(), taskID.String(), "provider_error", "ошибка")
	s.handleMachineFrame(context.Background(), serverConn, dbIntegrationID, marshalEnvelope(t, env))

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, _, err := clientConn.Read(ctx)
	if err == nil {
		t.Fatal("клиент получил ack, хотя TransitionWithEvent вернул ошибку")
	}
}

// TestHandleMachineFrame_AgentError_NoTaskID_DoesNotPanic — конверт error без
// task_id → ack НЕ отправляется, handleMachineFrame не паникует и не
// закрывает соединение (тот же принцип «безопасно игнорировать», что и у
// agent_question/task_accepted).
func TestHandleMachineFrame_AgentError_NoTaskID_DoesNotPanic(t *testing.T) {
	payload, err := json.Marshal(bus.ErrorPayload{Code: "provider_error", Message: "ошибка без task_id"})
	if err != nil {
		t.Fatalf("marshal ErrorPayload: %v", err)
	}
	env := bus.Envelope{
		MessageID:       bus.NewMessageID(),
		IntegrationID:   uuid.New().String(),
		Type:            bus.MessageTypeError,
		Ts:              time.Now().UTC().Format(time.RFC3339),
		ProtocolVersion: bus.ProtocolVersion,
		Payload:         payload,
	}

	q := fakeQuerier{}
	s := newTestServer(q)
	s.SetTransitioner(&fakeTransitioner{to: task.StatusFailed})

	serverConn, clientConn, cleanup := newWSPair(t)
	defer cleanup()

	s.handleMachineFrame(context.Background(), serverConn, uuid.New(), marshalEnvelope(t, env))

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, _, err = clientConn.Read(ctx)
	if err == nil {
		t.Fatal("клиент получил ack, хотя конверт error без task_id")
	}
}

// TestHandleMachineFrame_AgentError_EmptyMessage_DoesNotPanic — payload с
// пустым message (code может быть непустым) → невалидный error-кадр (тикет
// 5.8): ack НЕ отправляется, handleMachineFrame не паникует и не закрывает
// соединение.
func TestHandleMachineFrame_AgentError_EmptyMessage_DoesNotPanic(t *testing.T) {
	dbIntegrationID := uuid.New()
	taskID := uuid.New()
	q := fakeQuerier{
		getTaskByIDAndIntegrationResult: db.Task{ID: pgtype.UUID{Bytes: taskID, Valid: true}},
	}
	s := newTestServer(q)
	s.SetTransitioner(&fakeTransitioner{to: task.StatusFailed})

	serverConn, clientConn, cleanup := newWSPair(t)
	defer cleanup()

	env := agentErrorEnvelope(t, bus.NewMessageID(), taskID.String(), "provider_error", "")
	s.handleMachineFrame(context.Background(), serverConn, dbIntegrationID, marshalEnvelope(t, env))

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, _, err := clientConn.Read(ctx)
	if err == nil {
		t.Fatal("клиент получил ack, хотя payload error с пустым message")
	}
}

// helloEnvelope собирает конверт type==hello (protocol.md §2/§4,
// bus.HelloPayload) с заданными protocol_version и uuid-секретом — для
// тестов authenticateMachineHello (тикеты 2.3/4.7).
func helloEnvelope(t *testing.T, protocolVersion, uuidSecret string) bus.Envelope {
	t.Helper()
	payload, err := json.Marshal(bus.HelloPayload{UUID: uuidSecret, AgentVersion: "1.2.3"})
	if err != nil {
		t.Fatalf("marshal HelloPayload: %v", err)
	}
	return bus.Envelope{
		MessageID:       bus.NewMessageID(),
		IntegrationID:   uuidSecret,
		Type:            bus.MessageTypeHello,
		Ts:              time.Now().UTC().Format(time.RFC3339),
		ProtocolVersion: protocolVersion,
		Payload:         payload,
	}
}

// writeClientFrame пишет data от лица клиента (агента) в clientConn — общий
// хелпер для тестов authenticateMachineHello, чтобы не дублировать таймаут
// записи в каждом тесте.
func writeClientFrame(t *testing.T, clientConn *websocket.Conn, data []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := clientConn.Write(ctx, websocket.MessageText, data); err != nil {
		t.Fatalf("clientConn.Write: %v", err)
	}
}

// TestAuthenticateMachineHello_IncompatibleProtocolVersionRejected —
// приёмка тикета 4.7 (FR C5, ADR 0003): hello с protocol_version, не
// совпадающим с bus.ProtocolVersion, отклоняется ДО похода в БД —
// authenticateMachineHello возвращает (uuid.Nil, false), а соединение само
// закрывается кодом wsCloseIncompatibleProtocolVersion (4426) с понятной
// причиной, называющей и присланное, и ожидаемое значение версии.
func TestAuthenticateMachineHello_IncompatibleProtocolVersionRejected(t *testing.T) {
	// fakeQuerier без getIntegrationByUUIDHMACResult/Err: если бы проверка
	// protocol_version не сработала ДО похода в БД (см. ADR 0003 п.3), тест
	// упал бы на нулевом db.Integration{}, а не на ожидаемом close 4426 —
	// так тест заодно фиксирует и порядок проверок.
	s := newTestServer(fakeQuerier{})

	serverConn, clientConn, cleanup := newWSPair(t)
	defer cleanup()

	const badVersion = "999"
	secret := uuid.New().String()
	writeClientFrame(t, clientConn, marshalEnvelope(t, helloEnvelope(t, badVersion, secret)))

	// Читаем на клиенте КОНКУРЕНТНО с authenticateMachineHello (а не после
	// её возврата): conn.Close на сервере выполняет полный close-handshake
	// и ждёт ответного close-кадра от пира до 5с (coder/websocket
	// close.go); если клиент не читает в этот момент, он не может
	// ответить, и Close на сервере тратит все 5с впустую. Конкурентное
	// чтение даёт клиенту немедленно среагировать на close-кадр, как и в
	// реальности (read-loop агента читает постоянно).
	type readResult struct {
		err error
	}
	resultCh := make(chan readResult, 1)
	go func() {
		readCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _, err := clientConn.Read(readCtx)
		resultCh <- readResult{err: err}
	}()

	req := httptest.NewRequest(http.MethodGet, "/machine/ws", nil)
	integrationID, ok := s.authenticateMachineHello(req, serverConn)
	if ok {
		t.Fatalf("authenticateMachineHello: ok=true (integrationID=%s), ожидался отказ по несовместимому protocol_version", integrationID)
	}
	if integrationID != uuid.Nil {
		t.Fatalf("authenticateMachineHello: integrationID=%s, ожидался uuid.Nil при отказе", integrationID)
	}

	res := <-resultCh
	err := res.err
	if err == nil {
		t.Fatal("clientConn.Read: ожидалось закрытие соединения, а не кадр данных")
	}
	var closeErr websocket.CloseError
	if !errors.As(err, &closeErr) {
		t.Fatalf("ошибка чтения клиента — не websocket.CloseError: %v", err)
	}
	if closeErr.Code != wsCloseIncompatibleProtocolVersion {
		t.Fatalf("close code = %d, ожидался %d (wsCloseIncompatibleProtocolVersion)", closeErr.Code, wsCloseIncompatibleProtocolVersion)
	}
	if !strings.Contains(closeErr.Reason, badVersion) || !strings.Contains(closeErr.Reason, bus.ProtocolVersion) {
		t.Fatalf("close reason = %q, ожидалось понятное упоминание присланной (%q) и ожидаемой (%q) версии", closeErr.Reason, badVersion, bus.ProtocolVersion)
	}
}

// TestAuthenticateMachineHello_CompatibleProtocolVersionAndValidAuthSucceeds —
// регрессия: hello с СОВПАДАЮЩИМ protocol_version и валидным (найденным по
// HMAC) UUID продолжает аутентифицироваться как и до тикета 4.7— введённая
// проверка версии не должна ломать штатный путь (FR B3/B6, тикет 2.3).
func TestAuthenticateMachineHello_CompatibleProtocolVersionAndValidAuthSucceeds(t *testing.T) {
	dbIntegrationID := uuid.New()
	q := fakeQuerier{
		getIntegrationByUUIDHMACResult: db.Integration{
			ID: pgtype.UUID{Bytes: dbIntegrationID, Valid: true},
		},
	}
	s := newTestServer(q)

	serverConn, clientConn, cleanup := newWSPair(t)
	defer cleanup()

	secret := uuid.New().String()
	writeClientFrame(t, clientConn, marshalEnvelope(t, helloEnvelope(t, bus.ProtocolVersion, secret)))

	req := httptest.NewRequest(http.MethodGet, "/machine/ws", nil)
	integrationID, ok := s.authenticateMachineHello(req, serverConn)
	if !ok {
		t.Fatal("authenticateMachineHello: ok=false, ожидался успех для совместимого protocol_version и валидного UUID/HMAC (регрессия тикета 4.7)")
	}
	if integrationID != dbIntegrationID {
		t.Fatalf("authenticateMachineHello: integrationID=%s, ожидался %s", integrationID, dbIntegrationID)
	}
}
