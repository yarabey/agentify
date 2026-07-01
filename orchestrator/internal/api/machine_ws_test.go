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
package api

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

	"github.com/yarabey/agentify/internal/bus"
)

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
