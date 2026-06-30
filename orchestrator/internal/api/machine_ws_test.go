// Unit-тесты разбора пост-hello кадров WS машины (тикет 3.4, protocol.md §5):
// parseAckFrame и handleMachineFrame — без сети/БД/Redpanda (в отличие от
// machine_ws_integration_test.go, который гоняет полный handshake через
// httptest.NewServer + реальный WS-клиент).
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
package api

import (
	"encoding/json"
	"testing"

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
	s.handleMachineFrame(marshalEnvelope(t, ackEnvelope(t, wantID)))

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
		s.handleMachineFrame(data)
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
	s.handleMachineFrame(marshalEnvelope(t, ackEnvelope(t, bus.NewMessageID())))
}
