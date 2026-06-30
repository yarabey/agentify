package bus

import (
	"encoding/json"
	"errors"
	"testing"
)

// taskIDPtr — хелпер для nullable task_id в тестах.
func taskIDPtr(s string) *string { return &s }

// TestEnvelopeMarshalUnmarshalRoundTrip проверяет, что конверт (protocol.md §2)
// корректно сериализуется и десериализуется без потери полей, включая json-теги
// и nullable task_id.
func TestEnvelopeMarshalUnmarshalRoundTrip(t *testing.T) {
	env := Envelope{
		MessageID:       NewMessageID(),
		TaskID:          taskIDPtr("task-1"),
		IntegrationID:   "int-1",
		Type:            "task_accepted",
		Seq:             7,
		Ts:              "2026-06-30T12:00:00Z",
		ProtocolVersion: ProtocolVersion,
		Payload:         json.RawMessage(`{"k":"v"}`),
	}
	data, err := env.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.MessageID != env.MessageID || got.IntegrationID != env.IntegrationID ||
		got.Type != env.Type || got.Seq != env.Seq || got.Ts != env.Ts ||
		got.ProtocolVersion != env.ProtocolVersion {
		t.Fatalf("round-trip потерял поля: got=%+v want=%+v", got, env)
	}
	if got.TaskID == nil || *got.TaskID != "task-1" {
		t.Fatalf("task_id round-trip: got=%v", got.TaskID)
	}
	if string(got.Payload) != `{"k":"v"}` {
		t.Fatalf("payload round-trip: got=%s", got.Payload)
	}
}

// TestEnvelopeJSONFieldNames фиксирует точные имена json-полей конверта по
// protocol.md §2 (контракт с агентом/мостом — снейк-кейс).
func TestEnvelopeJSONFieldNames(t *testing.T) {
	env := Envelope{
		MessageID: "m", IntegrationID: "i", Type: "t",
		Ts: "2026-06-30T12:00:00Z", ProtocolVersion: "1",
	}
	data, err := env.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("Unmarshal map: %v", err)
	}
	for _, k := range []string{"message_id", "task_id", "integration_id", "type", "seq", "ts", "protocol_version", "payload"} {
		if _, ok := m[k]; !ok {
			t.Errorf("конверт не содержит обязательного поля %q (protocol.md §2)", k)
		}
	}
}

// TestEnvelopeNullableTaskID проверяет, что nil TaskID сериализуется как
// "task_id": null (machine-level сообщения, protocol.md §2/§4).
func TestEnvelopeNullableTaskID(t *testing.T) {
	env := Envelope{
		MessageID: "m", TaskID: nil, IntegrationID: "i", Type: "heartbeat",
		Ts: "2026-06-30T12:00:00Z", ProtocolVersion: "1",
	}
	data, err := env.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var m map[string]json.RawMessage
	_ = json.Unmarshal(data, &m)
	if string(m["task_id"]) != "null" {
		t.Fatalf("ожидался task_id: null, got=%s", m["task_id"])
	}
}

// TestEnvelopeValidate проверяет, что Validate отлавливает отсутствие каждого
// обязательного поля §2 и возвращает соответствующую sentinel-ошибку.
func TestEnvelopeValidate(t *testing.T) {
	base := func() Envelope {
		return Envelope{
			MessageID: "m", IntegrationID: "i", Type: "t",
			Ts: "2026-06-30T12:00:00Z", ProtocolVersion: "1",
		}
	}
	cases := []struct {
		name    string
		mutate  func(*Envelope)
		wantErr error
	}{
		{"ok", func(*Envelope) {}, nil},
		{"no message_id", func(e *Envelope) { e.MessageID = "" }, ErrMissingMessageID},
		{"no integration_id", func(e *Envelope) { e.IntegrationID = "" }, ErrMissingIntegrationID},
		{"no type", func(e *Envelope) { e.Type = "" }, ErrMissingType},
		{"no ts", func(e *Envelope) { e.Ts = "" }, ErrMissingTimestamp},
		{"no protocol_version", func(e *Envelope) { e.ProtocolVersion = "" }, ErrMissingProtocolVersion},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := base()
			tc.mutate(&env)
			err := env.Validate()
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Validate: got %v, want %v", err, tc.wantErr)
			}
		})
	}
}

// TestUnmarshalRejectsBroken проверяет, что битый JSON и неполный конверт
// отвергаются Unmarshal (приёмник не применяет мусор).
func TestUnmarshalRejectsBroken(t *testing.T) {
	if _, err := Unmarshal([]byte("{not json")); err == nil {
		t.Error("ожидалась ошибка на битом JSON")
	}
	if _, err := Unmarshal([]byte(`{"type":"t"}`)); err == nil {
		t.Error("ожидалась ошибка на неполном конверте (нет message_id)")
	}
}

// TestEnvelopePartitionKey проверяет выбор ключа партиции из конверта по ADR 0001:
// integration_id всегда доступен; task_id обязателен для PartitionKeyTaskID и
// отвергается при nil (machine-level → integration_id).
func TestEnvelopePartitionKey(t *testing.T) {
	withTask := Envelope{MessageID: "m", IntegrationID: "int-9", TaskID: taskIDPtr("task-5"), Type: "agent_progress", Ts: "t", ProtocolVersion: "1"}
	machineLevel := Envelope{MessageID: "m", IntegrationID: "int-9", TaskID: nil, Type: "heartbeat", Ts: "t", ProtocolVersion: "1"}

	if k, err := withTask.PartitionKey(PartitionKeyIntegrationID); err != nil || k != "int-9" {
		t.Fatalf("integration_id ключ: got=%q err=%v", k, err)
	}
	if k, err := withTask.PartitionKey(PartitionKeyTaskID); err != nil || k != "task-5" {
		t.Fatalf("task_id ключ: got=%q err=%v", k, err)
	}
	if _, err := machineLevel.PartitionKey(PartitionKeyTaskID); err == nil {
		t.Fatal("ожидалась ошибка: machine-level не партиционируется по task_id")
	}
	if k, err := machineLevel.PartitionKey(PartitionKeyIntegrationID); err != nil || k != "int-9" {
		t.Fatalf("machine-level integration_id ключ: got=%q err=%v", k, err)
	}
	if _, err := withTask.PartitionKey("bogus"); err == nil {
		t.Fatal("ожидалась ошибка на неизвестном поле ключа")
	}
}

// TestNewMessageIDUnique проверяет, что генератор message_id выдаёт уникальные
// непустые значения (основа дедупа, §2).
func TestNewMessageIDUnique(t *testing.T) {
	seen := make(map[string]struct{}, 1000)
	for i := 0; i < 1000; i++ {
		id := NewMessageID()
		if id == "" {
			t.Fatal("пустой message_id")
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("дубль message_id: %s", id)
		}
		seen[id] = struct{}{}
	}
}
