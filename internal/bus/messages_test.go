package bus

import (
	"encoding/json"
	"testing"
)

// TestMessageTypeConstantsUniqueAndNonEmpty проверяет, что все объявленные
// MessageType* константы (protocol.md §4) непустые и не сталкиваются друг с
// другом — иначе получатель не смог бы отличить типы сообщений по Type.
func TestMessageTypeConstantsUniqueAndNonEmpty(t *testing.T) {
	types := []string{
		MessageTypeHello,
		MessageTypeHeartbeat,
		MessageTypeAck,
		MessageTypeTaskAccepted,
		MessageTypeAgentQuestion,
		MessageTypeCommandApprovalRequest,
		MessageTypeAgentProgress,
		MessageTypeAgentCompleted,
		MessageTypeError,
		MessageTypeTaskAssigned,
		MessageTypeUserAnswer,
		MessageTypeCommandDecision,
		MessageTypeCancel,
		MessageTypePing,
	}
	seen := make(map[string]struct{}, len(types))
	for _, ty := range types {
		if ty == "" {
			t.Fatal("обнаружена пустая MessageType* константа")
		}
		if _, dup := seen[ty]; dup {
			t.Fatalf("дубль значения MessageType*: %q", ty)
		}
		seen[ty] = struct{}{}
	}
}

// TestHelloPayloadJSONFieldNames фиксирует json-теги HelloPayload по
// protocol.md §4 ({uuid, agent_version, providers[]}) — контракт, который
// должны соблюдать обе стороны (orchestrator GetMachineWs, agent wsclient).
func TestHelloPayloadJSONFieldNames(t *testing.T) {
	p := HelloPayload{UUID: "u", AgentVersion: "v", Providers: []string{"claude"}}
	data, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("Unmarshal map: %v", err)
	}
	for _, k := range []string{"uuid", "agent_version", "providers"} {
		if _, ok := m[k]; !ok {
			t.Errorf("HelloPayload не содержит обязательного поля %q (protocol.md §4)", k)
		}
	}
}
