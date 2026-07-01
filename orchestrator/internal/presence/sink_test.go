// Юнит-тесты Sink (тикет 3.6, FR B4) — без Redpanda/Docker: продьюсер
// подменяется фейком (busProducer), Sink собирается прямым структурным
// литералом (в отличие от NewSink, который принимает конкретный
// *bus.Producer, — см. godoc Sink) — тот же приём, что и в
// orchestrator/internal/bridge для busConsumer/ConnRegistry.
package presence

import (
	"context"
	"testing"
	"time"

	"github.com/yarabey/agentify/internal/bus"
)

// fakePublisher — фейковая реализация busProducer: запоминает
// topic/keyField/env последнего вызова PublishKeyed; err (если задан)
// возвращается вызывающему.
type fakePublisher struct {
	calls []publishCall
	err   error
}

type publishCall struct {
	topic    string
	keyField string
	env      bus.Envelope
}

func (f *fakePublisher) PublishKeyed(_ context.Context, topic, keyField string, env bus.Envelope) error {
	f.calls = append(f.calls, publishCall{topic: topic, keyField: keyField, env: env})
	return f.err
}

func heartbeatEnv(integrationID string) bus.Envelope {
	return bus.Envelope{
		MessageID:       bus.NewMessageID(),
		IntegrationID:   integrationID,
		TaskID:          nil,
		Type:            bus.MessageTypeHeartbeat,
		Ts:              time.Now().UTC().Format(time.RFC3339),
		ProtocolVersion: bus.ProtocolVersion,
		Payload:         []byte("{}"),
	}
}

// TestSink_HandleEvent_MachineLevelUsesIntegrationIDKey — heartbeat (task_id
// == nil) публикуется с ключом партиции integration_id (ADR 0001, protocol.md §6).
func TestSink_HandleEvent_MachineLevelUsesIntegrationIDKey(t *testing.T) {
	fp := &fakePublisher{}
	s := &Sink{producer: fp}

	env := heartbeatEnv("11111111-1111-1111-1111-111111111111")
	if err := s.HandleEvent(context.Background(), env); err != nil {
		t.Fatalf("HandleEvent: неожиданная ошибка: %v", err)
	}

	if len(fp.calls) != 1 {
		t.Fatalf("PublishKeyed вызван %d раз(а), ожидался 1", len(fp.calls))
	}
	call := fp.calls[0]
	if call.topic != bus.TopicMachineEvents {
		t.Fatalf("topic=%q, ожидался %q", call.topic, bus.TopicMachineEvents)
	}
	if call.keyField != bus.PartitionKeyIntegrationID {
		t.Fatalf("keyField=%q, ожидался %q", call.keyField, bus.PartitionKeyIntegrationID)
	}
	if call.env.MessageID != env.MessageID {
		t.Fatalf("опубликован не тот конверт: message_id=%q, ожидался %q", call.env.MessageID, env.MessageID)
	}
}

// TestSink_HandleEvent_TaskLevelUsesTaskIDKey — конверт с непустым task_id
// (общее правило раскладки ADR 0001, не завязанное на heartbeat) публикуется
// с ключом партиции task_id.
func TestSink_HandleEvent_TaskLevelUsesTaskIDKey(t *testing.T) {
	fp := &fakePublisher{}
	s := &Sink{producer: fp}

	taskID := "22222222-2222-2222-2222-222222222222"
	env := bus.Envelope{
		MessageID:       bus.NewMessageID(),
		IntegrationID:   "11111111-1111-1111-1111-111111111111",
		TaskID:          &taskID,
		Type:            bus.MessageTypeTaskAccepted,
		Ts:              time.Now().UTC().Format(time.RFC3339),
		ProtocolVersion: bus.ProtocolVersion,
		Payload:         []byte("{}"),
	}
	if err := s.HandleEvent(context.Background(), env); err != nil {
		t.Fatalf("HandleEvent: неожиданная ошибка: %v", err)
	}

	if len(fp.calls) != 1 {
		t.Fatalf("PublishKeyed вызван %d раз(а), ожидался 1", len(fp.calls))
	}
	if fp.calls[0].keyField != bus.PartitionKeyTaskID {
		t.Fatalf("keyField=%q, ожидался %q", fp.calls[0].keyField, bus.PartitionKeyTaskID)
	}
}

// TestSink_HandleEvent_PublishError_Propagated — ошибка публикации
// пробрасывается вызывающему (GetMachineWs не должен слать ack агенту).
func TestSink_HandleEvent_PublishError_Propagated(t *testing.T) {
	fp := &fakePublisher{err: context.DeadlineExceeded}
	s := &Sink{producer: fp}

	if err := s.HandleEvent(context.Background(), heartbeatEnv("11111111-1111-1111-1111-111111111111")); err == nil {
		t.Fatal("HandleEvent: ожидалась ошибка публикации, получен nil")
	}
}

// TestNewSink_NilProducer — nil producer — ошибка конструктора, не паника.
func TestNewSink_NilProducer(t *testing.T) {
	sink, err := NewSink(nil)
	if err == nil {
		t.Fatal("NewSink(nil): ожидалась ошибка")
	}
	if sink != nil {
		t.Fatalf("NewSink(nil): ожидался nil Sink, получен %v", sink)
	}
}
