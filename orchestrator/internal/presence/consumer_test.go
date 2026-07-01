// Юнит-тесты Consumer (тикет 3.6, FR B4) — без Redpanda/Docker: bus.Consumer
// подменяется фейком (busConsumer), sqlc-запрос — фейком (onlineMarker), тот
// же приём, что и в orchestrator/internal/bridge для busConsumer/ConnRegistry.
//
// Покрывает приёмку тикета 3.6 «прочие типы кадров/битый integration_id
// безопасно игнорируются, heartbeat помечает интеграцию online»:
//   - handle игнорирует конверт НЕ типа heartbeat (MarkIntegrationOnline не вызван);
//   - handle игнорирует heartbeat с integration_id, не парсящимся как UUID
//     (не паникует, не вызывает запрос);
//   - handle вызывает MarkIntegrationOnline с DB id из integration_id
//     конверта для валидного heartbeat;
//   - Run — тонкая обёртка, передающая Consumer.handle в busConsumer.Run.
package presence

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/yarabey/agentify/internal/bus"
)

// fakeOnlineMarker — фейковая реализация onlineMarker: запоминает все id, для
// которых вызван MarkIntegrationOnline; err (если задан) возвращается
// вызывающему.
type fakeOnlineMarker struct {
	marked []pgtype.UUID
	err    error
}

func (f *fakeOnlineMarker) MarkIntegrationOnline(_ context.Context, id pgtype.UUID) error {
	f.marked = append(f.marked, id)
	return f.err
}

// fakeBusConsumer — фейковая реализация busConsumer: Run синхронно
// прогоняет заданные конверты через переданный handler и возвращает первую
// ошибку handler (если есть).
type fakeBusConsumer struct {
	envs []bus.Envelope
}

func (f *fakeBusConsumer) Run(ctx context.Context, handler bus.Handler) error {
	for _, env := range f.envs {
		if err := handler(ctx, env); err != nil {
			return err
		}
	}
	return nil
}

func testHeartbeatEnv(integrationID string) bus.Envelope {
	return bus.Envelope{
		MessageID:       bus.NewMessageID(),
		IntegrationID:   integrationID,
		Type:            bus.MessageTypeHeartbeat,
		Ts:              time.Now().UTC().Format(time.RFC3339),
		ProtocolVersion: bus.ProtocolVersion,
		Payload:         []byte("{}"),
	}
}

// TestConsumer_Handle_HeartbeatMarksOnline — валидный heartbeat помечает
// интеграцию online по её DB id (integration_id конверта).
func TestConsumer_Handle_HeartbeatMarksOnline(t *testing.T) {
	marker := &fakeOnlineMarker{}
	c, err := NewConsumer(&fakeBusConsumer{}, marker)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	id := uuid.New()
	if err := c.handle(context.Background(), testHeartbeatEnv(id.String())); err != nil {
		t.Fatalf("handle: неожиданная ошибка: %v", err)
	}

	if len(marker.marked) != 1 {
		t.Fatalf("MarkIntegrationOnline вызван %d раз(а), ожидался 1", len(marker.marked))
	}
	if marker.marked[0].Bytes != id {
		t.Fatalf("MarkIntegrationOnline получил %v, ожидался %v", uuid.UUID(marker.marked[0].Bytes), id)
	}
}

// TestConsumer_Handle_IgnoresNonHeartbeat — конверт НЕ типа heartbeat не
// вызывает MarkIntegrationOnline и не возвращает ошибку.
func TestConsumer_Handle_IgnoresNonHeartbeat(t *testing.T) {
	marker := &fakeOnlineMarker{}
	c, err := NewConsumer(&fakeBusConsumer{}, marker)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	env := bus.Envelope{
		MessageID:       bus.NewMessageID(),
		IntegrationID:   uuid.New().String(),
		Type:            bus.MessageTypeAck,
		Ts:              time.Now().UTC().Format(time.RFC3339),
		ProtocolVersion: bus.ProtocolVersion,
		Payload:         []byte(`{"ack_message_id":"x"}`),
	}
	if err := c.handle(context.Background(), env); err != nil {
		t.Fatalf("handle: неожиданная ошибка: %v", err)
	}
	if len(marker.marked) != 0 {
		t.Fatalf("MarkIntegrationOnline вызван для не-heartbeat конверта: %v", marker.marked)
	}
}

// TestConsumer_Handle_IgnoresInvalidIntegrationID — heartbeat с
// integration_id, не парсящимся как UUID, безопасно игнорируется (не
// паникует, не вызывает запрос, не возвращает ошибку — иначе консьюмер
// зациклился бы на битой записи).
func TestConsumer_Handle_IgnoresInvalidIntegrationID(t *testing.T) {
	marker := &fakeOnlineMarker{}
	c, err := NewConsumer(&fakeBusConsumer{}, marker)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	if err := c.handle(context.Background(), testHeartbeatEnv("not-a-uuid")); err != nil {
		t.Fatalf("handle: неожиданная ошибка: %v", err)
	}
	if len(marker.marked) != 0 {
		t.Fatalf("MarkIntegrationOnline вызван для битого integration_id: %v", marker.marked)
	}
}

// TestConsumer_Handle_QueryError_Propagated — ошибка запроса пробрасывается
// вызывающему (bus.Consumer.Run не закоммитит offset — at-least-once, запись
// перечитается).
func TestConsumer_Handle_QueryError_Propagated(t *testing.T) {
	marker := &fakeOnlineMarker{err: errors.New("db down")}
	c, err := NewConsumer(&fakeBusConsumer{}, marker)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	if err := c.handle(context.Background(), testHeartbeatEnv(uuid.New().String())); err == nil {
		t.Fatal("handle: ожидалась ошибка запроса, получен nil")
	}
}

// TestConsumer_Run_DelegatesToBusConsumer — Run передаёт c.handle в
// busConsumer.Run и прогоняет через него все конверты.
func TestConsumer_Run_DelegatesToBusConsumer(t *testing.T) {
	marker := &fakeOnlineMarker{}
	id := uuid.New()
	fc := &fakeBusConsumer{envs: []bus.Envelope{testHeartbeatEnv(id.String())}}
	c, err := NewConsumer(fc, marker)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	if err := c.Run(context.Background()); err != nil {
		t.Fatalf("Run: неожиданная ошибка: %v", err)
	}
	if len(marker.marked) != 1 {
		t.Fatalf("MarkIntegrationOnline вызван %d раз(а) через Run, ожидался 1", len(marker.marked))
	}
}

// TestNewConsumer_NilArgs — nil consumer/queries — ошибка конструктора, не паника.
func TestNewConsumer_NilArgs(t *testing.T) {
	if _, err := NewConsumer(nil, &fakeOnlineMarker{}); err == nil {
		t.Fatal("NewConsumer(nil, ...): ожидалась ошибка")
	}
	if _, err := NewConsumer(&fakeBusConsumer{}, nil); err == nil {
		t.Fatal("NewConsumer(..., nil): ожидалась ошибка")
	}
}
