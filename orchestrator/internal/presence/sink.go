package presence

import (
	"context"
	"fmt"

	"github.com/yarabey/agentify/internal/bus"
)

// busProducer — узкий интерфейс на единственном методе *bus.Producer, нужном
// Sink (см. godoc пакета). Сужение — для юнит-тестов: Sink хранит именно этот
// интерфейс (а не конкретный *bus.Producer), поэтому tests могут собрать Sink
// с фейковой реализацией напрямую (структурный литерал в том же пакете, см.
// sink_test.go), не поднимая Redpanda; в проде NewSink принимает настоящий
// *bus.Producer (он ему удовлетворяет).
type busProducer interface {
	PublishKeyed(ctx context.Context, topic, keyField string, env bus.Envelope) error
}

// Sink — реализация api.EventSink (тикет 3.6, см. godoc пакета): публикует
// конверт события машины в топик machine.events. Собирается через NewSink;
// нулевое значение не готово к использованию (нет producer).
type Sink struct {
	producer busProducer
}

// NewSink собирает Sink поверх продьюсера Redpanda. producer обязателен
// (nil — ошибка конструктора, не паника, тот же принцип, что и у
// bridge.New).
func NewSink(producer *bus.Producer) (*Sink, error) {
	if producer == nil {
		return nil, fmt.Errorf("presence: nil producer")
	}
	return &Sink{producer: producer}, nil
}

// HandleEvent — реализация api.EventSink: публикует env в machine.events
// (ADR 0001). Ключ партиции выбирается по ОБЩЕМУ правилу раскладки ADR 0001,
// не завязанному специально на heartbeat: task_id, если он есть в конверте,
// иначе integration_id (heartbeat, будучи событием уровня машины, а не
// задачи, всегда попадает во вторую ветку — task_id у него nil). Ошибка
// публикации пробрасывается вызывающему (GetMachineWs/handleMachineEvent) —
// он не отправит агенту ack, и агент повторит событие сам через свой durable
// outbox (тикет 3.5, protocol.md §5).
func (s *Sink) HandleEvent(ctx context.Context, env bus.Envelope) error {
	keyField := bus.PartitionKeyIntegrationID
	if env.TaskID != nil {
		keyField = bus.PartitionKeyTaskID
	}
	if err := s.producer.PublishKeyed(ctx, bus.TopicMachineEvents, keyField, env); err != nil {
		return fmt.Errorf("presence: публикация события в %q: %w", bus.TopicMachineEvents, err)
	}
	return nil
}
