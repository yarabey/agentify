package presence

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/yarabey/agentify/internal/bus"
)

// busConsumer — узкий интерфейс на *bus.Consumer.Run, нужном Consumer (см.
// godoc пакета). Сужение — для юнит-тестов (fakeBusConsumer вместо реальной
// Redpanda, см. consumer_test.go); в проде передаётся настоящий
// *bus.Consumer (он ему удовлетворяет).
type busConsumer interface {
	Run(ctx context.Context, handler bus.Handler) error
}

// onlineMarker — узкий интерфейс sqlc-запроса, нужного Consumer: пометить
// интеграцию online со свежим last_seen_at (тикет 3.6, FR B4). Реализуется
// *db.Queries; сужение позволяет юнит-тестам подменить запрос фейком без
// поднятия Postgres.
type onlineMarker interface {
	MarkIntegrationOnline(ctx context.Context, id pgtype.UUID) error
}

// ConsumerOption — функциональная опция NewConsumer (по аналогии с
// bridge.Option).
type ConsumerOption func(*Consumer)

// WithConsumerLogger задаёт логгер Consumer (по умолчанию — slog.Default()).
func WithConsumerLogger(logger *slog.Logger) ConsumerOption {
	return func(c *Consumer) {
		if logger != nil {
			c.logger = logger
		}
	}
}

// Consumer читает machine.events (consumer group "orchestrator-heartbeat",
// см. godoc пакета) и по каждому конверту type==heartbeat помечает
// соответствующую интеграцию online. Собирается через NewConsumer; нулевое
// значение не готово к использованию (нет consumer/queries). Run запускает
// цикл обработки.
type Consumer struct {
	consumer busConsumer
	queries  onlineMarker
	logger   *slog.Logger
}

// NewConsumer собирает Consumer поверх консьюмера Redpanda и sqlc-запросов.
// consumer и queries обязательны (nil — ошибка конструктора, не паника, тот
// же принцип, что и у bridge.New).
func NewConsumer(consumer busConsumer, queries onlineMarker, opts ...ConsumerOption) (*Consumer, error) {
	if consumer == nil {
		return nil, fmt.Errorf("presence: nil consumer")
	}
	if queries == nil {
		return nil, fmt.Errorf("presence: nil queries")
	}
	c := &Consumer{
		consumer: consumer,
		queries:  queries,
		logger:   slog.Default(),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

// Run — тонкая обёртка над bus.Consumer.Run с обычным batch-циклом
// commit-after-success (см. godoc пакета, почему это достаточно для
// heartbeat — в отличие от commit-after-ack моста команд). Возвращает
// управление только при отмене ctx (nil, штатное завершение) либо при
// неустранимой ошибке консьюмера.
func (c *Consumer) Run(ctx context.Context) error {
	return c.consumer.Run(ctx, c.handle)
}

// handle — bus.Handler для machine.events (тикет 3.6, FR B4). Любой конверт
// НЕ типа heartbeat молча игнорируется (nil) — другие типы событий
// (task_accepted/agent_question/... — тикеты 3.5/5.x) вне объёма этого
// консьюмера. Конверт с integration_id, не парсящимся как UUID, тоже
// игнорируется (логируется и nil) — тот же принцип, что и у bridge.deliver
// для битых команд: консьюмер не должен падать/останавливать всю партию
// из-за одной мусорной записи (at-least-once, ADR 0001).
func (c *Consumer) handle(ctx context.Context, env bus.Envelope) error {
	if env.Type != bus.MessageTypeHeartbeat {
		return nil
	}

	id, err := uuid.Parse(env.IntegrationID)
	if err != nil {
		c.logger.Warn("presence: integration_id heartbeat не парсится как UUID — пропускаю",
			slog.String("error", err.Error()), slog.String("message_id", env.MessageID))
		return nil
	}

	return c.queries.MarkIntegrationOnline(ctx, pgtype.UUID{Bytes: id, Valid: true})
}
