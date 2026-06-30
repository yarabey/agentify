package bus

// Producer — обёртка над franz-go (kgo) для публикации конвертов в Redpanda
// (тикет 3.2, ADR 0001). Синхронный produce с ключом партиции из конверта:
// ключ определяет партицию, а партиция — порядок (§126). Ключ передаёт
// вызывающий (он знает раскладку type→ключ, ADR 0001), но Producer проверяет
// его консистентность через Envelope.PartitionKey. Trace: FR E3 (надёжная
// доставка), §126 (порядок в рамках задачи через ключ партиции).

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer публикует конверты в Redpanda через franz-go. Потокобезопасен
// (kgo.Client потокобезопасен). Создаётся NewProducer, закрывается Close.
type Producer struct {
	client *kgo.Client
}

// NewProducer создаёт Producer, подключённый к брокерам Redpanda по адресам seeds
// (host:port). opts — дополнительные опции kgo (для тестов/тюнинга). Возвращает
// ошибку, если клиент не удалось инициализировать.
func NewProducer(seeds []string, opts ...kgo.Opt) (*Producer, error) {
	base := []kgo.Opt{
		kgo.SeedBrokers(seeds...),
		// Идемпотентный продьюсер franz-go (включён по умолчанию) гасит дубли
		// записей на уровне брокера при ретраях produce — дополняет дедуп по
		// message_id на стороне приёмника (ADR 0001, at-least-once).
	}
	client, err := kgo.NewClient(append(base, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("bus: создание kgo-клиента продьюсера: %w", err)
	}
	return &Producer{client: client}, nil
}

// Publish синхронно публикует конверт в topic с ключом партиции key. Конверт
// маршалится и валидируется (Envelope.Marshal); пустой topic/key отвергается,
// т.к. пустой ключ ломает детерминированное партиционирование (порядок, §126).
//
// Ключ обязан соответствовать раскладке ADR 0001 для топика; для проверки, что
// ключ действительно взят из конверта, вызывающий может получить его через
// Envelope.PartitionKey (см. PublishKeyed). Метод блокируется до подтверждения
// брокером или ошибки (синхронный produce).
func (p *Producer) Publish(ctx context.Context, topic, key string, env Envelope) error {
	if topic == "" {
		return fmt.Errorf("bus: пустой topic при публикации")
	}
	if key == "" {
		return fmt.Errorf("bus: пустой ключ партиции при публикации в %q (ломает порядок, §126/ADR 0001)", topic)
	}
	value, err := env.Marshal()
	if err != nil {
		return err
	}
	rec := &kgo.Record{
		Topic: topic,
		Key:   []byte(key),
		Value: value,
	}
	if res := p.client.ProduceSync(ctx, rec); res.FirstErr() != nil {
		return fmt.Errorf("bus: синхронная публикация в %q: %w", topic, res.FirstErr())
	}
	return nil
}

// PublishKeyed публикует конверт в topic, выводя ключ партиции из самого конверта
// по имени поля keyField (PartitionKeyIntegrationID / PartitionKeyTaskID, ADR 0001).
// Это безопасная обёртка над Publish: гарантирует, что ключ берётся из конверта
// консистентно и не расходится с содержимым сообщения (порядок, §126).
func (p *Producer) PublishKeyed(ctx context.Context, topic, keyField string, env Envelope) error {
	key, err := env.PartitionKey(keyField)
	if err != nil {
		return err
	}
	return p.Publish(ctx, topic, key, env)
}

// Close завершает работу продьюсера: дожидается недопубликованных записей и
// закрывает соединение с брокером. Вызывать при остановке сервиса.
func (p *Producer) Close() {
	p.client.Close()
}
