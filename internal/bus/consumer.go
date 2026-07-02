package bus

// Consumer — обёртка-консьюмер на consumer group поверх franz-go (kgo) с РУЧНЫМ
// commit offset (kgo.DisableAutoCommit). Тикет 3.2, контракт — ADR 0001 (раздел
// «Consumer groups / commit offset», at-least-once) и protocol.md §5.
//
// Гарантии (Trace: FR E3/E7, §126):
//   - ПОРЯДОК по ключу партиции: записи одной партиции обрабатываются строго
//     последовательно (один поток на партицию), поэтому события с одним ключом
//     (task_id/integration_id, ADR 0001) видны Handler в порядке seq.
//   - ДЕДУП по message_id: повторный message_id (at-least-once) не вызывает Handler
//     повторно — гасится Deduper до вызова (см. dedup.go про границу vs durable).
//   - COMMIT ТОЛЬКО ПОСЛЕ УСПЕХА: offset партии коммитится лишь после успешного
//     применения её записей. Ошибка Handler => offset не двигается => запись
//     перечитается (at-least-once), дубль безопасен за счёт дедупа/идемпотентности.
//
// Семантика commit-after-ACK для команд (machine.commands, §5) реализуется НЕ здесь,
// а на уровне моста 3.4: этот Consumer даёт ручной commit и хук CommitRecords, на
// которых мост строит цикл «прочитал → WS → ack → commit». Сам ACK-цикл тут не делаем.

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Handler — колбэк обработки одного конверта. Возврат nil означает «применено
// успешно» (offset может быть закоммичен); ошибка означает «не применено» —
// Consumer прекращает обработку текущей партии и НЕ коммитит её offset, чтобы
// запись была перечитана (at-least-once, ADR 0001).
type Handler func(ctx context.Context, env Envelope) error

// ConsumerConfig — параметры консьюмера. Seeds и Group обязательны; Topics —
// топики для подписки; Dedup, если nil, создаётся со значениями по умолчанию.
type ConsumerConfig struct {
	// Seeds — адреса брокеров Redpanda (host:port).
	Seeds []string
	// Group — имя consumer group (одна логическая роль на группу, ADR 0001).
	Group string
	// Topics — топики для подписки (ADR 0001: machine.commands/events/...).
	Topics []string
	// Dedup — слой дедупа по message_id; nil => NewDeduper(0,0) (значения по умолчанию).
	Dedup *Deduper
	// Opts — дополнительные опции kgo (тесты/тюнинг); DisableAutoCommit и группа
	// выставляются конструктором независимо от Opts.
	Opts []kgo.Opt
}

// Consumer читает конверты из Redpanda в рамках consumer group с ручным commit.
// Создаётся NewConsumer, цикл обработки запускается Run, закрывается Close.
type Consumer struct {
	client *kgo.Client
	dedup  *Deduper
}

// NewConsumer создаёт консьюмера consumer group с ОТКЛЮЧЁННЫМ автокоммитом
// (kgo.DisableAutoCommit) — offset двигаем только мы, после успешного применения
// (ADR 0001). Возвращает ошибку при пустой группе/seeds или сбое инициализации.
func NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	if len(cfg.Seeds) == 0 {
		return nil, fmt.Errorf("bus: пустой список seeds брокеров для консьюмера")
	}
	if cfg.Group == "" {
		return nil, fmt.Errorf("bus: пустое имя consumer group")
	}
	dedup := cfg.Dedup
	if dedup == nil {
		dedup = NewDeduper(0, 0)
	}
	base := []kgo.Opt{
		kgo.SeedBrokers(cfg.Seeds...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topics...),
		// Ручной commit offset (ADR 0001): коммитим ТОЛЬКО после успешного
		// применения, чтобы недоприменённая запись перечиталась (at-least-once).
		kgo.DisableAutoCommit(),
	}
	client, err := kgo.NewClient(append(base, cfg.Opts...)...)
	if err != nil {
		return nil, fmt.Errorf("bus: создание kgo-клиента консьюмера: %w", err)
	}
	return &Consumer{client: client, dedup: dedup}, nil
}

// Run запускает цикл poll→apply→commit до отмены ctx. Для каждой партии записей:
//  1. конверт демаршалится и валидируется (битый — пропускается с продолжением);
//  2. повтор по message_id гасится Deduper (Handler не вызывается);
//  3. Handler применяет конверт; при ошибке партия НЕ коммитится и Run возвращает
//     ошибку (вызывающий решает: ретрай/перезапуск — at-least-once это допускает);
//  4. после успешной обработки всей партии её offset'ы коммитятся синхронно.
//
// Порядок в рамках партиции сохраняется: записи одной партиции в итерации идут
// последовательно (FetchTopicPartition → Records по возрастанию offset), Handler
// вызывается строго по порядку (§126).
func (c *Consumer) Run(ctx context.Context, handler Handler) error {
	if handler == nil {
		return fmt.Errorf("bus: nil Handler")
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil // отмена контекста — штатное завершение.
		}
		fetches := c.client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			// Контекст отменён во время poll — штатное завершение, не ошибка.
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("bus: poll fetches: %w", errs[0].Err)
		}

		var applyErr error
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			if applyErr != nil {
				return // уже была ошибка в этой итерации — не трогаем дальше.
			}
			for _, rec := range p.Records {
				if err := c.applyRecord(ctx, rec, handler); err != nil {
					applyErr = err
					return
				}
			}
		})
		if applyErr != nil {
			return applyErr
		}

		// Все записи партии применены успешно — коммитим их offset'ы синхронно
		// (commit-after-success, ADR 0001).
		if err := c.client.CommitUncommittedOffsets(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("bus: commit offsets: %w", err)
		}
	}
}

// applyRecord обрабатывает одну запись: демаршалинг → дедуп → Handler. Битый
// конверт пропускается (логировать/метрить — забота вызывающего слоя; здесь не
// валим всю партию из-за одной мусорной записи). Повтор по message_id гасится.
func (c *Consumer) applyRecord(ctx context.Context, rec *kgo.Record, handler Handler) error {
	env, err := Unmarshal(rec.Value)
	if err != nil {
		// Невалидная запись: не применяем, но и не блокируем партицию навсегда.
		// Возврат nil => offset продвинется при коммите партии (мусор не зациклит).
		return nil
	}
	if !c.dedup.MarkProcessed(env.MessageID) {
		return nil // повтор — Handler не вызываем (FR E7, «применяется один раз»).
	}
	if err := handler(ctx, env); err != nil {
		return fmt.Errorf("bus: handler для message_id=%s type=%s: %w", env.MessageID, env.Type, err)
	}
	return nil
}

// CommitRecords синхронно коммитит offset'ы переданных записей. Хук для моста 3.4
// (commit-after-ACK, §5): мост держит запись незакоммиченной, пока не получит ack
// агента, затем коммитит именно её через этот метод. В обычном Run-цикле
// коммитом управляет Consumer сам.
func (c *Consumer) CommitRecords(ctx context.Context, recs ...*kgo.Record) error {
	if err := c.client.CommitRecords(ctx, recs...); err != nil {
		return fmt.Errorf("bus: commit records: %w", err)
	}
	return nil
}

// PollFetches — второй хук для моста 3.4 (наравне с CommitRecords): тонкая
// обёртка над client.PollFetches, нужная мосту, чтобы строить СВОЙ цикл
// «прочитал запись → доставил в WS машины → дождался ack → закоммитил именно
// эту запись» (protocol.md §5), а не batch-цикл Run (Run коммитит ЦЕЛУЮ
// пачку партии сразу после успешного Handler — для команд machine.commands
// это было бы преждевременным коммитом ДО получения ack агента, что нарушило
// бы at-least-once гарантию §5). client — неэкспортированное поле, поэтому
// мосту из другого пакета (orchestrator/internal/bridge) нужен именно такой
// публичный проброс; собственное состояние (dedup и т.п.) Consumer тут не
// трогает — это осознанно «сырой» доступ к fetch-циклу для моста.
func (c *Consumer) PollFetches(ctx context.Context) kgo.Fetches {
	return c.client.PollFetches(ctx)
}

// Close покидает consumer group (коммитит то, что было помечено) и закрывает
// клиента. Вызывать при остановке сервиса.
func (c *Consumer) Close() {
	c.client.Close()
}

// ErrClientClosed — клиент консьюмера/продьюсера уже закрыт. Обёртка над
// kgo.ErrClientClosed для сверки через errors.Is без прямой зависимости от kgo
// у вызывающего слоя.
var ErrClientClosed = errors.New("bus: kgo-клиент закрыт")
