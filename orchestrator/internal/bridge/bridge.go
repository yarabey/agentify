// Package bridge — мост Redpanda → WS машины (тикет 3.4, протокол §5,
// ADR 0001 «Consumer groups / commit offset», ADR 0002 «machine-ws duplicate
// uuid policy»).
//
// Назначение (бизнес): команды оркестратор → агент (FR E1/F2/F3/E6, §4
// "Оркестратор → агент") публикуются продьюсером в Redpanda-топик
// machine.commands (ADR 0001, ключ партиции — integration_id) — так
// доставка переживает временную недоступность конкретной машины и не
// зависит от того, поднят ли в данный момент HTTP/WS-процесс оркестратора,
// который изначально принял команду (бизнес-ТЗ §124-126). Bridge — тот
// компонент, который читает эти записи и реально доводит их до машины по
// открытому WS-соединению (orchestrator/internal/api.GetMachineWs), с
// гарантией at-least-once: запись считается доставленной ТОЛЬКО после того,
// как агент явно подтвердил её обработку кадром ack{ack_message_id}
// (protocol.md §5) — до этого момента offset записи НЕ коммитится, то есть
// при перезапуске моста (рестарт процесса/ребаланс consumer group) она
// будет вычитана заново. Если у машины прямо сейчас нет активного
// WS-соединения (агент офлайн/переподключается), запись тоже не коммитится
// и доставка откладывается без потери сообщения и без busy-loop (см.
// offlineRetryInterval).
//
// Как устроено (тех): Run — единственный публичный цикл жизненного цикла.
// Он опрашивает Redpanda через busConsumer.PollFetches (тонкий проброс
// поверх *bus.Consumer.PollFetches, см. internal/bus/consumer.go — хук,
// специально добавленный тикетом 3.4, наравне с уже существовавшим
// CommitRecords) и НЕ использует batch-цикл bus.Consumer.Run: тот коммитит
// offset сразу после успешного применения Handler, что для machine.commands
// было бы преждевременным коммитом ДО получения ack агента (нарушение §5).
//
// Каждая прочитанная запись маршрутизируется в долгоживущую горутину-воркер,
// одну на integration_id (ключ записи, ADR 0001 — он же совпадает с
// IntegrationID конверта) — так записи ОДНОЙ машины обрабатываются строго
// последовательно (порядок команд машине сохраняется), а МЕДЛЕННАЯ/ОФЛАЙН
// машина блокирует доставку только себе самой: воркеры разных машин
// работают независимо и не ждут друг друга (это и есть требуемое свойство
// «один офлайн-агент не блокирует доставку остальным надолго», в отличие от
// маршрутизации по партиции Kafka, где несколько разных машин могли бы
// делить одну партицию и поэтому — общий FIFO внутри неё).
//
// Поиск активного WS-соединения машины — через ConnRegistry (реализуется
// *api.Server.MachineConn; реестр Server.machineConns заведён тикетом 2.4 и
// ADR 0002 прямо предусматривает его переиспользование мостом). Обратная
// связь от WS к мосту (ack-кадр) идёт через интерфейс api.AckSink, который
// Bridge реализует структурно (метод HandleAck) — пакеты api и bridge не
// импортируют друг друга, связывает их orchestrator/main.go.
package bridge

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/yarabey/agentify/internal/bus"
)

// defaultAckTimeout — сколько Bridge ждёт ack агента на одну попытку
// доставки команды, прежде чем счесть попытку неудачной и повторить
// (protocol.md §5: «нет ack за таймаут … → переотправка»). Конкретное
// значение не зафиксировано протоколом; 30s — разумный запас на сетевые
// задержки и обработку команды агентом без чрезмерно долгого удержания
// записи незакоммиченной.
const defaultAckTimeout = 30 * time.Second

// defaultOfflineRetryInterval — пауза перед повторной попыткой найти
// активное WS-соединение машины, если его не было (агент офлайн/между
// реконнектами), и перед повторной попыткой после неудачной записи в WS.
// Нужна, чтобы воркер машины не превращался в busy-loop, опрашивающий
// ConnRegistry в цикле без пауз.
const defaultOfflineRetryInterval = 5 * time.Second

// writeTimeout — крайний срок одной записи команды в WS-соединение машины.
// Симметрично machineHelloReadTimeout/helloWriteTimeout в
// orchestrator/internal/api/machine_ws.go и agent/internal/wsclient: запись
// не должна блокироваться дольше разумного на потенциально полумёртвом
// соединении — после таймаута попытка просто засчитывается неудачной и
// повторяется (тот же исход, что для офлайн-машины).
const writeTimeout = 10 * time.Second

// machineQueueCapacity — глубина буферизованного канала записей одной
// машины (см. godoc пакета про воркер на integration_id). Ограничивает
// память на сильно отставшую машину; при заполнении канала диспетчер Run
// временно блокируется на отправке в ЭТОТ канал (и только в него — другие
// машины это не затрагивает, см. godoc Run), что для MVP — осознанно
// принятый компромисс простоты вместо неограниченной очереди.
const machineQueueCapacity = 256

// busConsumer — узкий интерфейс на двух хуках *bus.Consumer, нужных мосту:
// PollFetches (чтение, тикет 3.4) и CommitRecords (commit-after-ack, уже
// существовавший хук тикета 3.2). Сужение нужно для юнит-тестов — позволяет
// подменить консьюмера фейком без поднятия Redpanda; интеграционные тесты
// используют настоящий *bus.Consumer (он удовлетворяет интерфейсу).
type busConsumer interface {
	PollFetches(ctx context.Context) kgo.Fetches
	CommitRecords(ctx context.Context, recs ...*kgo.Record) error
}

// ConnRegistry — узкий интерфейс поиска активного WS-соединения машины по
// integration_id (тикет 3.4). Реализуется *api.Server.MachineConn — реестр
// Server.machineConns заведён тикетом 2.4, и ADR 0002 прямо предусматривает
// его переиспользование мостом оркестратора в качестве источника «активное
// WS для integration_id» (см. godoc пакета). Bridge намеренно НЕ
// импортирует пакет api: зависимость идёт только от orchestrator/main.go,
// которая связывает Bridge с конкретным *api.Server через этот интерфейс.
type ConnRegistry interface {
	// MachineConn возвращает активное WS-соединение машины integrationID,
	// если оно сейчас есть. ok==false — машина офлайн прямо сейчас.
	MachineConn(integrationID uuid.UUID) (*websocket.Conn, bool)
}

// Option — функциональная опция New (по аналогии с agent/internal/wsclient.Option).
type Option func(*Bridge)

// WithLogger задаёт логгер моста (по умолчанию — slog.Default()).
func WithLogger(logger *slog.Logger) Option {
	return func(b *Bridge) {
		if logger != nil {
			b.logger = logger
		}
	}
}

// WithAckTimeout задаёт таймаут ожидания ack на одну попытку доставки (см.
// defaultAckTimeout). Неположительное значение игнорируется (остаётся
// дефолт) — нужен в первую очередь тестам, чтобы не ждать реальные 30s.
func WithAckTimeout(d time.Duration) Option {
	return func(b *Bridge) {
		if d > 0 {
			b.ackTimeout = d
		}
	}
}

// WithOfflineRetryInterval задаёт паузу между попытками найти активное
// соединение офлайн-машины (см. defaultOfflineRetryInterval). Неположительное
// значение игнорируется — нужен тестам, чтобы не ждать реальные секунды.
func WithOfflineRetryInterval(d time.Duration) Option {
	return func(b *Bridge) {
		if d > 0 {
			b.offlineRetryInterval = d
		}
	}
}

// Bridge — мост machine.commands → WS машины с commit-after-ACK (см. godoc
// пакета). Собирается через New; нулевое значение не готово к использованию
// (нет consumer/conns). Run запускает цикл моста; HandleAck — точка входа
// со стороны WS (api.AckSink, см. orchestrator/internal/api/machine_ws.go).
type Bridge struct {
	consumer busConsumer
	conns    ConnRegistry
	logger   *slog.Logger

	ackTimeout           time.Duration
	offlineRetryInterval time.Duration

	// pending — ожидающие ack попытки доставки: message_id команды → канал,
	// закрываемый HandleAck при получении соответствующего ack. Под pendingMu.
	pendingMu sync.Mutex
	pending   map[string]chan struct{}
}

// New собирает Bridge поверх консьюмера Redpanda и реестра WS-соединений
// машин (см. godoc пакета). consumer и conns обязательны (nil — ошибка
// конструктора, не паника). Тип параметра consumer — узкий интерфейс
// busConsumer (а не конкретный *bus.Consumer): в проде передаётся настоящий
// *bus.Consumer (он ему удовлетворяет), в юнит-тестах — fakeConsumer, без
// поднятия Redpanda (см. bridge_test.go).
func New(consumer busConsumer, conns ConnRegistry, opts ...Option) (*Bridge, error) {
	if consumer == nil {
		return nil, fmt.Errorf("bridge: nil consumer")
	}
	if conns == nil {
		return nil, fmt.Errorf("bridge: nil ConnRegistry")
	}
	b := &Bridge{
		consumer:             consumer,
		conns:                conns,
		logger:               slog.Default(),
		ackTimeout:           defaultAckTimeout,
		offlineRetryInterval: defaultOfflineRetryInterval,
		pending:              make(map[string]chan struct{}),
	}
	for _, opt := range opts {
		opt(b)
	}
	return b, nil
}

// Run — основной цикл моста (см. godoc пакета): опрашивает Redpanda и
// маршрутизирует каждую запись в долгоживущий воркер её машины (ключ записи
// — integration_id, ADR 0001), создавая воркер лениво при первой записи.
// Возвращает управление только при отмене ctx (тогда — nil, штатное
// завершение) либо при неустранимой ошибке poll (тогда — ошибка). Перед
// возвратом дожидается завершения всех воркеров (они сами останавливаются
// по отмене ctx).
func (b *Bridge) Run(ctx context.Context) error {
	workers := make(map[string]chan *kgo.Record)
	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		fetches := b.consumer.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("bridge: poll fetches: %w", errs[0].Err)
		}

		fetches.EachRecord(func(rec *kgo.Record) {
			key := string(rec.Key)
			ch, ok := workers[key]
			if !ok {
				ch = make(chan *kgo.Record, machineQueueCapacity)
				workers[key] = ch
				wg.Add(1)
				go func() {
					defer wg.Done()
					b.machineWorker(ctx, ch)
				}()
			}
			select {
			case ch <- rec:
			case <-ctx.Done():
			}
		})
	}
}

// machineWorker обрабатывает записи ОДНОЙ машины строго последовательно
// (порядок команд машине, §126), пока не отменён ctx или не закрыт канал.
func (b *Bridge) machineWorker(ctx context.Context, ch <-chan *kgo.Record) {
	for {
		select {
		case <-ctx.Done():
			return
		case rec, ok := <-ch:
			if !ok {
				return
			}
			b.deliver(ctx, rec)
		}
	}
}

// deliver доставляет ОДНУ запись-команду до её машины с ретраями до ack
// (commit-after-ack, protocol.md §5, см. godoc пакета): пока машина офлайн —
// ждёт и повторяет поиск соединения; как только соединение нашлось — пишет
// конверт в WS и ждёт ack с совпадающим message_id не дольше ackTimeout;
// получен ack — коммитит offset записи и завершает; таймаут — повторяет
// доставку (тот же конверт, тот же message_id — допустимый дубль,
// гасится дедупом агента по message_id, §5); отмена ctx — завершает без
// коммита (запись будет вычитана заново при следующем запуске моста).
func (b *Bridge) deliver(ctx context.Context, rec *kgo.Record) {
	env, err := bus.Unmarshal(rec.Value)
	if err != nil {
		// Невалидный конверт команды: доставлять нечего и некому. Коммитим,
		// чтобы битая запись не блокировала машину навсегда (тот же принцип,
		// что Consumer.applyRecord для machine.events, ADR 0001).
		b.logWarn("bridge: невалидный конверт команды — пропускаю и коммичу offset", "error", err)
		b.commit(ctx, rec)
		return
	}
	integrationID, err := uuid.Parse(env.IntegrationID)
	if err != nil {
		b.logWarn("bridge: integration_id команды не парсится как UUID — пропускаю и коммичу offset",
			"error", err, "message_id", env.MessageID)
		b.commit(ctx, rec)
		return
	}

	for {
		if ctx.Err() != nil {
			return
		}

		conn, ok := b.conns.MachineConn(integrationID)
		if !ok {
			// Машина офлайн: НЕ коммитим, ждём и повторяем (без busy-loop).
			if !b.sleep(ctx, b.offlineRetryInterval) {
				return
			}
			continue
		}

		ackCh := b.registerPending(env.MessageID)

		writeCtx, cancel := context.WithTimeout(ctx, writeTimeout)
		writeErr := conn.Write(writeCtx, websocket.MessageText, rec.Value)
		cancel()
		if writeErr != nil {
			b.unregisterPending(env.MessageID)
			b.logWarn("bridge: запись команды в WS машины не удалась — повторю",
				"error", writeErr, "integration_id", env.IntegrationID, "message_id", env.MessageID)
			if !b.sleep(ctx, b.offlineRetryInterval) {
				return
			}
			continue
		}

		timer := time.NewTimer(b.ackTimeout)
		select {
		case <-ackCh:
			timer.Stop()
			b.commit(ctx, rec)
			// Структурный лог успешной доставки с task_id (тикет 11.4, ТЗ
			// «эксплуатация», FSM-переход уже залогирован отдельно
			// task.Transitioner — здесь именно ФАКТ доставки КОНКРЕТНОЙ команды
			// до машины, что отдельная от смены статуса точка пути задачи).
			// env.TaskID — machine-level команды (маловероятны в этом потоке,
			// см. годок пакета bridge про источник machine.commands) не имеют
			// task_id — тогда пропускаем поле, а не логируем пустую строку.
			if env.TaskID != nil {
				b.logger.Info("bridge: команда доставлена на машину",
					slog.String("task_id", *env.TaskID),
					slog.String("integration_id", env.IntegrationID),
					slog.String("type", env.Type),
				)
			}
			return
		case <-timer.C:
			b.unregisterPending(env.MessageID)
			// Нет ack за отведённое время — переотправка (protocol.md §5).
			continue
		case <-ctx.Done():
			timer.Stop()
			b.unregisterPending(env.MessageID)
			return
		}
	}
}

// commit коммитит offset записи (commit-after-ack). Ошибка коммита
// логируется (если ctx ещё жив — отмена ctx во время shutdown не ошибка) —
// мост не падает: при следующем запуске запись просто будет переобработана
// (at-least-once это допускает).
func (b *Bridge) commit(ctx context.Context, rec *kgo.Record) {
	if err := b.consumer.CommitRecords(ctx, rec); err != nil && ctx.Err() == nil {
		b.logWarn("bridge: commit offset не удался", "error", err)
	}
}

// sleep ждёт d, прерываясь немедленно при отмене ctx. Возвращает false, если
// ожидание прервано отменой ctx (вызывающий должен завершиться, не повторять).
func (b *Bridge) sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// registerPending заводит ожидание ack для messageID и возвращает канал,
// закрываемый HandleAck при его получении (см. godoc поля pending).
func (b *Bridge) registerPending(messageID string) chan struct{} {
	ch := make(chan struct{})
	b.pendingMu.Lock()
	b.pending[messageID] = ch
	b.pendingMu.Unlock()
	return ch
}

// unregisterPending снимает ожидание ack для messageID (таймаут/отмена —
// ack для ЭТОЙ попытки больше не интересен; см. deliver).
func (b *Bridge) unregisterPending(messageID string) {
	b.pendingMu.Lock()
	delete(b.pending, messageID)
	b.pendingMu.Unlock()
}

// HandleAck — реализация api.AckSink (тикет 3.4): сигнализирует о получении
// ack с данным ack_message_id. Неизвестный message_id (нет ожидающей записи
// — например, ack пришёл уже после того, как deliver сам ушёл в повтор по
// таймауту) и повторный ack (HandleAck для уже снятого с ожидания
// message_id) безопасно игнорируются — никогда не паникует (at-least-once,
// protocol.md §5, дубли ack — штатная ситуация).
func (b *Bridge) HandleAck(ackMessageID string) {
	b.pendingMu.Lock()
	ch, ok := b.pending[ackMessageID]
	if ok {
		delete(b.pending, ackMessageID)
	}
	b.pendingMu.Unlock()
	if !ok {
		return
	}
	close(ch)
}

// logWarn пишет предупреждение в лог моста (логгер задаётся через
// WithLogger, по умолчанию slog.Default() — никогда не nil).
func (b *Bridge) logWarn(msg string, args ...any) {
	b.logger.Warn(msg, args...)
}
