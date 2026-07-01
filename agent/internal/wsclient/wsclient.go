// Package wsclient — исходящий WS-транспорт агента к оркестратору (тикеты
// 3.3/3.5, docs/protocol.md §1, §2, §4, §5; зеркало серверной стороны
// orchestrator/internal/api/machine_ws.go, тикеты 2.3/2.4, и моста
// orchestrator/internal/bridge, тикет 3.4).
//
// Назначение (бизнес): агент держит исходящее WebSocket-соединение к
// оркестратору (работает за NAT, РЕШЕНИЕ 1 из
// docs/01_tech_stack_and_architecture.md), аутентифицируется первым кадром
// hello (FR B3) и переподключается с экспоненциальным backoff при разрыве
// (бизнес-ТЗ §124, §126: связь асинхронная, оркестратор и Redpanda-мост
// (тикет 3.4) переживают временную недоступность агента, поэтому агенту
// достаточно настойчиво пытаться восстановить «трубу», не теряя данные).
// Надёжность доставки СОБЫТИЙ агент→оркестратор (protocol.md §5, «События
// агент → оркестратор») обеспечивается локальным durable outbox агента
// (тикет 3.5, agent/internal/outbox, bbolt): SendEvent сначала durable
// записывает событие в outbox и только потом пытается отправить его по
// сети; удаляется событие из outbox ТОЛЬКО после ack оркестратора. Если
// оркестратор упал/связь оборвалась ДО ack — событие остаётся в outbox и
// будет переотправлено при следующем подключении (реплей, FR E3, §125) —
// это и есть приёмка тикета 3.5 «убить оркестратор на время → события
// агента не потеряны».
//
// Как устроено (тех): Client.Run — единственный публичный метод жизненного
// цикла: dial → hello → на каждом успешном (пере)подключении запускается
// СЕССИЯ соединения (см. connSession) — пара конкурентных циклов,
// живущих и завершающихся вместе на время жизни ОДНОГО соединения через
// errgroup с общим ctx:
//   - read-loop читает входящие кадры; распознаёт type==ack
//     (bus.MessageTypeAck/bus.AckPayload, зеркало parseAckFrame/
//     handleMachineFrame из machine_ws.go) и сигнализирует send-loop'у
//     через ack-per-message канал (тот же паттерн "pending map[string]chan
//     struct{}", что orchestrator/internal/bridge.Bridge, только зеркально:
//     здесь агент ждёт ack оркестратора на СВОЁ исходящее событие), а также
//     type==task_assigned (FR E1, тикет 5.4) — вызывает
//     Config.OnTaskAssigned и, при успехе, немедленно отвечает ack-кадром
//     (writeAck); бизнес-логика запуска задачи (выбор провайдера, durable
//     постановка task_accepted в outbox) целиком живёт в вызывающем коде
//     (agent/main.go), wsclient её не знает. Любой иной/нераспознанный кадр
//     молча игнорируется (обработка user_answer/command_decision/
//     cancel/ping — EPIC 5.x, вне объёма).
//   - send-loop читает outbox.Pending() и последовательно шлёт каждое
//     событие, ждёт ack именно на его message_id (или разрыва
//     сессии/отмены ctx — БЕЗ отдельного внутреннего таймера "нет ack →
//     переотправка": redelivery происходит строго "при следующем
//     коннекте", protocol.md §5), и только тогда удаляет его из outbox и
//     переходит к следующему pending. Раз общий порядок outbox FIFO
//     глобальный, любой под-порядок по task_id — его подпоследовательность
//     (порядок в рамках задачи, §126, сохраняется как следствие).
//
// При разрыве ЛЮБОГО из двух циклов сессии (или ctx.Done()) — экспоненциальный
// backoff с full jitter (AWS-style, см. backoff.go) и повтор, БЕСКОНЕЧНО,
// пока не отменён переданный ctx. Счётчик попыток (attempt) сбрасывается в
// 0 при каждом успешном dial+hello.
//
// Контрактная совместимость с сервером обеспечивается тем, что обе стороны
// используют общий repo-root пакет internal/bus (bus.Envelope,
// bus.HelloPayload, bus.MessageTypeHello, bus.MessageTypeAck,
// bus.AckPayload) — НЕ копией структур (см. рефакторинг machine_ws.go
// тикета 3.3).
package wsclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"github.com/yarabey/agentify/internal/bus"
)

// Ошибки валидации Config, возвращаемые New (конструктор не паникует,
// см. godoc New).
var (
	// ErrEmptyOrchestratorWSURL — пустой Config.OrchestratorWSURL: транспорту
	// некуда подключаться.
	ErrEmptyOrchestratorWSURL = errors.New("wsclient: пустой OrchestratorWSURL")
	// ErrInvalidIntegrationUUID — Config.IntegrationUUID не парсится как UUID
	// (FR B3: hello-кадр требует синтаксически валидный секрет интеграции).
	ErrInvalidIntegrationUUID = errors.New("wsclient: IntegrationUUID не является валидным UUID")
	// ErrNilOutbox — New вызван с nil Outbox: durability исходящих событий
	// (тикет 3.5, docs/protocol.md §1/§5) — это ядро контракта клиента, а не
	// опциональная возможность, поэтому конструктор требует Outbox всегда, а
	// не через functional option (см. godoc New).
	ErrNilOutbox = errors.New("wsclient: nil Outbox")
)

// helloWriteTimeout — крайний срок отправки hello-кадра после успешного
// dial. Симметрично machineHelloReadTimeout на стороне сервера
// (orchestrator/internal/api/machine_ws.go): агент не должен зависнуть на
// записи дольше разумного, прежде чем счесть попытку подключения неудачной
// и уйти в backoff.
const helloWriteTimeout = 10 * time.Second

// eventWriteTimeout — крайний срок записи ОДНОГО события (протокол §5,
// send-loop connSession) в WS-соединение. Симметрично helloWriteTimeout и
// writeTimeout в orchestrator/internal/bridge: ограничивает только саму
// сетевую запись, НЕ ожидание ack (оно намеренно неограничено внутри одной
// сессии — см. godoc пакета про "нет ack → переотправка только при
// следующем коннекте").
const eventWriteTimeout = 10 * time.Second

// ackWriteTimeout — крайний срок отправки ack-кадра в ответ на успешно
// обработанный task_assigned (тикет 5.4, зеркало machineEventAckWriteTimeout
// в orchestrator/internal/api/machine_ws.go). Отдельная константа от
// helloWriteTimeout/eventWriteTimeout — те ограничивают исходящий
// hello/событие, эта — исходящий ack на входящую команду; значение то же
// (10s), но семантически это другая операция на другом направлении обмена.
const ackWriteTimeout = 10 * time.Second

// Config — параметры подключения агента к оркестратору (docs/protocol.md
// §1, §4 "hello").
type Config struct {
	// OrchestratorWSURL — полный WS-адрес эндпоинта оркестратора
	// (ws://.../machine/ws или wss://.../machine/ws в проде, см.
	// docs/protocol.md §8 "WS только поверх TLS"). Обязателен.
	OrchestratorWSURL string

	// IntegrationUUID — секрет интеграции (plaintext UUID), выданный
	// владельцу при создании интеграции (POST /integrations, тикет 2.2) и
	// настроенный на машине агента (FR B2). Отправляется в payload hello
	// (bus.HelloPayload.UUID); сервер ищет интеграцию по HMAC-отпечатку
	// (FR B6) — сам секрет агент больше нигде не передаёт. Обязателен и
	// должен парситься как UUID (см. New).
	IntegrationUUID string

	// AgentVersion — версия бинаря агента, переносится в hello
	// (bus.HelloPayload.AgentVersion); полноценная сверка версии/протокола —
	// FR C5, тикет 4.7.
	AgentVersion string

	// Providers — список провайдеров, доступных этому агенту (claude,
	// claude-code, ...; EPIC 4.5), переносится в hello
	// (bus.HelloPayload.Providers).
	Providers []string

	// OnTaskAssigned — колбэк входящей команды task_assigned (тикет 5.4, FR
	// E1). Вызывается синхронно из read-loop сессии СРАЗУ по получении
	// кадра — реализация ОБЯЗАНА вернуться быстро (не блокировать на
	// выполнении самой задачи; долгую работу — в отдельной горутине) и
	// передать всю бизнес-логику (выбор провайдера, запуск, постановку
	// task_accepted в outbox) наружу, в agent/main.go — wsclient остаётся
	// чисто транспортным пакетом (см. годок пакета). Возвращает ошибку
	// ТОЛЬКО если задачу не удалось даже НАЧАТЬ (невалидный payload, нет
	// сконфигурированного провайдера) — в этом случае wsclient НЕ шлёт ack
	// (агент повторит попытку через redelivery моста, тот же at-least-once
	// принцип, что и везде в протоколе); ошибки уже ЗАПУЩЕННОГО выполнения
	// задачи (сбой самого провайдера) сюда не относятся — они вне объёма
	// этого тикета (тикет 5.8). Пусто (nil) → task_assigned молча
	// игнорируется, как и любой нераспознанный кадр (обратная совместимость
	// с тикетом 3.5), но логируется предупреждением (см. handleFrame) —
	// отличимо от штатного игнорирования прочих типов.
	OnTaskAssigned func(ctx context.Context, env bus.Envelope) error
}

// validate проверяет обязательные поля Config (см. godoc New).
func (c Config) validate() error {
	if strings.TrimSpace(c.OrchestratorWSURL) == "" {
		return ErrEmptyOrchestratorWSURL
	}
	if _, err := uuid.Parse(c.IntegrationUUID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidIntegrationUUID, err)
	}
	return nil
}

// Outbox — узкий интерфейс durable-очереди исходящих событий агента (тикет
// 3.5; agent/internal/outbox.Store реализует его структурно). Сужение — тот
// же приём, что busConsumer/ConnRegistry в orchestrator/internal/bridge
// (тикет 3.4): позволяет тестировать wsclient через fake-outbox в памяти
// (wsclient_test.go), не поднимая bbolt-файл на диске в каждом тесте, и не
// тащит зависимость от bbolt в сам пакет wsclient — её видит только
// agent/main.go, которая связывает Client с конкретным *outbox.Store. Close
// сюда намеренно не входит: временем жизни Store управляет вызывающий код
// (main.go: defer store.Close()), а не Client.
type Outbox interface {
	// Enqueue durable-записывает событие (см. godoc SendEvent).
	Enqueue(env bus.Envelope) error
	// Pending возвращает неподтверждённые события в порядке добавления
	// (FIFO) — send-loop шлёт их именно в этом порядке (см. godoc пакета).
	Pending() ([]bus.Envelope, error)
	// Delete убирает событие по message_id после ack; неизвестный
	// message_id — безопасный no-op (at-least-once, дубли/поздние ack).
	Delete(messageID string) error
}

// Client — WS-клиент агента (см. godoc пакета). Собирается через New;
// нулевое значение не готово к использованию.
type Client struct {
	cfg    Config
	outbox Outbox

	logger *slog.Logger

	backoffBase time.Duration
	backoffMax  time.Duration

	// notify сигнализирует send-loop'у текущей сессии, что в outbox
	// появилась новая работа (SendEvent). Буфер 1 и неблокирующая отправка
	// (см. SendEvent) — вызывающий SendEvent никогда не ждёт реальной
	// доставки/ack, только durable-записи в outbox (см. godoc SendEvent).
	// Живёт на весь срок жизни Client (переживает реконнекты) — send-loop
	// новой сессии в любом случае начинает с полного дренажа Pending(), так
	// что сигнал, "потерянный" между сессиями, не теряет данные, только
	// возможную лишнюю пустую итерацию.
	notify chan struct{}
}

// Option — функциональная опция New, по аналогии с конструкторами остальных
// сервисов пакета (platform.NewService и т.п.), но без жёсткой связи с ними:
// нужна в первую очередь тестам wsclient_test.go, чтобы подставить быстрый
// backoff (миллисекунды) вместо реальных секунд/минут.
type Option func(*Client)

// WithLogger задаёт логгер клиента (по умолчанию — slog.Default()).
// integration_id логируется (диагностика реконнектов), сам UUID-секрет —
// никогда (та же дисциплина, что в orchestrator/internal/api/machine_ws.go).
func WithLogger(logger *slog.Logger) Option {
	return func(c *Client) {
		if logger != nil {
			c.logger = logger
		}
	}
}

// WithBackoff задаёт базовую и максимальную задержку экспоненциального
// backoff с full jitter (см. backoff.go). Нулевые/отрицательные значения
// игнорируются (остаётся дефолт). Тесты используют это, чтобы не ждать
// реальные секунды между попытками реконнекта.
func WithBackoff(base, max time.Duration) Option {
	return func(c *Client) {
		if base > 0 {
			c.backoffBase = base
		}
		if max > 0 {
			c.backoffMax = max
		}
	}
}

// New собирает Client, валидируя Config (см. godoc Config/validate) и
// требуя ненулевой outbox. outbox — обязательный параметр конструктора, а
// не Option: durability исходящих событий (тикет 3.5) — часть базового
// контракта клиента (protocol.md §1 «локальный durable outbox на агенте»),
// а не опциональная надстройка, поэтому её нельзя молча забыть подключить
// (в отличие от, например, WithLogger). Возвращает ошибку конструктора
// (ErrEmptyOrchestratorWSURL / ErrInvalidIntegrationUUID / ErrNilOutbox), не
// паникует — невалидный конфиг агента не должен ронять процесс
// необработанной паникой.
func New(cfg Config, outbox Outbox, opts ...Option) (*Client, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if outbox == nil {
		return nil, ErrNilOutbox
	}
	c := &Client{
		cfg:         cfg,
		outbox:      outbox,
		logger:      slog.Default(),
		backoffBase: defaultBackoffBase,
		backoffMax:  defaultBackoffMax,
		notify:      make(chan struct{}, 1),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

// SendEvent — универсальный примитив отправки события агент→оркестратор
// (protocol.md §4 "Агент → оркестратор", §5 "События агент → оркестратор").
// Если env.MessageID пуст — генерирует его через bus.NewMessageID(). Сначала
// (и это ключевая гарантия) durable-записывает конверт в outbox — ДО любой
// попытки сетевой отправки: физическая запись в bbolt-файл переживает
// падение процесса агента и/или оркестратора, поэтому именно она, а не
// успех WS-записи, и есть точка "событие больше не потеряется". Затем
// неблокирующе будит send-loop текущей сессии (если она есть) — но НЕ ждёт
// реальной доставки/ack, это забота фонового цикла (см. godoc пакета,
// connSession.sendLoop); вызывающий получает управление обратно сразу после
// успешной durable-записи.
//
// Возвращает ошибку, только если сама durable-запись (outbox.Enqueue) не
// удалась — в этом случае событие нигде не сохранено и вызывающий должен
// решить сам (повторить / отчитаться об ошибке выше).
func (c *Client) SendEvent(ctx context.Context, env bus.Envelope) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if env.MessageID == "" {
		env.MessageID = bus.NewMessageID()
	}
	if err := c.outbox.Enqueue(env); err != nil {
		return fmt.Errorf("wsclient: durable-запись события message_id=%s в outbox: %w", env.MessageID, err)
	}
	select {
	case c.notify <- struct{}{}:
	default:
		// Уже есть неподобранный сигнал — send-loop и так перечитает Pending
		// целиком на следующей итерации, второй сигнал не добавляет ничего.
	}
	return nil
}

// Run — основной цикл клиента (см. godoc пакета): dial → hello → сессия
// соединения (read-loop + send-loop, см. connSession), при разрыве —
// backoff и повтор, бесконечно, пока не отменён ctx. Run возвращает
// управление ТОЛЬКО когда ctx отменён (тогда возвращает ctx.Err()) —
// сетевые ошибки сами по себе цикл не останавливают, только провоцируют
// backoff и следующую попытку.
//
// Отзывчивость на отмену ctx обеспечена на каждой блокирующей точке: dial
// (websocket.Dial принимает ctx), запись hello (conn.Write с ctx с
// таймаутом), backoff-пауза (select на ctx.Done()/time.After) и чтение
// (conn.Read с ctx — coder/websocket прерывает блокирующее чтение при
// отмене ctx, см. godoc websocket.Conn.Read).
func (c *Client) Run(ctx context.Context) error {
	attempt := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		conn, err := c.connect(ctx)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			c.logger.Warn("не удалось подключиться к оркестратору",
				slog.Int("attempt", attempt),
				slog.String("error", err.Error()),
			)
			if sleepErr := c.sleepBackoff(ctx, attempt); sleepErr != nil {
				return sleepErr
			}
			attempt++
			continue
		}

		// Успешное подключение+hello: счётчик попыток сбрасывается (см.
		// godoc пакета).
		attempt = 0
		c.logger.Info("WS-соединение к оркестратору установлено",
			slog.String("integration_id", c.cfg.IntegrationUUID),
		)

		sessErr := c.runSession(ctx, conn)
		_ = conn.Close(websocket.StatusNormalClosure, "")

		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		c.logger.Warn("WS-соединение с оркестратором разорвано, переподключение",
			slog.String("error", sessErr.Error()),
		)
		if sleepErr := c.sleepBackoff(ctx, attempt); sleepErr != nil {
			return sleepErr
		}
		attempt++
	}
}

// runSession запускает read-loop и send-loop ОДНОЙ WS-сессии (см. godoc
// пакета, connSession) конкурентно через errgroup с общим дочерним ctx:
// если любой из циклов завершается с ошибкой (или отменяется исходный ctx),
// errgroup отменяет ctx сессии, из-за чего второй цикл тоже разблокируется
// и завершается — оба гарантированно останавливаются вместе. Возвращает
// первую полученную ошибку (errgroup.Wait), по которой Run решает про
// backoff/реконнект.
func (c *Client) runSession(ctx context.Context, conn *websocket.Conn) error {
	sess := &connSession{
		c:       c,
		conn:    conn,
		pending: make(map[string]chan struct{}),
	}

	g, sessCtx := errgroup.WithContext(ctx)
	g.Go(func() error { return sess.readLoop(sessCtx) })
	g.Go(func() error { return sess.sendLoop(sessCtx) })
	return g.Wait()
}

// connect выполняет один dial + отправку hello-кадра. При ошибке на любом
// шаге закрывает уже установленное (если было) соединение и возвращает
// ошибку — вызывающий (Run) уходит в backoff.
func (c *Client) connect(ctx context.Context) (*websocket.Conn, error) {
	conn, _, err := websocket.Dial(ctx, c.cfg.OrchestratorWSURL, nil)
	if err != nil {
		return nil, fmt.Errorf("wsclient: dial: %w", err)
	}

	if err := c.sendHello(ctx, conn); err != nil {
		_ = conn.Close(websocket.StatusInternalError, "hello failed")
		return nil, err
	}

	return conn, nil
}

// sendHello отправляет первый кадр hello (docs/protocol.md §2, §4) —
// зеркало payload'а, который разбирает
// orchestrator/internal/api.authenticateMachineHello.
func (c *Client) sendHello(ctx context.Context, conn *websocket.Conn) error {
	writeCtx, cancel := context.WithTimeout(ctx, helloWriteTimeout)
	defer cancel()

	payload, err := json.Marshal(bus.HelloPayload{
		UUID:         c.cfg.IntegrationUUID,
		AgentVersion: c.cfg.AgentVersion,
		Providers:    c.cfg.Providers,
	})
	if err != nil {
		return fmt.Errorf("wsclient: маршалинг hello payload: %w", err)
	}

	env := bus.Envelope{
		MessageID:       bus.NewMessageID(),
		TaskID:          nil, // machine-level сообщение (protocol.md §2)
		IntegrationID:   c.cfg.IntegrationUUID,
		Type:            bus.MessageTypeHello,
		Ts:              time.Now().UTC().Format(time.RFC3339),
		ProtocolVersion: bus.ProtocolVersion,
		Payload:         payload,
	}
	data, err := env.Marshal()
	if err != nil {
		return fmt.Errorf("wsclient: маршалинг hello-конверта: %w", err)
	}

	if err := conn.Write(writeCtx, websocket.MessageText, data); err != nil {
		return fmt.Errorf("wsclient: запись hello-кадра: %w", err)
	}
	return nil
}

// connSession — состояние ОДНОГО WS-соединения (см. godoc пакета): живёт от
// успешного hello до разрыва, координирует read-loop (принимающий ack) и
// send-loop (шлющий pending-события из outbox) через pending — тот же
// паттерн "message_id → канал, закрываемый при получении ack", что
// orchestrator/internal/bridge.Bridge (тикет 3.4), только зеркально: там
// мост ждал ack агента на СВОЮ исходящую команду, здесь — агент ждёт ack
// оркестратора на СВОЁ исходящее событие. pending и его мьютекс скопом
// ограничены одной сессией намеренно: неподтверждённые попытки предыдущей
// (разорванной) сессии не имеют смысла — событие всё равно осталось в
// outbox и будет заново подобрано send-loop'ом новой сессии (redelivery
// "при следующем коннекте", protocol.md §5).
type connSession struct {
	c    *Client
	conn *websocket.Conn

	pendingMu sync.Mutex
	pending   map[string]chan struct{}
}

// readLoop читает входящие кадры до ошибки/закрытия соединения. Распознаёт
// type==ack и type==task_assigned (см. handleFrame) — любой иной или
// нераспознанный кадр молча игнорируется (обработка
// user_answer/command_decision/cancel/ping — вне объёма тикета 5.4, EPIC
// 5.x; тот же принцип, что handleMachineFrame в
// orchestrator/internal/api/machine_ws.go).
func (s *connSession) readLoop(ctx context.Context) error {
	for {
		_, data, err := s.conn.Read(ctx)
		if err != nil {
			return err
		}
		s.handleFrame(ctx, data)
	}
}

// handleFrame разбирает один входящий кадр:
//   - type==ack (protocol.md §4/§5) сигнализирует ожидающему
//     sendAndAwaitAck (см. registerPending/handleAck);
//   - type==task_assigned (FR E1, тикет 5.4) вызывает
//     Config.OnTaskAssigned; при её отсутствии (nil) или ошибке (задачу не
//     удалось даже начать) ack НЕ отправляется — агент получит редоставку
//     той же команды от моста оркестратора (at-least-once, тот же принцип,
//     что и у остальных путей протокола); при успехе — немедленно отправляет
//     ack-кадр (writeAck).
//   - любой другой/нераспознанный кадр — no-op (см. godoc readLoop).
func (s *connSession) handleFrame(ctx context.Context, data []byte) {
	if ackMessageID, ok := parseAckFrame(data); ok {
		s.handleAck(ackMessageID)
		return
	}

	var env bus.Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return
	}
	if env.Type != bus.MessageTypeTaskAssigned {
		return
	}

	if s.c.cfg.OnTaskAssigned == nil {
		// Хэндлер не сконфигурирован (агент без wiring провайдера в
		// main.go, либо намеренно) — штатно игнорируем, как и любой
		// нераспознанный кадр (см. godoc Config.OnTaskAssigned), но
		// логируем предупреждением, чтобы это было отличимо от штатного
		// игнорирования прочих типов. Содержимое задачи (env.Payload) в лог
		// НЕ попадает — та же дисциплина, что и в отношении кредов/секретов.
		s.c.logger.Warn("task_assigned получен, но OnTaskAssigned не настроен — кадр проигнорирован",
			slog.String("integration_id", s.c.cfg.IntegrationUUID),
			slog.String("message_id", env.MessageID),
		)
		return
	}

	if err := s.c.cfg.OnTaskAssigned(ctx, env); err != nil {
		s.c.logger.Warn("OnTaskAssigned вернул ошибку — ack не отправлен, ожидаем redelivery",
			slog.String("integration_id", s.c.cfg.IntegrationUUID),
			slog.String("message_id", env.MessageID),
			slog.String("error", err.Error()),
		)
		return
	}

	if err := s.writeAck(ctx, env.MessageID); err != nil {
		s.c.logger.Warn("запись ack-кадра task_assigned в WS",
			slog.String("integration_id", s.c.cfg.IntegrationUUID),
			slog.String("message_id", env.MessageID),
			slog.String("error", err.Error()),
		)
	}
}

// writeAck строит и отправляет ack-конверт (protocol.md §4/§5,
// bus.AckPayload) в ответ на успешно обработанный входящий кадр-команду
// (сейчас — только task_assigned, тикет 5.4) — зеркало writeMachineAck из
// orchestrator/internal/api/machine_ws.go, только с IntegrationID,
// заполненным секретом интеграции (c.cfg.IntegrationUUID), а не DB id: у
// агента, в отличие от оркестратора, нет доступа к DB id своей интеграции —
// он и не нужен, оркестратор всё равно не доверяет присланному в конверте
// значению (см. handleMachineEvent на серверной стороне).
func (s *connSession) writeAck(ctx context.Context, ackMessageID string) error {
	ack := bus.Envelope{
		MessageID:       bus.NewMessageID(),
		IntegrationID:   s.c.cfg.IntegrationUUID,
		Type:            bus.MessageTypeAck,
		Ts:              time.Now().UTC().Format(time.RFC3339),
		ProtocolVersion: bus.ProtocolVersion,
	}
	payload, err := json.Marshal(bus.AckPayload{AckMessageID: ackMessageID})
	if err != nil {
		return fmt.Errorf("wsclient: маршалинг AckPayload: %w", err)
	}
	ack.Payload = payload

	data, err := ack.Marshal()
	if err != nil {
		return fmt.Errorf("wsclient: маршалинг ack-конверта: %w", err)
	}

	writeCtx, cancel := context.WithTimeout(ctx, ackWriteTimeout)
	defer cancel()
	if err := s.conn.Write(writeCtx, websocket.MessageText, data); err != nil {
		return fmt.Errorf("wsclient: запись ack-кадра: %w", err)
	}
	return nil
}

// parseAckFrame пытается разобрать сырой WS-кадр как конверт type==ack
// (protocol.md §4/§5, bus.AckPayload). Возвращает (ack_message_id, true) при
// успехе; ("", false) для любого иного случая (кадр не JSON, конверт
// другого типа, payload без ack_message_id) — зеркало одноимённой функции
// orchestrator/internal/api/machine_ws.go (серверная сторона того же
// протокола), продублированное здесь намеренно: readLoop разбирает кадры
// оркестратор→агент (ack на события), а не наоборот, так что общий код
// пришлось бы либо экспортировать из api (нежелательная зависимость
// agent→orchestrator), либо тащить в bus (лишняя логика в транспортном
// пакете) — простое дублирование пяти строк дешевле любого из этого.
func parseAckFrame(data []byte) (string, bool) {
	var env bus.Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return "", false
	}
	if env.Type != bus.MessageTypeAck {
		return "", false
	}
	var payload bus.AckPayload
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		return "", false
	}
	if payload.AckMessageID == "" {
		return "", false
	}
	return payload.AckMessageID, true
}

// registerPending заводит ожидание ack для messageID и возвращает канал,
// закрываемый handleAck при его получении.
func (s *connSession) registerPending(messageID string) chan struct{} {
	ch := make(chan struct{})
	s.pendingMu.Lock()
	s.pending[messageID] = ch
	s.pendingMu.Unlock()
	return ch
}

// unregisterPending снимает ожидание ack для messageID (сессия
// завершается/разрывается — ack для ЭТОЙ попытки в рамках этой сессии
// больше не интересен, см. godoc connSession).
func (s *connSession) unregisterPending(messageID string) {
	s.pendingMu.Lock()
	delete(s.pending, messageID)
	s.pendingMu.Unlock()
}

// handleAck сигнализирует о получении ack с данным message_id. Неизвестный
// message_id (нет ожидающей записи в ЭТОЙ сессии) безопасно игнорируется —
// никогда не паникует (at-least-once, дубли/поздние ack — штатная
// ситуация).
func (s *connSession) handleAck(messageID string) {
	s.pendingMu.Lock()
	ch, ok := s.pending[messageID]
	if ok {
		delete(s.pending, messageID)
	}
	s.pendingMu.Unlock()
	if !ok {
		return
	}
	close(ch)
}

// sendLoop — send-loop сессии (см. godoc пакета): на старте и после каждого
// пробуждения по notify дренирует outbox.Pending() целиком (drainPending),
// затем блокируется до следующего сигнала SendEvent или отмены ctx.
// Немедленный дренаж на старте (без ожидания первого notify) — то, что
// обеспечивает redelivery "при следующем коннекте" (protocol.md §5): всё,
// что осталось в outbox от предыдущей (разорванной) сессии или пережило
// рестарт процесса агента (тикет 3.5), будет отправлено сразу, как только
// появилось новое соединение, а не только по новому вызову SendEvent.
func (s *connSession) sendLoop(ctx context.Context) error {
	for {
		if err := s.drainPending(ctx); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.c.notify:
		}
	}
}

// drainPending последовательно отправляет ВСЕ события, которые
// outbox.Pending() считает неподтверждёнными, в порядке FIFO — раз этот
// порядок глобальный, любой под-порядок в рамках одного task_id является
// его подпоследовательностью, поэтому порядок задачи (§126) сохраняется
// автоматически, без отдельной группировки по task_id. Каждое событие
// отправляется через sendAndAwaitAck; ошибка (обрыв соединения, отмена ctx)
// прерывает дренаж — событие, на котором это случилось, остаётся в outbox
// (Delete не вызывался) и будет подобрано заново следующей сессией.
func (s *connSession) drainPending(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		pending, err := s.c.outbox.Pending()
		if err != nil {
			return fmt.Errorf("wsclient: outbox.Pending: %w", err)
		}
		if len(pending) == 0 {
			return nil
		}
		if err := s.sendAndAwaitAck(ctx, pending[0]); err != nil {
			return err
		}
	}
}

// sendAndAwaitAck отправляет ОДНО событие и ждёт ack именно на его
// message_id (или разрыва сессии/отмены ctx — см. godoc пакета про
// отсутствие отдельного внутреннего таймера). Получен ack — событие
// удаляется из outbox (protocol.md §5, шаг 3 "Агент получил ack → удаляет
// из outbox") и вызывающий (drainPending) переходит к следующему pending;
// ЛЮБАЯ иная развязка (ошибка записи, разрыв сессии, отмена ctx) НЕ
// удаляет событие — оно останется в outbox и будет переотправлено при
// следующем подключении.
func (s *connSession) sendAndAwaitAck(ctx context.Context, env bus.Envelope) error {
	data, err := env.Marshal()
	if err != nil {
		// Невалидный конверт в outbox не должен возникать (Enqueue уже
		// валидирует через тот же Marshal), но если это всё-таки случилось —
		// такое событие никогда не станет валидным сколько ни повторяй,
		// поэтому его нужно снять с очереди, а не блокировать им всю
		// доставку остальных событий навсегда.
		s.c.logger.Warn("wsclient: невалидный конверт в outbox — удаляю и пропускаю",
			slog.String("message_id", env.MessageID),
			slog.String("error", err.Error()),
		)
		return s.c.outbox.Delete(env.MessageID)
	}

	ackCh := s.registerPending(env.MessageID)

	writeCtx, cancel := context.WithTimeout(ctx, eventWriteTimeout)
	writeErr := s.conn.Write(writeCtx, websocket.MessageText, data)
	cancel()
	if writeErr != nil {
		s.unregisterPending(env.MessageID)
		return fmt.Errorf("wsclient: запись события message_id=%s: %w", env.MessageID, writeErr)
	}

	select {
	case <-ackCh:
		if err := s.c.outbox.Delete(env.MessageID); err != nil {
			return fmt.Errorf("wsclient: outbox.Delete после ack message_id=%s: %w", env.MessageID, err)
		}
		return nil
	case <-ctx.Done():
		s.unregisterPending(env.MessageID)
		return ctx.Err()
	}
}

// sleepBackoff ждёт backoff-задержку для данной попытки (см. backoff.go),
// прерываясь немедленно при отмене ctx.
func (c *Client) sleepBackoff(ctx context.Context, attempt int) error {
	delay := backoffDelay(attempt, c.backoffBase, c.backoffMax)
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
