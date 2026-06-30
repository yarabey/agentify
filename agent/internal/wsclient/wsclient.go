// Package wsclient — исходящий WS-транспорт агента к оркестратору (тикет 3.3,
// docs/protocol.md §1, §2, §4; зеркало серверной стороны
// orchestrator/internal/api/machine_ws.go, тикеты 2.3/2.4).
//
// Назначение (бизнес): агент держит исходящее WebSocket-соединение к
// оркестратору (работает за NAT, РЕШЕНИЕ 1 из
// docs/01_tech_stack_and_architecture.md), аутентифицируется первым кадром
// hello (FR B3) и переподключается с экспоненциальным backoff при разрыве
// (бизнес-ТЗ §124, §126: связь асинхронная, оркестратор и Redpanda-мост
// (тикет 3.4) переживают временную недоступность агента, поэтому агенту
// достаточно настойчиво пытаться восстановить «трубу», не теряя данные —
// надёжность доставки команд/событий обеспечивается Redpanda-бэкбоном на
// сервере и локальным durable outbox на агенте (тикет 3.5, bbolt), НЕ этим
// пакетом).
//
// Как устроено (тех): Client.Run — единственный публичный метод
// жизненного цикла: dial → hello → минимальный read-loop (только держит
// соединение читаемым, обработка payload-ов команд — будущие тикеты,
// зеркало комментария в machine_ws.go про read-loop тикета 2.3) → при любой
// ошибке (dial/hello/read) — экспоненциальный backoff с full jitter
// (AWS-style, см. backoff.go) и повтор, БЕСКОНЕЧНО, пока не отменён
// переданный ctx. Счётчик попыток (attempt) сбрасывается в 0 при каждом
// успешном dial+hello — длинная стабильная сессия не должна «помнить»
// долгую историю прошлых обрывов.
//
// Контрактная совместимость с сервером обеспечивается тем, что обе стороны
// используют общий repo-root пакет internal/bus (bus.Envelope,
// bus.HelloPayload, bus.MessageTypeHello) — НЕ копией структур, как было до
// тикета 3.3 на стороне сервера (см. рефакторинг machine_ws.go того же
// тикета).
package wsclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"

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
)

// helloWriteTimeout — крайний срок отправки hello-кадра после успешного
// dial. Симметрично machineHelloReadTimeout на стороне сервера
// (orchestrator/internal/api/machine_ws.go): агент не должен зависнуть на
// записи дольше разумного, прежде чем счесть попытку подключения неудачной
// и уйти в backoff.
const helloWriteTimeout = 10 * time.Second

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

// Client — WS-клиент агента (см. godoc пакета). Собирается через New;
// нулевое значение не готово к использованию.
type Client struct {
	cfg Config

	logger *slog.Logger

	backoffBase time.Duration
	backoffMax  time.Duration
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

// New собирает Client, валидируя Config (см. godoc Config/validate).
// Возвращает ошибку конструктора (ErrEmptyOrchestratorWSURL /
// ErrInvalidIntegrationUUID), не паникует — невалидный конфиг агента не
// должен ронять процесс необработанной паникой.
func New(cfg Config, opts ...Option) (*Client, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	c := &Client{
		cfg:         cfg,
		logger:      slog.Default(),
		backoffBase: defaultBackoffBase,
		backoffMax:  defaultBackoffMax,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

// Run — основной цикл клиента (см. godoc пакета): dial → hello → read-loop,
// при разрыве — backoff и повтор, бесконечно, пока не отменён ctx. Run
// возвращает управление ТОЛЬКО когда ctx отменён (тогда возвращает
// ctx.Err()) — сетевые ошибки сами по себе цикл не останавливают, только
// провоцируют backoff и следующую попытку.
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

		readErr := c.readLoop(ctx, conn)
		_ = conn.Close(websocket.StatusNormalClosure, "")

		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		c.logger.Warn("WS-соединение с оркестратором разорвано, переподключение",
			slog.String("error", readErr.Error()),
		)
		if sleepErr := c.sleepBackoff(ctx, attempt); sleepErr != nil {
			return sleepErr
		}
		attempt++
	}
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

// readLoop — минимальный read-loop после успешного hello: блокируется на
// чтении кадров до ошибки/закрытия соединения, ничего не делая с
// содержимым (полноценная обработка machine.commands — вне объёма тикета
// 3.3, см. godoc пакета и зеркальный комментарий в machine_ws.go). Нужен,
// чтобы соединение не выглядело повисшим и control-фреймы coder/websocket
// обрабатывались штатно.
func (c *Client) readLoop(ctx context.Context, conn *websocket.Conn) error {
	for {
		if _, _, err := conn.Read(ctx); err != nil {
			return err
		}
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
