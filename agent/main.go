// Package main — точка входа агента-демона на машине пользователя.
//
// Назначение (бизнес): агент — Go-демон, который ставится на машину
// пользователя одной командой (FR C1, C5), держит исходящий WebSocket к
// оркестратору, локальный durable outbox и запускает провайдеров Claude /
// Claude Code. В compose не входит — распространяется как бинарь через
// GoReleaser. В тикете 0.6 здесь реализован общий операционный каркас (конфиг
// из env, slog, /healthz, graceful shutdown); тикет 3.3 добавляет WS-транспорт
// к оркестратору (agent/internal/wsclient) — dial, hello-аутентификация,
// авто-реконнект с backoff (docs/protocol.md §1, §4, бизнес-ТЗ §124, §126);
// тикет 3.5 добавляет локальный durable outbox (agent/internal/outbox, bbolt)
// — durability исходящих событий агент→оркестратор переживает и рестарт
// агента, и временную недоступность оркестратора (docs/protocol.md §5).
// Провайдеры — EPIC 4. /healthz и graceful shutdown нужны агенту уже сейчас
// для демонизации (systemd/launchd, FR C5) и проверок живости.
//
// Как устроено (тех): main — тонкий: грузит конфиг под префиксом AGENT_ через
// общий пакет platform, поднимает каркас сервиса (slog + chi /healthz) и
// (если задан AGENT_ORCHESTRATOR_WS_URL) открывает outbox.Store по пути
// AGENT_OUTBOX_PATH и WS-клиент к оркестратору поверх него, запускает оба
// конкурентно на общем сигнал-чувствительном ctx через errgroup и
// блокируется до SIGTERM/SIGINT, после чего оба гасятся: HTTP-сервер —
// gracefully (как и раньше), WS-клиент — по отмене ctx (см. wsclient.Run);
// outbox.Store закрывается defer'ом ПОСЛЕ остановки обеих горутин (см. run).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/yarabey/agentify/agent/internal/outbox"
	"github.com/yarabey/agentify/agent/internal/wsclient"
	"github.com/yarabey/agentify/internal/bus"
	"github.com/yarabey/agentify/internal/platform"
)

// serviceName — каноническое имя сервиса в логах и в теле /healthz.
const serviceName = "agent"

// envPrefix — префикс env-переменных агента; на машине пользователя коллизий с
// другими сервисами нет, но единый стиль упрощает конфигурацию и доку.
const envPrefix = "AGENT_"

// version — версия бинаря агента, переносится в hello-кадр
// (bus.HelloPayload.AgentVersion, docs/protocol.md §4). Полноценное
// версионирование сборки (ldflags из CI/GoReleaser) — тикеты 4.1/4.7; здесь —
// заглушка, чтобы hello уже сейчас нёс осмысленное (хоть и статичное)
// значение, а не пустую строку.
var version = "dev"

// config — конфиг агента: общий операционный базис (platform.Config) плюс
// специфичные поля WS-транспорта (тикет 3.3) под тем же префиксом AGENT_.
type config struct {
	platform.Config

	// OrchestratorWSURL — полный WS-адрес /machine/ws оркестратора
	// (ws://.../machine/ws или wss://.../machine/ws в проде). Переменная
	// AGENT_ORCHESTRATOR_WS_URL. Пустое значение (дефолт) означает «WS-транспорт
	// отключён» — агент работает только с /healthz, как в тикете 0.6 (удобно
	// для каркасных прогонов/CI без оркестратора); см. run.
	OrchestratorWSURL string `env:"ORCHESTRATOR_WS_URL"`

	// IntegrationUUID — секрет интеграции (plaintext UUID), выданный при
	// создании интеграции в оркестраторе (POST /integrations, тикет 2.2) и
	// настроенный на машине агента (FR B2). Переменная
	// AGENT_INTEGRATION_UUID. Обязателен, если задан OrchestratorWSURL (см.
	// run) — пустой/невалидный UUID при включённом WS-транспорте делает
	// аутентификацию (FR B3) невозможной.
	IntegrationUUID string `env:"INTEGRATION_UUID"`

	// Providers — список провайдеров, доступных этому агенту (claude,
	// claude-code, ...; EPIC 4.5), переносится в hello
	// (bus.HelloPayload.Providers). Переменная AGENT_PROVIDERS,
	// comma-separated (например "claude,claude-code"). Пустой список
	// допустим (агент ещё не настроил ни одного провайдера).
	Providers []string `env:"PROVIDERS" envSeparator:","`

	// OutboxPath — путь к файлу локального durable outbox (тикет 3.5,
	// agent/internal/outbox, docs/protocol.md §1/§5). Переменная
	// AGENT_OUTBOX_PATH. Дефолт "agent-outbox.db" — файл в рабочей
	// директории процесса; для прод-эксплуатации (systemd/launchd, FR C5)
	// стоит указывать абсолютный путь в персистентную директорию данных
	// агента. Открывается ТОЛЬКО если задан OrchestratorWSURL (см. run) — при
	// выключенном WS-транспорте outbox’у нечего доставлять, поэтому нет
	// смысла заводить файл на диске.
	OutboxPath string `env:"OUTBOX_PATH" envDefault:"agent-outbox.db"`

	// HeartbeatInterval — пауза между heartbeat-событиями агент→оркестратор
	// (тикет 3.6, FR B4, docs/protocol.md §6: HEARTBEAT_INTERVAL). Переменная
	// AGENT_HEARTBEAT_INTERVAL, дефолт 15s — как зафиксировано протоколом.
	// Используется, только если задан OrchestratorWSURL (см. run) — без
	// WS-транспорта слать heartbeat некуда.
	HeartbeatInterval time.Duration `env:"HEARTBEAT_INTERVAL" envDefault:"15s"`

	// ClaudeAPIKey — креды провайдера Claude (Anthropic API, тикет 9.7),
	// записываются интерактивной настройкой (agent/setup.go, тикет 4.3, FR
	// C2/C4) в локальный конфиг-файл 0600 и подхватываются отсюда через
	// AGENT_CLAUDE_API_KEY; оркестратору не передаются. Пусто, если провайдер
	// "claude" не выбран. Использование в проверке команд провайдера —
	// тикет 4.5 (пока поле только хранится).
	ClaudeAPIKey string `env:"CLAUDE_API_KEY"`

	// ClaudeCodeAPIKey — креды провайдера Claude Code (тикет 4.5), аналогично
	// ClaudeAPIKey. Переменная AGENT_CLAUDE_CODE_API_KEY.
	ClaudeCodeAPIKey string `env:"CLAUDE_CODE_API_KEY"`
}

func main() {
	// install.sh (тикет 4.2) делает финальную самопроверку установки через
	// `agentify-agent --version` — обрабатываем это ДО run(), т.к. вывод
	// версии не требует конфига/env и должен работать независимо от
	// platform.LoadConfig (на чистой системе AGENT_* переменных ещё нет).
	if len(os.Args) > 1 && (os.Args[1] == "--version" || os.Args[1] == "-v") {
		fmt.Println(version)
		return
	}
	// `agentify-agent setup` — интерактивная фаза настройки (тикет 4.3, FR
	// C2), запускается пользователем после install.sh (тикет 4.2) и ДО
	// первого запуска демона run() — она сама не поднимает WS-транспорт,
	// только спрашивает адрес оркестратора/UUID/провайдера и пишет конфиг
	// (agent/setup.go).
	if len(os.Args) > 1 && os.Args[1] == "setup" {
		if err := runSetup(os.Stdin, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "agent: setup завершился с ошибкой:", err)
			os.Exit(1)
		}
		return
	}
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "agent: фатальная ошибка:", err)
		os.Exit(1)
	}
}

// run загружает конфиг, собирает каркас сервиса и WS-клиент (если включён) и
// блокируется до остановки.
func run() error {
	var cfg config
	if err := platform.LoadConfig(&cfg, envPrefix); err != nil {
		return err
	}

	svc, err := platform.NewService(serviceName, cfg.Config)
	if err != nil {
		return err
	}

	// Общий сигнал-чувствительный ctx создаётся ЗДЕСЬ, ДО запуска svc.Run, и
	// передаётся обеим горутинам (HTTP-сервер, WS-клиент) через errgroup —
	// иначе у них были бы два независимых, не связанных друг с другом
	// обработчика SIGTERM/SIGINT (Service.Run сам оборачивает переданный ctx в
	// собственный signal.NotifyContext, см. internal/platform/service.go,
	// поэтому достаточно один раз отменить общий родительский ctx, чтобы оба
	// узнали о сигнале остановки).
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return svc.Run(gctx)
	})

	// WS-транспорт (тикет 3.3) + его durable outbox (тикет 3.5): опциональны
	// вместе. Пустой AGENT_ORCHESTRATOR_WS_URL — тихо пропускаем обе фичи
	// (агент работает как в тикете 0.6, только /healthz; открывать файл
	// outbox’а незачем — доставлять всё равно некуда), тот же паттерн, что в
	// orchestrator/main.go для ORCH_DATABASE_URL. Если URL задан,
	// IntegrationUUID становится обязательным — wsclient.New сам валидирует
	// его непустоту и формат UUID (FR B3); провал валидации здесь —
	// фатальная ошибка старта, а не тихий запуск без аутентификации.
	if cfg.OrchestratorWSURL == "" {
		svc.Logger().Warn("WS-транспорт отключён: AGENT_ORCHESTRATOR_WS_URL не задан")
	} else {
		// outbox.Open — ДО конструирования wsclient (durability исходящих
		// событий обязательна конструктору wsclient.New, см. её godoc).
		// defer store.Close() исполнится при возврате из run(), то есть уже
		// ПОСЛЕ g.Wait() ниже — оба конкурентных цикла (HTTP-сервер,
		// WS-клиент) успевают полностью остановиться до закрытия файла.
		store, err := outbox.Open(cfg.OutboxPath)
		if err != nil {
			return fmt.Errorf("agent: не удалось открыть outbox по AGENT_OUTBOX_PATH=%q: %w", cfg.OutboxPath, err)
		}
		defer func() { _ = store.Close() }()

		wsClient, err := wsclient.New(wsclient.Config{
			OrchestratorWSURL: cfg.OrchestratorWSURL,
			IntegrationUUID:   cfg.IntegrationUUID,
			AgentVersion:      version,
			Providers:         cfg.Providers,
		}, store, wsclient.WithLogger(svc.Logger()))
		if err != nil {
			return fmt.Errorf("agent: AGENT_ORCHESTRATOR_WS_URL задан, но конфиг WS-клиента невалиден — задайте корректный AGENT_INTEGRATION_UUID (UUID, выданный при создании интеграции, см. docs/MANUAL_STEPS.md и POST /integrations, тикет 2.2): %w", err)
		}

		g.Go(func() error {
			return wsClient.Run(gctx)
		})

		// Heartbeat-цикл (тикет 3.6, FR B4, docs/protocol.md §6): каждые
		// HeartbeatInterval шлёт machine-level событие type=="heartbeat" через
		// тот же durable-путь (wsclient.Client.SendEvent → outbox → WS), что и
		// остальные события агент→оркестратор — героя не выделяем: SendEvent
		// сам переживает и временную недоступность оркестратора (durable
		// outbox), и падение самого агента (bbolt переживает рестарт). Именно
		// эти heartbeat-конверты дальше публикует presence.Sink в
		// machine.events, откуда их читает presence.Consumer
		// (orchestrator/internal/presence) и помечает интеграцию online.
		g.Go(func() error {
			return runHeartbeatLoop(gctx, wsClient, cfg.IntegrationUUID, cfg.HeartbeatInterval, svc.Logger())
		})
	}

	// errgroup.Wait возвращает первую реальную ошибку любой из горутин;
	// context.Canceled при штатном shutdown (сигнал/отмена ctx) — не ошибка,
	// а ожидаемый путь остановки обеих горутин, поэтому отфильтровывается (тот
	// же принцип, что graceful shutdown в internal/platform/service.go).
	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

// eventSender — узкий интерфейс той части wsclient.Client, что нужна
// heartbeat-циклу (только SendEvent), — сужение позволяет юнит-тестам
// подменить отправку фейком, не поднимая реальный WS/outbox (тот же приём,
// что и в orchestrator/internal/presence для busProducer/busConsumer).
type eventSender interface {
	SendEvent(ctx context.Context, env bus.Envelope) error
}

// runHeartbeatLoop — основной цикл отправки heartbeat (тикет 3.6, FR B4,
// docs/protocol.md §6): каждые interval шлёт machine-level событие
// type=="heartbeat" (task_id == nil) через sender.SendEvent. Первый heartbeat
// уходит сразу при старте (не через interval) — иначе интеграция выглядела
// бы offline первые HeartbeatInterval секунд после каждого запуска/реконнекта
// агента без веской причины. Возвращает управление только при отмене ctx
// (nil, штатное завершение, тот же принцип, что presence.OfflineWorker.Run)
// — ошибка отдельной отправки логируется и не останавливает цикл (durable
// outbox сам переживает временный сбой; агент, падающий из-за отдельного
// неуспешного heartbeat, был бы явно избыточной реакцией).
func runHeartbeatLoop(ctx context.Context, sender eventSender, integrationUUID string, interval time.Duration, logger *slog.Logger) error {
	sendHeartbeat(ctx, sender, integrationUUID, logger)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			sendHeartbeat(ctx, sender, integrationUUID, logger)
		}
	}
}

// sendHeartbeat собирает и отправляет один heartbeat-конверт (protocol.md
// §2, §6). Ошибку SendEvent (только сбой durable-записи в outbox, см. её
// godoc) не пробрасывает выше — героическая ретрай-логика не нужна: следующий
// тик runHeartbeatLoop попробует снова.
func sendHeartbeat(ctx context.Context, sender eventSender, integrationUUID string, logger *slog.Logger) {
	env := bus.Envelope{
		MessageID:       bus.NewMessageID(),
		TaskID:          nil, // machine-level сообщение (protocol.md §2)
		IntegrationID:   integrationUUID,
		Type:            bus.MessageTypeHeartbeat,
		Ts:              time.Now().UTC().Format(time.RFC3339),
		ProtocolVersion: bus.ProtocolVersion,
		Payload:         json.RawMessage("{}"),
	}
	if err := sender.SendEvent(ctx, env); err != nil && ctx.Err() == nil {
		logger.Warn("agent: не удалось поставить heartbeat в outbox", "error", err)
	}
}
