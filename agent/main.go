// Package main — точка входа агента-демона на машине пользователя.
//
// Назначение (бизнес): агент — Go-демон, который ставится на машину
// пользователя одной командой (FR C1, C5), держит исходящий WebSocket к
// оркестратору, локальный durable outbox и запускает провайдеров Claude /
// Claude Code. В compose не входит — распространяется как бинарь через
// GoReleaser. В тикете 0.6 здесь реализован общий операционный каркас (конфиг
// из env, slog, /healthz, graceful shutdown); тикет 3.3 добавляет WS-транспорт
// к оркестратору (agent/internal/wsclient) — dial, hello-аутентификация,
// авто-реконнект с backoff (docs/protocol.md §1, §4, бизнес-ТЗ §124, §126).
// Outbox и провайдеры — EPIC 3.5/4. /healthz и graceful shutdown нужны агенту
// уже сейчас для демонизации (systemd/launchd, FR C5) и проверок живости.
//
// Как устроено (тех): main — тонкий: грузит конфиг под префиксом AGENT_ через
// общий пакет platform, поднимает каркас сервиса (slog + chi /healthz) и
// (если задан AGENT_ORCHESTRATOR_WS_URL) WS-клиент к оркестратору, запускает
// оба конкурентно на общем сигнал-чувствительном ctx через errgroup и
// блокируется до SIGTERM/SIGINT, после чего оба гасятся: HTTP-сервер —
// gracefully (как и раньше), WS-клиент — по отмене ctx (см. wsclient.Run).
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/sync/errgroup"

	"github.com/yarabey/agentify/agent/internal/wsclient"
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
}

func main() {
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

	// WS-транспорт (тикет 3.3): опционален. Пустой AGENT_ORCHESTRATOR_WS_URL —
	// тихо пропускаем фичу (агент работает как в тикете 0.6, только /healthz),
	// тот же паттерн, что в orchestrator/main.go для ORCH_DATABASE_URL. Если
	// URL задан, IntegrationUUID становится обязательным — wsclient.New сам
	// валидирует его непустоту и формат UUID (FR B3); провал валидации здесь —
	// фатальная ошибка старта, а не тихий запуск без аутентификации.
	if cfg.OrchestratorWSURL == "" {
		svc.Logger().Warn("WS-транспорт отключён: AGENT_ORCHESTRATOR_WS_URL не задан")
	} else {
		wsClient, err := wsclient.New(wsclient.Config{
			OrchestratorWSURL: cfg.OrchestratorWSURL,
			IntegrationUUID:   cfg.IntegrationUUID,
			AgentVersion:      version,
			Providers:         cfg.Providers,
		}, wsclient.WithLogger(svc.Logger()))
		if err != nil {
			return fmt.Errorf("agent: AGENT_ORCHESTRATOR_WS_URL задан, но конфиг WS-клиента невалиден — задайте корректный AGENT_INTEGRATION_UUID (UUID, выданный при создании интеграции, см. docs/MANUAL_STEPS.md и POST /integrations, тикет 2.2): %w", err)
		}

		g.Go(func() error {
			return wsClient.Run(gctx)
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
