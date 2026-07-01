// Package main — точка входа сервиса-оркестратора.
//
// Назначение (бизнес): оркестратор — ядро системы (FR E*, A*, F*): REST+WS,
// FSM задач, auth, история, мост к Redpanda. В тикете 0.6 здесь реализован лишь
// общий операционный каркас (конфиг из env, slog, /healthz, graceful shutdown);
// бизнес-логика добавляется в EPIC 1/3/5+. Тикет 3.4 добавляет сам мост
// Redpanda → WS (machine.commands → конкретная машина, commit-after-ACK,
// protocol.md §5, см. orchestrator/internal/bridge) — он запускается
// конкурентно с HTTP-сервером, если задан ORCH_REDPANDA_SEEDS.
//
// Как устроено (тех): main — тонкий: грузит конфиг под префиксом ORCH_ через
// общий пакет platform, применяет миграции на старте, при наличии БД поднимает
// pgxpool и монтирует сгенерированный из openapi API-роутер (тикет 1.2: реальный
// POST /auth/register; тикет 1.3: POST /auth/login, /auth/refresh, /auth/logout;
// прочие операции — 501). Если заданы ORCH_DATABASE_URL И ORCH_REDPANDA_SEEDS,
// поднимается ещё и Redpanda-консьюмер моста (bridge.Bridge) — он и
// HTTP-сервер запускаются конкурентно на общем сигнал-чувствительном ctx через
// errgroup (тот же паттерн, что agent/main.go для WS-клиента), и оба гасятся
// при SIGTERM/SIGINT. Пустой ORCH_REDPANDA_SEEDS — мост просто не запускается
// (нефатально, warn-лог) — нужно, чтобы существующие прогоны/тесты без
// Redpanda не ломались. Бинарь поддерживает одну подкоманду —
// `orchestrator bootstrap` (тикет 1.7, см. cmd_bootstrap.go): без аргументов
// запускается обычный сервис (как раньше), с аргументом "bootstrap" —
// идемпотентно создаёт первого администратора и стартовый токен регистрации и
// завершается, не поднимая HTTP.
package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"

	"github.com/yarabey/agentify/internal/bus"
	"github.com/yarabey/agentify/internal/platform"
	"github.com/yarabey/agentify/orchestrator/internal/api"
	"github.com/yarabey/agentify/orchestrator/internal/bridge"
	"github.com/yarabey/agentify/orchestrator/internal/db"
	"github.com/yarabey/agentify/orchestrator/internal/migrate"
	"github.com/yarabey/agentify/orchestrator/internal/presence"
	"github.com/yarabey/agentify/orchestrator/internal/task"
	"github.com/yarabey/agentify/orchestrator/migrations"
)

// bridgeConsumerGroup — имя consumer group моста (тикет 3.4, ADR 0001:
// «мост команд — своя группа (orchestrator-bridge)»). Отдельная от любых
// других consumer group (например, обработчика heartbeat, см.
// heartbeatConsumerGroup), чтобы коммиты моста не задевали офсеты других
// подписчиков той же темы.
const bridgeConsumerGroup = "orchestrator-bridge"

// heartbeatConsumerGroup — имя consumer group presence-консьюмера (тикет 3.6,
// FR B4, ADR 0001). ОБЯЗАНА отличаться от bridgeConsumerGroup — обе группы
// читают разные топики (machine.commands у моста, machine.events у presence),
// но принцип "своя группа на свою роль потребления" общий: раздельные группы
// не делят офсеты и ребалансируются независимо.
const heartbeatConsumerGroup = "orchestrator-heartbeat"

// serviceName — каноническое имя сервиса в логах и в теле /healthz.
const serviceName = "orchestrator"

// envPrefix — префикс env-переменных оркестратора, чтобы три сервиса не
// конфликтовали по именам в общем окружении (docker compose).
const envPrefix = "ORCH_"

// appEncryptionKeyLen — требуемая длина мастер-ключа шифрования после
// base64-декодирования ORCH_APP_ENCRYPTION_KEY: 32 байта (AES-256, см.
// internal/crypto.Encrypt).
const appEncryptionKeyLen = 32

// config — конфиг оркестратора: общий операционный базис плюс специфичные поля
// под тем же префиксом ORCH_ (Redpanda, JWT появятся в следующих тикетах).
type config struct {
	platform.Config

	// DatabaseURL — строка подключения к Postgres (pgx/libpq DSN). Нужна уже в
	// тикете 0.3 для применения goose-миграций на старте; имя переменной —
	// ORCH_DATABASE_URL (префикс ORCH_ как у остального конфига оркестратора).
	// Пустое значение допустимо для запусков без БД (юнит-тесты каркаса): тогда
	// шаг миграций пропускается.
	DatabaseURL string `env:"DATABASE_URL"`

	// JWTSigningKey — секрет HMAC для подписи/проверки access-JWT (тикет 1.3,
	// FR A3). Переменная ORCH_JWT_SIGNING_KEY генерируется ОДНОКРАТНО вручную
	// (`openssl rand -base64 48`, см. docs/MANUAL_STEPS.md) и кладётся в секреты
	// окружения — здесь НЕТ дефолта и НЕ генерируется фолбэк: пустой ключ в
	// проде означал бы предсказуемую подпись токенов доступа, поэтому при
	// поднятом API (DatabaseURL непуст) пустой JWTSigningKey — фатальная ошибка
	// старта (см. run), а не тихий запуск с небезопасным ключом.
	JWTSigningKey string `env:"JWT_SIGNING_KEY"`

	// AppEncryptionKey — base64-кодированный мастер-ключ (32 байта после
	// декодирования) для at-rest шифрования и HMAC-отпечатков (internal/crypto,
	// тикет 2.2, зерно общего крипто-модуля тикета 11.1). Переменная
	// ORCH_APP_ENCRYPTION_KEY генерируется ОДНОКРАТНО вручную (`openssl rand
	// -base64 32`, см. docs/MANUAL_STEPS.md) и кладётся в секреты окружения —
	// как и JWTSigningKey, здесь НЕТ дефолта и НЕ генерируется фолбэк: пустой
	// или предсказуемый мастер-ключ означал бы, что UUID-секреты интеграций
	// (FR B2) можно расшифровать/подделать HMAC без секрета, поэтому при
	// поднятом API пустое/некорректное значение — фатальная ошибка старта
	// (см. run), а не тихий запуск с небезопасным ключом.
	AppEncryptionKey string `env:"APP_ENCRYPTION_KEY"`

	// RedpandaSeeds — адреса брокеров Redpanda (host:port), через запятую.
	// Переменная ORCH_REDPANDA_SEEDS. Пустое значение (дефолт) означает «мост
	// Redpanda → WS отключён» (тикет 3.4) — оркестратор работает как раньше,
	// только REST+WS-handshake, без доставки команд из machine.commands; тот
	// же принцип «пустая опциональная фича — не ошибка старта», что у
	// DatabaseURL/OrchestratorWSURL в остальных конфигах сервисов. Presence-
	// подсистема (тикет 3.6) гейтится ТЕМ ЖЕ условием (см. run) — heartbeat
	// тоже идёт через Redpanda (machine.events).
	RedpandaSeeds []string `env:"REDPANDA_SEEDS" envSeparator:","`

	// OfflineThreshold — порог устаревания last_seen_at, после которого
	// фоновый воркер переводит интеграцию в offline (FR B4, protocol.md §6:
	// OFFLINE_THRESHOLD). Переменная ORCH_OFFLINE_THRESHOLD, дефолт 45s — как
	// зафиксировано протоколом.
	OfflineThreshold time.Duration `env:"OFFLINE_THRESHOLD" envDefault:"45s"`

	// StaleThreshold — порог устаревания last_seen_at интеграции, после
	// которого её активные (running/waiting_user) задачи переводятся в stale с
	// уведомлением (FR E5, protocol.md §6: STALE_THRESHOLD; тикет 5.7).
	// Переменная ORCH_STALE_THRESHOLD. Продуктовое решение об окончательном
	// значении не зафиксировано (docs/MANUAL_STEPS.md §4) — дефолт 120s выбран
	// заметно больше OfflineThreshold (45s), чтобы задача помечалась зависшей
	// уже ПОСЛЕ того, как сама интеграция определённо помечена offline.
	StaleThreshold time.Duration `env:"STALE_THRESHOLD" envDefault:"120s"`
}

func main() {
	// Подкоманда `orchestrator bootstrap` (тикет 1.7) перехватывается ДО
	// обычного запуска сервиса: без аргументов (len(os.Args) == 1) поведение
	// не меняется — стартует сервис, как и раньше. Простого разбора os.Args[1]
	// достаточно для единственной подкоманды MVP — без новой CLI-библиотеки.
	if len(os.Args) > 1 && os.Args[1] == "bootstrap" {
		if err := runBootstrap(); err != nil {
			fmt.Fprintln(os.Stderr, "orchestrator bootstrap: фатальная ошибка:", err)
			os.Exit(1)
		}
		return
	}

	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "orchestrator: фатальная ошибка:", err)
		os.Exit(1)
	}
}

// run загружает конфиг, собирает каркас сервиса и блокируется до остановки.
func run() error {
	var cfg config
	if err := platform.LoadConfig(&cfg, envPrefix); err != nil {
		return err
	}

	svc, err := platform.NewService(serviceName, cfg.Config)
	if err != nil {
		return err
	}

	// Общий сигнал-чувствительный ctx — как в agent/main.go: создаётся ЗДЕСЬ,
	// ДО запуска svc.Run, и передаётся всем горутинам (HTTP-сервер, мост
	// Redpanda) через errgroup. Service.Run сам оборачивает переданный ctx в
	// собственный signal.NotifyContext (internal/platform/service.go), но
	// горутина моста такой обёртки не имеет — ей нужен этот общий ctx, чтобы
	// тоже узнать об отмене по сигналу.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	// Миграции-на-старте (тикет 0.3): прежде чем сервис начнёт отвечать готовым
	// на /healthz, приводим схему БД к актуальной версии встроенными
	// goose-миграциями. Если DATABASE_URL не задан (например, в каркасных
	// прогонах без БД), шаг пропускаем — это инфраструктурная, а не бизнес-часть.
	if cfg.DatabaseURL != "" {
		if err := migrate.Apply(ctx, cfg.DatabaseURL, migrations.FS, svc.Logger()); err != nil {
			return err
		}
	} else {
		svc.Logger().Warn("ORCH_DATABASE_URL пуст — пропускаю применение миграций на старте")
	}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return svc.Run(gctx)
	})

	// Пул соединений и реальный API-сервер (тикеты 1.2/1.3). Без БД API не
	// поднимаем — auth-эндпоинты обращаются к Postgres; остаётся только /healthz
	// из каркаса platform. С БД монтируем сгенерированный из openapi chi-роутер
	// (POST /auth/register, /auth/login, /auth/refresh, /auth/logout +
	// 501-заглушки прочих операций) и отдаём его сервису через SetHandler,
	// сохраняя единый graceful shutdown.
	if cfg.DatabaseURL != "" {
		// Auth-эндпоинты подписывают access-JWT этим ключом (FR A3, тикет 1.3) —
		// пустой ключ означал бы предсказуемую подпись токенов в проде, поэтому
		// падаем на старте, а не поднимаем API с небезопасным дефолтом.
		if cfg.JWTSigningKey == "" {
			return fmt.Errorf("orchestrator: ORCH_JWT_SIGNING_KEY пуст — задайте секрет (см. docs/MANUAL_STEPS.md), пустой/предсказуемый ключ подписи JWT недопустим в проде")
		}

		// Мастер-ключ шифрования (тикет 2.2, internal/crypto): декодируем base64
		// (как сгенерировано `openssl rand -base64 32`) и требуем РОВНО 32 байта
		// после декодирования — иначе тихий запуск с нулевым/коротким/мусорным
		// ключом сделал бы UUID-секреты интеграций расшифровываемыми/подделываемыми.
		if cfg.AppEncryptionKey == "" {
			return fmt.Errorf("orchestrator: ORCH_APP_ENCRYPTION_KEY пуст — задайте секрет (см. docs/MANUAL_STEPS.md), пустой/предсказуемый ключ шифрования недопустим в проде")
		}
		encryptionKey, err := base64.StdEncoding.DecodeString(cfg.AppEncryptionKey)
		if err != nil {
			return fmt.Errorf("orchestrator: ORCH_APP_ENCRYPTION_KEY не является валидным base64 (ожидается вывод `openssl rand -base64 32`): %w", err)
		}
		if len(encryptionKey) != appEncryptionKeyLen {
			return fmt.Errorf("orchestrator: ORCH_APP_ENCRYPTION_KEY после base64-декодирования должен быть длиной %d байт, получено %d", appEncryptionKeyLen, len(encryptionKey))
		}

		pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
		if err != nil {
			return fmt.Errorf("orchestrator: создание пула соединений к Postgres: %w", err)
		}
		// Пул закрываем после возврата Run (т.е. после graceful shutdown HTTP):
		// новые запросы уже не принимаются, активные доиграны.
		defer pool.Close()

		server := api.NewServer(db.New(pool), svc.Logger(), []byte(cfg.JWTSigningKey), encryptionKey)
		server.SetTransitioner(task.NewTransitioner(pool))
		svc.SetHandler(api.NewRouter(server))

		// Мост Redpanda → WS (тикет 3.4, protocol.md §5): опционален, как и
		// WS-транспорт агента в agent/main.go. Нужен и сервер (источник
		// ConnRegistry — server.MachineConn, ADR 0002), и Redpanda-seeds;
		// пустой ORCH_REDPANDA_SEEDS — тихо пропускаем фичу (оркестратор
		// продолжает обслуживать REST+WS-handshake без доставки команд).
		if len(cfg.RedpandaSeeds) == 0 {
			svc.Logger().Warn("мост Redpanda → WS отключён: ORCH_REDPANDA_SEEDS не задан")
		} else {
			consumer, err := bus.NewConsumer(bus.ConsumerConfig{
				Seeds:  cfg.RedpandaSeeds,
				Group:  bridgeConsumerGroup,
				Topics: []string{bus.TopicMachineCommands},
			})
			if err != nil {
				return fmt.Errorf("orchestrator: ORCH_REDPANDA_SEEDS задан, но создание Redpanda-консьюмера моста не удалось: %w", err)
			}
			// Закрываем консьюмера при остановке: после graceful shutdown
			// HTTP/моста новые poll/commit уже не нужны.
			defer consumer.Close()

			brg, err := bridge.New(consumer, server, bridge.WithLogger(svc.Logger()))
			if err != nil {
				return fmt.Errorf("orchestrator: сборка моста Redpanda → WS: %w", err)
			}
			server.SetAckSink(brg)

			g.Go(func() error {
				return brg.Run(gctx)
			})

			// Presence-подсистема (тикет 3.6, FR B4, protocol.md §6): агент
			// шлёт heartbeat в machine.events (WS → GetMachineWs →
			// EventSink.HandleEvent → сюда), Sink публикует его дальше в
			// Redpanda, отдельный consumer группы heartbeatConsumerGroup читает
			// machine.events и помечает интеграцию online, а OfflineWorker
			// независимо от этого фонового чтения переводит в offline
			// интеграции с устаревшим last_seen_at. Гейтится тем же условием
			// ORCH_REDPANDA_SEEDS != "", что и мост выше — heartbeat тоже идёт
			// через Redpanda.
			producer, err := bus.NewProducer(cfg.RedpandaSeeds)
			if err != nil {
				return fmt.Errorf("orchestrator: ORCH_REDPANDA_SEEDS задан, но создание Redpanda-продьюсера presence не удалось: %w", err)
			}
			defer producer.Close()

			// Тот же producer instance переиспользуется как CommandPublisher
			// постановки задач (тикет 5.3, FR E1, machine.commands) — Producer
			// потокобезопасен (godoc internal/bus/producer.go), отдельный
			// экземпляр не нужен.
			server.SetCommandPublisher(producer)

			sink, err := presence.NewSink(producer)
			if err != nil {
				return fmt.Errorf("orchestrator: сборка presence.Sink: %w", err)
			}
			server.SetEventSink(sink)

			heartbeatConsumer, err := bus.NewConsumer(bus.ConsumerConfig{
				Seeds:  cfg.RedpandaSeeds,
				Group:  heartbeatConsumerGroup,
				Topics: []string{bus.TopicMachineEvents},
			})
			if err != nil {
				return fmt.Errorf("orchestrator: ORCH_REDPANDA_SEEDS задан, но создание Redpanda-консьюмера presence не удалось: %w", err)
			}
			defer heartbeatConsumer.Close()

			presenceConsumer, err := presence.NewConsumer(heartbeatConsumer, db.New(pool))
			if err != nil {
				return fmt.Errorf("orchestrator: сборка presence.Consumer: %w", err)
			}
			g.Go(func() error {
				return presenceConsumer.Run(gctx)
			})

			offlineWorker, err := presence.NewOfflineWorker(db.New(pool), presence.WithOfflineThreshold(cfg.OfflineThreshold))
			if err != nil {
				return fmt.Errorf("orchestrator: сборка presence.OfflineWorker: %w", err)
			}
			g.Go(func() error {
				return offlineWorker.Run(gctx)
			})

			// «Зависание» машины (тикет 5.7, FR E5, protocol.md §6): StaleWorker
			// читает ТУ ЖЕ integrations.last_seen_at, что и OfflineWorker выше, но
			// со своим (заметно бОльшим) порогом STALE_THRESHOLD и независимо от
			// integrations.status — гейтится тем же условием ORCH_REDPANDA_SEEDS,
			// потому что last_seen_at в принципе обновляется только через
			// heartbeat-consumer, который сам гейтится этим условием.
			staleWorker, err := task.NewStaleWorker(task.NewTransitioner(pool), db.New(pool), task.WithStaleThreshold(cfg.StaleThreshold))
			if err != nil {
				return fmt.Errorf("orchestrator: сборка task.StaleWorker: %w", err)
			}
			g.Go(func() error {
				return staleWorker.Run(gctx)
			})
		}
	} else if len(cfg.RedpandaSeeds) != 0 {
		// Без БД нет server.MachineConn (ConnRegistry моста) — поднять мост
		// нечем. Не фатально (тот же принцип «опциональная фича»), но явно
		// предупреждаем, чтобы не выглядело так, будто мост тихо работает.
		svc.Logger().Warn("ORCH_REDPANDA_SEEDS задан, но ORCH_DATABASE_URL пуст — мост Redpanda → WS не запускается (нужен API-сервер как реестр WS-соединений)")
	}

	// errgroup.Wait возвращает первую реальную ошибку любой из горутин;
	// context.Canceled при штатном shutdown (сигнал/отмена ctx) — не ошибка
	// (тот же принцип, что agent/main.go).
	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}
