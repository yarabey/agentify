// Package main — точка входа Telegram-бота.
//
// Назначение (бизнес): бот — тонкий адаптер канала Telegram (FR D1, D3, D4,
// G1, G2): webhook на входящие, привязка аккаунта и доставка уведомлений из
// топика notifications.telegram. Вся бизнес-логика остаётся в оркестраторе
// (принцип «единый API»). В тикете 0.6 здесь был реализован общий
// операционный каркас (конфиг из env, slog, /healthz, graceful shutdown);
// тикет 10.1 (эпик 10) добавляет каркас приёма апдейтов через webhook за
// Caddy (`bot.<домен>`): telebot v3, секретный путь `/webhook/<secret>`,
// регистрация webhook при старте (SetWebhook) и базовое логирование апдейта;
// тикет 10.2 добавляет первый реальный хендлер — `/start <code>` (обмен
// одноразового кода на привязку telegram_user_id ↔ user_id, FR D3, см.
// bot/start.go и bot/internal/orchestrator). Тикет 10.3 добавляет действия из
// Telegram от имени привязанного пользователя — постановка задачи обычным
// текстом, явный выбор машины (`/task`), отмена (`/cancel`), ответ на вопрос
// агента (`/answer`), см. bot/task.go. Тикет 10.4 (deps: 10.1, 7.3, FR G1,
// Gherkin §6 «Уведомление в Telegram») добавляет consumer топика
// notifications.telegram → Bot API, см. bot/notify.go: доменное уведомление
// формирует и резолвит оркестратор (orchestrator/internal/notify/telegram,
// тикет 7.3), бот лишь пересылает УЖЕ готовый текст в УЖЕ известный chat_id
// (принцип «единый API» — см. годок bot/notify.go).
//
// Как устроено (тех): main — тонкий: грузит конфиг под префиксом BOT_ через
// общий пакет platform, поднимает каркас сервиса (slog + chi /healthz) и
// конкурентно (errgroup, тот же паттерн, что и orchestrator/main.go) запускает
// HTTP-сервер и (опционально) consumer уведомлений. Если задан BOT_TOKEN,
// создаётся *tele.Bot, регистрируются хендлеры `/start` (newStartHandler,
// bot/start.go), `/task`/`/cancel`/`/answer`/обычный текст
// (newTaskCommandHandler/newCancelHandler/newAnswerHandler/newTaskTextHandler,
// bot/task.go — все работают и без BOT_ORCHESTRATOR_URL/BOT_SERVICE_SECRET,
// см. их godoc про мягкую деградацию) и на общий chi-роутер монтируется
// webhook.Handler по секретному пути (bot/internal/webhook). Если дополнительно
// задан BOT_PUBLIC_URL, webhook регистрируется в Telegram (SetWebhook) при
// старте; без него (dev) сервис поднимается, но webhook не регистрируется — так
// каркас не падает без полной конфигурации. Если ДОПОЛНИТЕЛЬНО заданы BOT_TOKEN
// И BOT_REDPANDA_SEEDS, поднимается ещё и consumer notifications.telegram
// (bot/notify.go, тикет 10.4) — конкурентно с HTTP-сервером, тот же принцип
// «опциональная фича», что и у Redpanda-моста orchestrator/main.go. Всё
// гасится по SIGTERM/SIGINT (поведение из 0.6 сохранено).
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"golang.org/x/sync/errgroup"
	tele "gopkg.in/telebot.v3"

	"github.com/yarabey/agentify/bot/internal/orchestrator"
	"github.com/yarabey/agentify/bot/internal/webhook"
	"github.com/yarabey/agentify/internal/bus"
	"github.com/yarabey/agentify/internal/platform"
)

// notifyConsumerGroup — имя consumer group доставки уведомлений (тикет 10.4).
// ДОСЛОВНО то имя, что уже зафиксировано в docs/adr/0001-redpanda-topics.md,
// раздел «Consumer groups / commit offset»: "бот уведомлений — своя
// (`telegram-bot`)" — используется ради консистентности с документом-
// источником правды (в отличие от имён групп оркестратора,
// bridgeConsumerGroup/heartbeatConsumerGroup в orchestrator/main.go, которые
// ADR явно называет "деталью реализации 3.2/3.4" — здесь имя НЕ деталь, оно
// уже названо в самом ADR).
const notifyConsumerGroup = "telegram-bot"

// serviceName — каноническое имя сервиса в логах и в теле /healthz.
const serviceName = "bot"

// envPrefix — префикс env-переменных бота, чтобы три сервиса не конфликтовали
// по именам в общем окружении (docker compose).
const envPrefix = "BOT_"

// config — конфиг бота: общий операционный базис плюс специфичные поля канала
// Telegram (тикет 10.1) под тем же префиксом BOT_.
type config struct {
	platform.Config

	// Token — токен Telegram-бота, выданный @BotFather (FR D1). Переменная
	// BOT_TOKEN. Секрет: НЕ коммитится, задаётся через окружение/секреты (см.
	// bot/README.md и docs/MANUAL_STEPS.md §3, ключ TELEGRAM_BOT_TOKEN). Пустое
	// значение (дефолт) означает «бот не поднимается» — сервис работает как в
	// тикете 0.6, только /healthz (удобно для dev/CI без реального токена, см.
	// run); без токена ни webhook-хендлер, ни регистрация webhook не имеют смысла.
	Token string `env:"TOKEN"`

	// WebhookSecret — секрет, встраиваемый в путь webhook (`/webhook/<secret>`,
	// см. webhook.Path) и передаваемый Telegram как secret_token (заголовок
	// X-Telegram-Bot-Api-Secret-Token). Делает адрес webhook неугадываемым и
	// отсекает запросы не от Telegram. Переменная BOT_WEBHOOK_SECRET. Секрет: НЕ
	// коммитится. Пустое значение допустимо только в dev (webhook-хендлер тогда
	// монтируется по пути `/webhook/` без проверки заголовка); при заданном
	// BOT_PUBLIC_URL пустой секрет — фатальная ошибка старта (см. run), т.к.
	// публичный webhook без секрета небезопасен.
	WebhookSecret string `env:"WEBHOOK_SECRET"`

	// PublicURL — внешний базовый URL бота за Caddy (например
	// https://bot.<домен>), на который Telegram доставляет апдейты. Переменная
	// BOT_PUBLIC_URL. Пустое значение (дефолт) означает «webhook не
	// регистрируется» — в dev бот работает без исходящего вызова SetWebhook и не
	// требует доступной публичной сети (см. run). В prod задаётся обязательно,
	// иначе Telegram не будет слать апдейты.
	PublicURL string `env:"PUBLIC_URL"`

	// OrchestratorURL — базовый URL API оркестратора (напр.
	// `http://orchestrator:8080` внутри docker compose или `https://api.<домен>`
	// в проде), нужен /start <code> для обмена кода привязки (тикет 10.2, FR D3,
	// см. bot/internal/orchestrator и bot/start.go). Переменная
	// BOT_ORCHESTRATOR_URL. Пустое значение (дефолт) означает «привязка через
	// /start отключена» — обработчик регистрируется всё равно (см.
	// newStartHandler), но отвечает пользователю "функция недоступна" вместо
	// обращения к оркестратору; удобно для dev/CI без поднятого оркестратора,
	// тот же принцип, что у Token/PublicURL выше.
	OrchestratorURL string `env:"ORCHESTRATOR_URL"`

	// ServiceSecret — общий сервисный секрет между ботом и оркестратором
	// (тикет 10.3, FR D1), нужен действиям из Telegram (постановка/отмена
	// задачи, ответ на вопрос, см. bot/task.go) — Client.GetActingToken
	// шлёт его как заголовок X-Bot-Service-Secret в POST
	// /channels/telegram/token. Переменная BOT_SERVICE_SECRET, генерируется
	// ОДНОКРАТНО вручную (`openssl rand -base64 32`, см.
	// docs/MANUAL_STEPS.md) и ОБЯЗАНА совпадать со значением
	// ORCH_BOT_SERVICE_SECRET на стороне оркестратора. Секрет: НЕ
	// коммитится. Пустое значение (дефолт) означает «действия из Telegram
	// отключены» — GetActingToken тогда всегда получит 401 от оркестратора
	// (см. годок SetBotServiceSecret, orchestrator/internal/api/server.go),
	// а newTaskHandler (bot/task.go) отвечает пользователю "функция
	// недоступна" вместо обращения к оркестратору — тот же принцип, что у
	// OrchestratorURL выше.
	ServiceSecret string `env:"SERVICE_SECRET"`

	// RedpandaSeeds — адреса брокеров Redpanda (host:port), через запятую
	// (тот же формат, что и ORCH_REDPANDA_SEEDS, orchestrator/main.go).
	// Переменная BOT_REDPANDA_SEEDS. Нужна ТОЛЬКО доставке уведомлений (тикет
	// 10.4, FR G1, consumer notifications.telegram, см. bot/notify.go) — ни
	// webhook, ни привязка аккаунта, ни действия из Telegram от Redpanda не
	// зависят. Пустое значение (дефолт) означает «доставка уведомлений
	// отключена» — тот же принцип «пустая опциональная фича — не ошибка
	// старта», что и у остальных полей этого конфига/у ORCH_REDPANDA_SEEDS;
	// нужен, чтобы dev/CI-прогоны без Redpanda не падали (см. run).
	RedpandaSeeds []string `env:"REDPANDA_SEEDS" envSeparator:","`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "bot: фатальная ошибка:", err)
		os.Exit(1)
	}
}

// run загружает конфиг, собирает каркас сервиса, при наличии токена поднимает
// webhook-приём Telegram и (при дополнительно заданном BOT_REDPANDA_SEEDS,
// тикет 10.4) consumer уведомлений, затем блокируется до остановки.
//
// Общий сигнал-чувствительный ctx + errgroup — тот же паттерн, что и
// orchestrator/main.go (мост Redpanda → WS) и agent/main.go (WS-клиент):
// HTTP-сервер (svc.Run) и consumer уведомлений (notifyConsumer.Run) гасятся
// вместе по SIGTERM/SIGINT.
func run() error {
	var cfg config
	if err := platform.LoadConfig(&cfg, envPrefix); err != nil {
		return err
	}

	svc, err := platform.NewService(serviceName, cfg.Config)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return svc.Run(gctx)
	})

	// Telegram-webhook (тикет 10.1, FR D1) опционален по тем же соображениям,
	// что WS-транспорт в agent/main.go и БД в orchestrator/main.go: пустой
	// BOT_TOKEN — тихо пропускаем всю Telegram-часть, сервис остаётся каркасом
	// 0.6 (только /healthz). Так dev/CI-прогоны без реального токена не падают.
	var bot *tele.Bot
	if cfg.Token == "" {
		svc.Logger().Warn("Telegram-webhook отключён: BOT_TOKEN не задан")
	} else {
		b, err := setupWebhook(svc, cfg)
		if err != nil {
			return err
		}
		bot = b
	}

	// Доставка уведомлений (тикет 10.4, FR G1, Gherkin §6 «Уведомление в
	// Telegram», см. bot/notify.go): нужен И *tele.Bot (Send уведомления —
	// без BOT_TOKEN нечем слать), И BOT_REDPANDA_SEEDS (источник топика
	// notifications.telegram) — отсутствие любого из двух тихо отключает
	// только эту фичу, тот же принцип «опциональная фича», что и у webhook
	// выше/Redpanda-моста orchestrator/main.go.
	switch {
	case bot == nil:
		svc.Logger().Warn("доставка уведомлений (10.4) отключена: BOT_TOKEN не задан")
	case len(cfg.RedpandaSeeds) == 0:
		svc.Logger().Warn("доставка уведомлений (10.4) отключена: BOT_REDPANDA_SEEDS не задан")
	default:
		consumer, err := bus.NewConsumer(bus.ConsumerConfig{
			Seeds:  cfg.RedpandaSeeds,
			Group:  notifyConsumerGroup,
			Topics: []string{bus.TopicNotificationsTelegram},
		})
		if err != nil {
			return fmt.Errorf("bot: BOT_REDPANDA_SEEDS задан, но создание Redpanda-консьюмера уведомлений не удалось: %w", err)
		}
		defer consumer.Close()

		nc := newNotifyConsumer(consumer, bot, svc.Logger())
		g.Go(func() error {
			return nc.Run(gctx)
		})
	}

	// errgroup.Wait возвращает первую реальную ошибку любой из горутин;
	// context.Canceled при штатном shutdown (сигнал/отмена ctx) — не ошибка
	// (тот же принцип, что orchestrator/main.go/agent/main.go).
	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

// setupWebhook создаёт *tele.Bot, монтирует webhook.Handler на chi-роутер
// сервиса по секретному пути и, если задан публичный URL, регистрирует webhook
// в Telegram (SetWebhook). Вызывается только при непустом BOT_TOKEN. Возвращает
// собранный *tele.Bot — вызывающий (run) переиспользует его для доставки
// уведомлений (тикет 10.4, см. bot/notify.go), один и тот же экземпляр и
// принимает апдейты, и шлёт исходящие сообщения.
func setupWebhook(svc *platform.Service, cfg config) (*tele.Bot, error) {
	// Публичный webhook без секрета небезопасен (адрес угадывается, заголовок не
	// проверяется), поэтому при заданном BOT_PUBLIC_URL пустой BOT_WEBHOOK_SECRET
	// — фатальная ошибка старта, а не тихий запуск с открытым эндпоинтом (тот же
	// принцип «пустой секрет в проде недопустим», что у ORCH_JWT_SIGNING_KEY).
	if cfg.PublicURL != "" && cfg.WebhookSecret == "" {
		return nil, fmt.Errorf("bot: BOT_PUBLIC_URL задан, но BOT_WEBHOOK_SECRET пуст — задайте секрет (см. bot/README.md), публичный webhook без секрета небезопасен")
	}

	// Offline=true в dev (без публичного URL) не даёт telebot делать исходящие
	// вызовы Bot API при создании бота (getMe) — каркас поднимается без сети и
	// без валидного токена. С публичным URL (prod) Offline=false, чтобы
	// getMe/SetWebhook реально работали. Synchronous=true — ProcessUpdate
	// выполняет хендлеры синхронно в рамках HTTP-запроса webhook'а (проще
	// жизненный цикл и предсказуемее для приёмки).
	bot, err := tele.NewBot(tele.Settings{
		Token:       cfg.Token,
		Offline:     cfg.PublicURL == "",
		Synchronous: true,
		OnError: func(err error, _ tele.Context) {
			svc.Logger().Error("telebot: ошибка обработки апдейта", "error", err)
		},
	})
	if err != nil {
		return nil, fmt.Errorf("bot: не удалось создать telebot-бота (проверьте BOT_TOKEN): %w", err)
	}

	// /start <code> — обмен кода привязки Telegram-аккаунта (тикет 10.2, FR
	// D3, см. bot/start.go); постановка/отмена задачи и ответ на вопрос из
	// Telegram — тикет 10.3, FR D1, см. bot/task.go. Оба набора хендлеров
	// используют ОДИН и тот же *orchestrator.Client (единый HTTP-клиент к
	// оркестратору) — nil, если BOT_ORCHESTRATOR_URL не задан, тогда все
	// хендлеры регистрируются всё равно, но отвечают "функция недоступна"
	// вместо обращения к оркестратору (см. godoc newStartHandler/
	// actingTokenOrReplyText про мягкую деградацию).
	// ВАЖНО: orchClient/orchActor объявлены именно интерфейсными типами
	// (telegramLinker, bot/start.go; telegramActor, bot/task.go), а не
	// *orchestrator.Client — иначе неприсвоенный typed-nil-указатель внутри
	// интерфейсного параметра хендлеров не был бы равен nil (классическая
	// ловушка Go), и проверки на nil внутри chistых функций молча перестали
	// бы работать.
	var orchClient telegramLinker
	var orchActor telegramActor
	if cfg.OrchestratorURL != "" {
		client := orchestrator.NewClient(cfg.OrchestratorURL, cfg.ServiceSecret)
		orchClient = client
		orchActor = client
		if cfg.ServiceSecret == "" {
			// Привязка (/start) не требует сервисного секрета (код привязки
			// сам по себе — доказательство права, тикет 10.2), а действия из
			// Telegram (тикет 10.3) требуют — предупреждаем отдельно, не
			// отключая привязку.
			svc.Logger().Warn("действия из Telegram (постановка/отмена задачи, ответ на вопрос) недоступны: BOT_SERVICE_SECRET не задан")
		}
	} else {
		svc.Logger().Warn("привязка аккаунта (/start) и действия из Telegram отключены: BOT_ORCHESTRATOR_URL не задан")
	}
	bot.Handle("/start", newStartHandler(orchClient, svc.Logger()))

	// Действия из Telegram (тикет 10.3, FR D1): постановка задачи обычным
	// текстом (tele.OnText), явный выбор машины (/task), отмена (/cancel),
	// ответ на вопрос агента (/answer) — см. bot/task.go.
	bot.Handle("/task", newTaskCommandHandler(orchActor, svc.Logger()))
	bot.Handle("/cancel", newCancelHandler(orchActor, svc.Logger()))
	bot.Handle("/answer", newAnswerHandler(orchActor, svc.Logger()))
	bot.Handle(tele.OnText, newTaskTextHandler(orchActor, svc.Logger()))

	path := webhook.Path(cfg.WebhookSecret)
	svc.Router().Post(path, webhook.NewHandler(bot, cfg.WebhookSecret, svc.Logger()).ServeHTTP)

	// Регистрация webhook в Telegram опциональна: без BOT_PUBLIC_URL (dev) не
	// вызываем SetWebhook — не требуется доступная публичная сеть, и каркас не
	// падает. С публичным URL сообщаем Telegram полный адрес доставки и тот же
	// секрет как secret_token.
	if cfg.PublicURL == "" {
		svc.Logger().Warn("webhook смонтирован, но не зарегистрирован в Telegram: BOT_PUBLIC_URL не задан", "path", path)
		return bot, nil
	}

	publicURL := strings.TrimSuffix(cfg.PublicURL, "/") + path
	if err := bot.SetWebhook(&tele.Webhook{
		SecretToken: cfg.WebhookSecret,
		Endpoint:    &tele.WebhookEndpoint{PublicURL: publicURL},
	}); err != nil {
		return nil, fmt.Errorf("bot: регистрация webhook в Telegram (SetWebhook) не удалась: %w", err)
	}
	svc.Logger().Info("webhook зарегистрирован в Telegram", "public_url", publicURL)
	return bot, nil
}
