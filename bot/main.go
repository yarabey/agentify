// Package main — точка входа Telegram-бота.
//
// Назначение (бизнес): бот — тонкий адаптер канала Telegram (FR D1, D3, D4,
// G2): webhook на входящие, привязка аккаунта и доставка уведомлений из
// топика notifications.telegram. Вся бизнес-логика остаётся в оркестраторе
// (принцип «единый API»). В тикете 0.6 здесь был реализован общий
// операционный каркас (конфиг из env, slog, /healthz, graceful shutdown);
// тикет 10.1 (эпик 10) добавляет каркас приёма апдейтов через webhook за
// Caddy (`bot.<домен>`): telebot v3, секретный путь `/webhook/<secret>`,
// регистрация webhook при старте (SetWebhook) и базовое логирование апдейта;
// тикет 10.2 добавляет первый реальный хендлер — `/start <code>` (обмен
// одноразового кода на привязку telegram_user_id ↔ user_id, FR D3, см.
// bot/start.go и bot/internal/orchestrator). Действия из Telegram
// (постановка/отмена задачи, ответ на вопрос) — тикет 10.3, consumer
// уведомлений — тикет 10.4.
//
// Как устроено (тех): main — тонкий: грузит конфиг под префиксом BOT_ через
// общий пакет platform, поднимает каркас сервиса (slog + chi /healthz). Если
// задан BOT_TOKEN, создаётся *tele.Bot, регистрируется хендлер `/start`
// (newStartHandler, bot/start.go — работает и без BOT_ORCHESTRATOR_URL,
// см. его godoc про мягкую деградацию) и на общий chi-роутер монтируется
// webhook.Handler по секретному пути (bot/internal/webhook). Если дополнительно
// задан BOT_PUBLIC_URL, webhook регистрируется в Telegram (SetWebhook) при
// старте; без него (dev) сервис поднимается, но webhook не регистрируется — так
// каркас не падает без полной конфигурации. Затем блокируется на svc.Run до
// SIGTERM/SIGINT и гаснет gracefully (поведение из 0.6 сохранено).
package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	tele "gopkg.in/telebot.v3"

	"github.com/yarabey/agentify/bot/internal/orchestrator"
	"github.com/yarabey/agentify/bot/internal/webhook"
	"github.com/yarabey/agentify/internal/platform"
)

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
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "bot: фатальная ошибка:", err)
		os.Exit(1)
	}
}

// run загружает конфиг, собирает каркас сервиса, при наличии токена поднимает
// webhook-приём Telegram и блокируется до остановки.
func run() error {
	var cfg config
	if err := platform.LoadConfig(&cfg, envPrefix); err != nil {
		return err
	}

	svc, err := platform.NewService(serviceName, cfg.Config)
	if err != nil {
		return err
	}

	// Telegram-webhook (тикет 10.1, FR D1) опционален по тем же соображениям,
	// что WS-транспорт в agent/main.go и БД в orchestrator/main.go: пустой
	// BOT_TOKEN — тихо пропускаем всю Telegram-часть, сервис остаётся каркасом
	// 0.6 (только /healthz). Так dev/CI-прогоны без реального токена не падают.
	if cfg.Token == "" {
		svc.Logger().Warn("Telegram-webhook отключён: BOT_TOKEN не задан")
	} else {
		if err := setupWebhook(svc, cfg); err != nil {
			return err
		}
	}

	return svc.Run(context.Background())
}

// setupWebhook создаёт *tele.Bot, монтирует webhook.Handler на chi-роутер
// сервиса по секретному пути и, если задан публичный URL, регистрирует webhook
// в Telegram (SetWebhook). Вызывается только при непустом BOT_TOKEN.
func setupWebhook(svc *platform.Service, cfg config) error {
	// Публичный webhook без секрета небезопасен (адрес угадывается, заголовок не
	// проверяется), поэтому при заданном BOT_PUBLIC_URL пустой BOT_WEBHOOK_SECRET
	// — фатальная ошибка старта, а не тихий запуск с открытым эндпоинтом (тот же
	// принцип «пустой секрет в проде недопустим», что у ORCH_JWT_SIGNING_KEY).
	if cfg.PublicURL != "" && cfg.WebhookSecret == "" {
		return fmt.Errorf("bot: BOT_PUBLIC_URL задан, но BOT_WEBHOOK_SECRET пуст — задайте секрет (см. bot/README.md), публичный webhook без секрета небезопасен")
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
		return fmt.Errorf("bot: не удалось создать telebot-бота (проверьте BOT_TOKEN): %w", err)
	}

	// /start <code> — обмен кода привязки Telegram-аккаунта (тикет 10.2, FR
	// D3, см. bot/start.go). Клиент к оркестратору nil, если
	// BOT_ORCHESTRATOR_URL не задан — обработчик регистрируется всё равно
	// (см. godoc newStartHandler про мягкую деградацию). Действия из Telegram
	// (постановка/отмена задачи, ответ на вопрос) — тикет 10.3, здесь не
	// регистрируются.
	// ВАЖНО: orchClient объявлена именно типом telegramLinker (интерфейс,
	// bot/start.go), а не *orchestrator.Client — иначе неприсвоенный
	// typed-nil-указатель внутри интерфейсного параметра newStartHandler не
	// был бы равен nil (классическая ловушка Go), и проверка client == nil в
	// startReplyText молча перестала бы работать.
	var orchClient telegramLinker
	if cfg.OrchestratorURL != "" {
		orchClient = orchestrator.NewClient(cfg.OrchestratorURL)
	} else {
		svc.Logger().Warn("привязка аккаунта (/start) отключена: BOT_ORCHESTRATOR_URL не задан")
	}
	bot.Handle("/start", newStartHandler(orchClient, svc.Logger()))

	path := webhook.Path(cfg.WebhookSecret)
	svc.Router().Post(path, webhook.NewHandler(bot, cfg.WebhookSecret, svc.Logger()).ServeHTTP)

	// Регистрация webhook в Telegram опциональна: без BOT_PUBLIC_URL (dev) не
	// вызываем SetWebhook — не требуется доступная публичная сеть, и каркас не
	// падает. С публичным URL сообщаем Telegram полный адрес доставки и тот же
	// секрет как secret_token.
	if cfg.PublicURL == "" {
		svc.Logger().Warn("webhook смонтирован, но не зарегистрирован в Telegram: BOT_PUBLIC_URL не задан", "path", path)
		return nil
	}

	publicURL := strings.TrimSuffix(cfg.PublicURL, "/") + path
	if err := bot.SetWebhook(&tele.Webhook{
		SecretToken: cfg.WebhookSecret,
		Endpoint:    &tele.WebhookEndpoint{PublicURL: publicURL},
	}); err != nil {
		return fmt.Errorf("bot: регистрация webhook в Telegram (SetWebhook) не удалась: %w", err)
	}
	svc.Logger().Info("webhook зарегистрирован в Telegram", "public_url", publicURL)
	return nil
}
