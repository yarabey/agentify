package main

// start.go — обработчик команды /start <code> (тикет 10.2, FR D3, Gherkin §6
// «Уведомления» — привязка Telegram-аккаунта, предусловие сценария
// «Уведомление в Telegram»).
//
// Назначение (бизнес): пользователь генерирует одноразовый код привязки в web
// (тикет 9.6, POST /channels/telegram/link-code — здесь НЕ реализуется) и
// пересылает его боту как deep-link (`t.me/<bot>?start=<code>`) либо вручную
// (`/start <code>`) — Telegram доставляет оба варианта одинаково: текстом
// команды с payload после пробела (см. telebot.Context.Args()). Обработчик
// вызывает оркестратор (bot/internal/orchestrator.Client.LinkTelegram) и
// отвечает пользователю понятным текстом: успех, либо конкретная причина
// отказа (код не найден / истёк / уже использован / telegram уже привязан к
// другому аккаунту).
//
// Как устроено (тех): бизнес-логика вынесена в чистую функцию startReplyText
// (без зависимости от telebot.Context) — так она покрывается unit-тестами без
// реального *tele.Bot/сети (см. bot/start_test.go); newStartHandler — тонкая
// telebot-обвязка поверх неё, тот же принцип разделения, что у
// webhook.Handler/UpdateProcessor (bot/internal/webhook). telegramLinker —
// узкий интерфейс на *orchestrator.Client.LinkTelegram, позволяющий тестам
// подменить сетевой клиент фейком. Клиент может быть nil-интерфейсом
// (BOT_ORCHESTRATOR_URL не задан, см. bot/main.go) — тогда startReplyText
// отвечает "функция недоступна" вместо паники, тем же принципом «пустой
// конфиг → мягкая деградация», что и у самого webhook (см. godoc
// bot/main.go run()).
import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"

	tele "gopkg.in/telebot.v3"

	"github.com/yarabey/agentify/bot/internal/orchestrator"
)

// telegramLinker — узкий интерфейс на *orchestrator.Client.LinkTelegram
// (тикет 10.2, FR D3). *orchestrator.Client удовлетворяет ему структурно;
// unit-тесты (bot/start_test.go) подставляют фейк без реальной сети.
type telegramLinker interface {
	LinkTelegram(ctx context.Context, code, telegramUserID string) error
}

// newStartHandler собирает tele.HandlerFunc для команды /start (FR D3).
//
// client — клиент к API оркестратора (bot/internal/orchestrator), может быть
// nil-интерфейсом, если BOT_ORCHESTRATOR_URL не задан (см. bot/main.go: там
// client объявлен переменной ИМЕННО типа telegramLinker, а не
// *orchestrator.Client, — иначе неприсвоенный typed-nil-указатель внутри
// интерфейса не был бы равен nil, классическая ловушка Go); logger — логгер
// сервиса, nil допустим.
func newStartHandler(client telegramLinker, logger *slog.Logger) tele.HandlerFunc {
	return func(c tele.Context) error {
		sender := c.Sender()
		var senderID int64
		hasSender := sender != nil
		if hasSender {
			senderID = sender.ID
		}
		// telebot v3 Context не несёт request-scoped context.Context (webhook
		// обрабатывается синхронно в рамках HTTP-запроса, см. Synchronous:true
		// в setupWebhook, bot/main.go) — используем Background с таймаутом,
		// заданным самим Client (requestTimeout, orchestrator/client.go).
		text := startReplyText(context.Background(), client, c.Args(), senderID, hasSender, logger)
		return c.Send(text)
	}
}

// startReplyText — чистая бизнес-логика /start (FR D3): по аргументам
// команды и отправителю решает, что ответить пользователю, вызывая
// client.LinkTelegram при наличии кода. Вынесена из newStartHandler ради
// unit-тестируемости без реального telebot.Context/сети.
func startReplyText(ctx context.Context, client telegramLinker, args []string, senderID int64, hasSender bool, logger *slog.Logger) string {
	if client == nil {
		// BOT_ORCHESTRATOR_URL не задан (см. bot/main.go) — тот же принцип
		// мягкой деградации, что у отсутствующего BOT_TOKEN/PUBLIC_URL:
		// сервис не падает, просто эта функция недоступна.
		return "Бот временно не настроен для привязки аккаунта. Попробуйте позже."
	}

	if len(args) == 0 || strings.TrimSpace(args[0]) == "" {
		// `/start` без payload — обычный первый запуск бота без deep-link
		// кода, не ошибка: просто приветствие с подсказкой, как привязать
		// аккаунт (код выдаёт web, тикет 9.6).
		return "Привет! Чтобы привязать аккаунт, откройте настройки в web-приложении agentify и перейдите по ссылке привязки Telegram."
	}
	code := strings.TrimSpace(args[0])

	if !hasSender {
		// Апдейт без отправителя (не должно случаться для обычного
		// текстового сообщения, но Sender() может быть nil для некоторых
		// системных апдейтов) — нечего привязывать.
		if logger != nil {
			logger.Warn("/start: апдейт без Sender()")
		}
		return "Не удалось определить ваш Telegram-аккаунт, попробуйте ещё раз."
	}
	telegramUserID := strconv.FormatInt(senderID, 10)

	err := client.LinkTelegram(ctx, code, telegramUserID)
	switch {
	case err == nil:
		return "Аккаунт привязан! Теперь вы будете получать уведомления и сможете ставить задачи из Telegram."
	case errors.Is(err, orchestrator.ErrLinkCodeNotFound):
		return "Код привязки не найден. Проверьте ссылку/код и попробуйте снова."
	case errors.Is(err, orchestrator.ErrLinkCodeExpired):
		return "Код привязки истёк. Сгенерируйте новый код в настройках web-приложения."
	case errors.Is(err, orchestrator.ErrLinkCodeUsed):
		return "Этот код привязки уже использован. Сгенерируйте новый код в настройках web-приложения."
	case errors.Is(err, orchestrator.ErrAlreadyLinked):
		return "Этот Telegram-аккаунт уже привязан к другому пользователю agentify."
	default:
		if logger != nil {
			logger.Error("/start: LinkTelegram", slog.Any("error", err))
		}
		return "Не удалось привязать аккаунт из-за внутренней ошибки. Попробуйте позже."
	}
}
