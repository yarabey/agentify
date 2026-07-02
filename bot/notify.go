package main

// notify.go — доставка уведомлений в Telegram (тикет 10.4, EPIC 10
// «Telegram-бот», deps: 10.1, 7.3; FR G1, Gherkin §6 «Уведомление в
// Telegram»).
//
// Назначение (бизнес): бот дочитывает топик notifications.telegram
// (публикует оркестратор, orchestrator/internal/notify/telegram, тикет 7.3)
// и отправляет каждое сообщение в соответствующий Telegram-чат. Ровно как
// весь бот — тонкий адаптер (принцип «единый API»): оркестратор УЖЕ резолвил
// привязку канала (channel_links, тикет 10.2) и УЖЕ отформатировал
// человекочитаемый текст (bus.TelegramNotificationPayload.Text) ДО публикации
// в Redpanda — этому файлу не нужен доступ к БД оркестратора и не нужно знать
// про Kind доменного события (agent_question/agent_completed/…), он лишь
// пересылает готовый текст в готовый chat_id через Bot API.
//
// Как устроено (тех): notifyConsumer оборачивает *bus.Consumer (тот же
// consumer/commit-after-success цикл, что и у presence.Consumer,
// orchestrator/internal/presence/consumer.go) поверх consumer group
// "telegram-bot" (ADR 0001, notifyConsumerGroup — bot/main.go) и топика
// notifications.telegram. handle разбирает конверт (bus.Envelope, тот же
// общий контракт, что и у машинного протокола, — см. годок
// bus.MessageTypeTelegramNotification) и шлёт payload.Text через
// telegramSender (узкий интерфейс на *tele.Bot.Send — тот же приём сужения,
// что и у telegramActor/telegramLinker/webhook.UpdateProcessor: позволяет
// юнит-тестам подменить отправку фейком без реальной сети/токена).
//
// Ошибки: конверт не того типа/не парсящийся JSON/невалидный chat_id — это
// ПОВРЕЖДЁННЫЕ данные, не транзиентный сбой; handle логирует и возвращает nil
// (offset коммитится, партия не блокируется навсегда — тот же принцип, что у
// presence.Consumer.handle при неразборчивом integration_id). Ошибка
// bot.Send (сетевая/Bot API) — ТРАНЗИЕНТНАЯ; handle возвращает её ДАЛЬШЕ:
// bus.Consumer.Run прекращает партию БЕЗ коммита (at-least-once, ADR 0001) и
// возвращает ошибку наверх — процесс завершается (тот же принцип
// "crash-and-restart == retry", что и у presence/bridge, см. orchestrator/
// main.go), запись перечитается после перезапуска бота.
import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	tele "gopkg.in/telebot.v3"

	"github.com/yarabey/agentify/internal/bus"
)

// busConsumer — узкий интерфейс на *bus.Consumer.Run, нужный notifyConsumer
// (тот же приём, что и у presence.busConsumer): юнит-тесты подставляют фейк
// без поднятия Redpanda; в проде передаётся настоящий *bus.Consumer.
type busConsumer interface {
	Run(ctx context.Context, handler bus.Handler) error
}

// telegramSender — узкий интерфейс на *tele.Bot.Send, нужный notifyConsumer
// (тот же приём сужения, что и у telegramActor/telegramLinker выше в этом
// пакете): позволяет юнит-тестам подменить отправку фейком без реального
// *tele.Bot/сети. *tele.Bot удовлетворяет ему структурно.
type telegramSender interface {
	Send(to tele.Recipient, what interface{}, opts ...interface{}) (*tele.Message, error)
}

// notifyConsumer — consumer топика notifications.telegram (тикет 10.4, см.
// годок файла). Собирается через newNotifyConsumer; нулевое значение не
// готово к использованию (нет consumer/sender).
type notifyConsumer struct {
	consumer busConsumer
	sender   telegramSender
	logger   *slog.Logger
}

// newNotifyConsumer собирает notifyConsumer поверх консьюмера Redpanda и
// отправителя Telegram. consumer и sender обязательны (nil — тот же принцип
// "nil → паника недопустима", как у presence.NewConsumer/bridge.New, но здесь
// вызывающая сторона (run, bot/main.go) уже гарантирует непустые аргументы
// перед вызовом — конструктор оставлен простым, без отдельной проверки, ради
// единообразия с прочими маленькими конструкторами пакета main этого
// сервиса, ни один из которых (newStartHandler и т.п.) не проверяет свои
// узкие интерфейсные параметры на nil при сборке).
func newNotifyConsumer(consumer busConsumer, sender telegramSender, logger *slog.Logger) *notifyConsumer {
	return &notifyConsumer{consumer: consumer, sender: sender, logger: logger}
}

// Run — тонкая обёртка над bus.Consumer.Run (тот же приём, что и
// presence.Consumer.Run). Возвращает управление только при отмене ctx (nil,
// штатное завершение) либо при неустранимой ошибке консьюмера/отправки (см.
// годок файла про транзиентные ошибки).
func (c *notifyConsumer) Run(ctx context.Context) error {
	return c.consumer.Run(ctx, c.handle)
}

// handle — bus.Handler для notifications.telegram (тикет 10.4, FR G1). См.
// годок файла про различие повреждённых данных (skip) и транзиентных ошибок
// отправки (propagate).
func (c *notifyConsumer) handle(ctx context.Context, env bus.Envelope) error {
	if env.Type != bus.MessageTypeTelegramNotification {
		// Топик notifications.telegram зарезервирован ЦЕЛИКОМ под этот
		// единственный тип (см. годок bus.MessageTypeTelegramNotification) —
		// иной тип не должен здесь появиться штатно, но не блокируем партию
		// из-за возможного будущего/чужого сообщения.
		return nil
	}

	var payload bus.TelegramNotificationPayload
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		c.warn("не удалось разобрать payload уведомления — пропускаю", env, err)
		return nil
	}
	if payload.TelegramChatID == 0 {
		c.warn("уведомление без telegram_chat_id — пропускаю", env, nil)
		return nil
	}
	if payload.Text == "" {
		c.warn("уведомление с пустым текстом — пропускаю", env, nil)
		return nil
	}

	if _, err := c.sender.Send(tele.ChatID(payload.TelegramChatID), payload.Text); err != nil {
		// Транзиентная ошибка (сеть/Bot API) — пробрасываем, см. годок файла.
		return fmt.Errorf("bot: отправка уведомления в Telegram (chat_id=%d): %w", payload.TelegramChatID, err)
	}
	return nil
}

// warn логирует пропуск повреждённого сообщения, если задан logger (nil
// допустим — тот же принцип, что и у остальных логгеров этого проекта).
func (c *notifyConsumer) warn(msg string, env bus.Envelope, err error) {
	if c.logger == nil {
		return
	}
	attrs := []any{slog.String("message_id", env.MessageID)}
	if err != nil {
		attrs = append(attrs, slog.String("error", err.Error()))
	}
	c.logger.Warn("bot: "+msg, attrs...)
}
