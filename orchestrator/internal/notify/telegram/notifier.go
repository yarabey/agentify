package telegram

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/yarabey/agentify/internal/bus"
	"github.com/yarabey/agentify/orchestrator/internal/db"
	"github.com/yarabey/agentify/orchestrator/internal/notify"
)

// telegramChannel — значение channel_links.channel для Telegram (единственное
// допустимое по CHECK-констрейнту миграции 00004; тикет 10.2).
const telegramChannel = "telegram"

// busProducer — узкий интерфейс на *bus.Producer.Publish, нужный Notifier (см.
// годок пакета — почему Publish, а не PublishKeyed: bus.Envelope не несёт
// поля user_id, поэтому ключ партиции ADR 0001 вычисляется вызывающим
// напрямую, а не через Envelope.PartitionKey). Сужение — для юнит-тестов (тот
// же приём, что и presence.busProducer): tests подставляют фейк, не поднимая
// Redpanda; в проде передаётся настоящий *bus.Producer (он ему удовлетворяет).
type busProducer interface {
	Publish(ctx context.Context, topic, key string, env bus.Envelope) error
}

// channelLinkFinder — узкий интерфейс sqlc-запроса
// GetChannelLinkByUserAndChannel (тикет 10.2/7.3), нужный Notifier для резолва
// привязки Telegram по user_id (см. годок пакета). Реализуется *db.Queries;
// сужение позволяет юнит-тестам подменить запрос фейком без поднятия Postgres.
type channelLinkFinder interface {
	GetChannelLinkByUserAndChannel(ctx context.Context, arg db.GetChannelLinkByUserAndChannelParams) (db.ChannelLink, error)
}

// taskIntegrationFinder — узкий интерфейс sqlc-запроса GetTaskByIDAndUser,
// нужный Notifier ТОЛЬКО ради поля IntegrationID результата (см. годок пакета
// про Envelope.IntegrationID) — заодно, как побочный эффект, лишний раз
// подтверждает, что TaskID действительно принадлежит UserID уведомления (тот
// же owner-scoped приём, что и у остальных использований GetTaskByIDAndUser,
// FR A4/I3).
type taskIntegrationFinder interface {
	GetTaskByIDAndUser(ctx context.Context, arg db.GetTaskByIDAndUserParams) (db.Task, error)
}

// Notifier — реализация api.Notifier для Telegram-канала (тикет 7.3, FR G1,
// см. годок пакета). Собирается через NewNotifier; нулевое значение не готово
// к использованию (нет producer/queries).
type Notifier struct {
	producer busProducer
	channels channelLinkFinder
	tasks    taskIntegrationFinder
}

// NewNotifier собирает Notifier поверх продьюсера Redpanda и sqlc-запросов.
// producer и queries обязательны (nil — ошибка конструктора, не паника, тот
// же принцип, что и у presence.NewSink/bridge.New).
func NewNotifier(producer *bus.Producer, queries *db.Queries) (*Notifier, error) {
	if producer == nil {
		return nil, fmt.Errorf("telegram: nil producer")
	}
	if queries == nil {
		return nil, fmt.Errorf("telegram: nil queries")
	}
	return &Notifier{producer: producer, channels: queries, tasks: queries}, nil
}

// Notify реализует api.Notifier (тикет 7.3, FR G1, см. годок пакета про три
// шага). Отсутствие активной привязки Telegram у n.UserID — ШТАТНЫЙ исход,
// возвращает nil (best-effort канал, тот же принцип, что и у
// ClientConnHub.Notify при отсутствии открытых WS-соединений): не у каждого
// пользователя обязан быть привязан Telegram. Ошибка возвращается только при
// реальном сбое инфраструктуры (БД/Redpanda недоступны) — вызывающий
// (handleAgentQuestion и т.п., orchestrator/internal/api/machine_ws.go) в
// любом случае лишь логирует её и продолжает штатный поток ack агенту (см.
// годок api.Notifier).
func (n *Notifier) Notify(ctx context.Context, notification notify.Notification) error {
	link, err := n.channels.GetChannelLinkByUserAndChannel(ctx, db.GetChannelLinkByUserAndChannelParams{
		UserID:  notification.UserID,
		Channel: telegramChannel,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil // Telegram не привязан — не ошибка, см. годок метода.
		}
		return fmt.Errorf("telegram: поиск привязки канала: %w", err)
	}

	chatID, err := strconv.ParseInt(link.ExternalID, 10, 64)
	if err != nil {
		// external_id у Telegram-привязки ВСЕГДА десятичный telegram_user_id
		// (тикет 10.2, bot/start.go: strconv.FormatInt(senderID, 10)) — сюда
		// попасть можно только при повреждённых данных, не штатный исход.
		return fmt.Errorf("telegram: external_id привязки %q не является числом: %w", link.ExternalID, err)
	}

	task, err := n.tasks.GetTaskByIDAndUser(ctx, db.GetTaskByIDAndUserParams{
		ID:     notification.TaskID,
		UserID: notification.UserID,
	})
	if err != nil {
		return fmt.Errorf("telegram: получить задачу для конверта уведомления: %w", err)
	}

	taskIDStr := uuid.UUID(notification.TaskID.Bytes).String()
	payload, err := json.Marshal(bus.TelegramNotificationPayload{
		TelegramChatID: chatID,
		Kind:           notification.Kind,
		TaskID:         taskIDStr,
		Text:           formatText(notification),
	})
	if err != nil {
		return fmt.Errorf("telegram: маршалинг payload уведомления: %w", err)
	}

	env := bus.Envelope{
		MessageID:       bus.NewMessageID(),
		TaskID:          &taskIDStr,
		IntegrationID:   uuid.UUID(task.IntegrationID.Bytes).String(),
		Type:            bus.MessageTypeTelegramNotification,
		Ts:              notification.CreatedAt.UTC().Format(time.RFC3339),
		ProtocolVersion: bus.ProtocolVersion,
		Payload:         payload,
	}

	// Ключ партиции — user_id (ADR 0001, docs/protocol.md §3: "уведомления
	// одного пользователя доставляются по порядку") — вычисляется напрямую из
	// notification.UserID, а не через Envelope.PartitionKey (см. годок
	// busProducer, почему).
	key := uuid.UUID(notification.UserID.Bytes).String()
	if err := n.producer.Publish(ctx, bus.TopicNotificationsTelegram, key, env); err != nil {
		return fmt.Errorf("telegram: публикация в %q: %w", bus.TopicNotificationsTelegram, err)
	}
	return nil
}

// formatText формирует готовый человекочитаемый текст уведомления (см. годок
// bus.TelegramNotificationPayload.Text) по Kind доменного события и его сырому
// Payload (тот же RAW payload agent-кадра, что несёт notify.Notification, см.
// orchestrator/internal/notify). Ошибка разбора Payload (не должна случаться —
// Payload уже провалидирован как соответствующий bus.*Payload тип на стороне
// формирования уведомления, см. handleAgentQuestion и т.п.) не прерывает
// доставку: используется общий, менее детальный текст-фолбэк с Kind и
// task_id, лишь бы пользователь ВООБЩЕ узнал о событии.
func formatText(n notify.Notification) string {
	taskIDStr := uuid.UUID(n.TaskID.Bytes).String()

	switch n.Kind {
	case notify.KindAgentQuestion:
		var p bus.AgentQuestionPayload
		if err := json.Unmarshal(n.Payload, &p); err == nil && p.Text != "" {
			return fmt.Sprintf(
				"🤖 Агент задал вопрос по задаче %s:\n%s\n\nОтветить: /answer %s %s <ваш ответ>",
				taskIDStr, p.Text, taskIDStr, p.QuestionID,
			)
		}
	case notify.KindCommandApprovalRequest:
		var p bus.CommandApprovalRequestPayload
		if err := json.Unmarshal(n.Payload, &p); err == nil && p.Command != "" {
			return fmt.Sprintf(
				"⚠️ Агент запросил согласование команды по задаче %s:\n%s\n\nРешение — в web-приложении.",
				taskIDStr, p.Command,
			)
		}
	case notify.KindAgentCompleted:
		var p bus.AgentCompletedPayload
		if err := json.Unmarshal(n.Payload, &p); err == nil && p.Summary != "" {
			return fmt.Sprintf(
				"✅ Задача %s завершена агентом и ждёт вашего подтверждения:\n%s\n\nПодтвердить — в web-приложении.",
				taskIDStr, p.Summary,
			)
		}
		return fmt.Sprintf("✅ Задача %s завершена агентом и ждёт вашего подтверждения в web-приложении.", taskIDStr)
	case notify.KindAnswerReminder:
		return fmt.Sprintf("⏰ Напоминание: агент по задаче %s всё ещё ждёт вашего ответа.", taskIDStr)
	}

	// Фолбэк — неизвестный/нераспарсенный Kind (см. годок функции).
	return fmt.Sprintf("Новое уведомление по задаче %s (%s). Подробности — в web-приложении.", taskIDStr, n.Kind)
}
