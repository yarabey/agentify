// Package bus содержит общие константы шины Redpanda — имена топиков и ключей
// партиций, — на которые опираются producer/consumer (тикет 3.2), мост
// оркестратора (3.4) и бот уведомлений. Источник правды по раскладке —
// docs/adr/0001-redpanda-topics.md и docs/protocol.md §3; здесь — её машинное
// представление, чтобы в коде не было «magic strings».
package bus

// Имена топиков Redpanda (ADR 0001, protocol.md §3). Один топик на направление.
const (
	// TopicMachineCommands — оркестратор → агент: команды машине (task_assigned,
	// user_answer, command_decision, cancel, ping). Ключ партиции —
	// PartitionKeyIntegrationID. Мост фильтрует по integration_id и пушит в WS
	// нужной машины; оффлайн-команда дочитывается при реконнекте.
	TopicMachineCommands = "machine.commands"

	// TopicMachineEvents — агент → оркестратор: события агента/машины
	// (task_accepted, agent_question, agent_progress, agent_completed, error,
	// ack, hello, heartbeat). Ключ партиции — PartitionKeyTaskID для событий
	// уровня задачи и PartitionKeyIntegrationID для machine-level (hello/heartbeat,
	// где task_id == null).
	TopicMachineEvents = "machine.events"

	// TopicNotificationsTelegram — оркестратор → бот: уведомления для доставки в
	// Telegram. Ключ партиции — PartitionKeyUserID.
	TopicNotificationsTelegram = "notifications.telegram"
)

// Имена полей конверта (protocol.md §2), используемые как ключи партиций Redpanda
// (ADR 0001). Значение ключа берётся из соответствующего поля конверта сообщения.
const (
	// PartitionKeyIntegrationID — ключ партиции по integration_id: для
	// TopicMachineCommands (порядок команд в адрес одной машины) и для
	// machine-level событий TopicMachineEvents (hello/heartbeat).
	PartitionKeyIntegrationID = "integration_id"

	// PartitionKeyTaskID — ключ партиции по task_id: для событий уровня задачи в
	// TopicMachineEvents (порядок в рамках задачи, бизнес-ТЗ §126).
	PartitionKeyTaskID = "task_id"

	// PartitionKeyUserID — ключ партиции по user_id: для TopicNotificationsTelegram
	// (порядок уведомлений одного пользователя).
	PartitionKeyUserID = "user_id"
)
