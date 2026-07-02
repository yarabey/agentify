package bus

// Типы сообщений конверта (protocol.md §4, "Типы сообщений") — машинное
// представление таблицы из §4, чтобы продьюсеры/потребители на обеих сторонах
// (orchestrator, agent) не дублировали "magic strings" значения поля Type
// (Envelope.Type). Источник правды по смыслу каждого типа и его payload —
// сам protocol.md §4; здесь — только имена констант и форма payload, которая
// сериализуется/парсится механически (hello — единственный payload, нужный
// уже на транспортном уровне тикета 3.3; остальные типы перечислены для
// полноты протокола и используются последующими тикетами 3.4/3.5/5.x).

// Типы сообщений агент → оркестратор (топик TopicMachineEvents, protocol.md
// §4, таблица "Агент → оркестратор").
const (
	// MessageTypeHello — первый кадр после WS-коннекта; аутентификация по UUID
	// (FR B3). Payload — HelloPayload.
	MessageTypeHello = "hello"
	// MessageTypeHeartbeat — живость машины; обновляет last_seen_at (FR B4).
	// Payload — {} (пустой объект).
	MessageTypeHeartbeat = "heartbeat"
	// MessageTypeAck — подтверждение обработки команды (protocol.md §5).
	// Payload — {ack_message_id}.
	MessageTypeAck = "ack"
	// MessageTypeTaskAccepted — задача принята в работу → FSM running.
	// Payload — {}.
	MessageTypeTaskAccepted = "task_accepted"
	// MessageTypeAgentQuestion — вопрос пользователю → FSM waiting_user (FR F1).
	// Payload — {question_id, text}.
	MessageTypeAgentQuestion = "agent_question"
	// MessageTypeCommandApprovalRequest — команда вне allowlist → waiting_user
	// (FR F3). Payload — {request_id, command, reason}.
	MessageTypeCommandApprovalRequest = "command_approval_request"
	// MessageTypeAgentProgress — прогресс (история/аудит). Payload — {text}.
	MessageTypeAgentProgress = "agent_progress"
	// MessageTypeAgentCompleted — агент отчитался → FSM awaiting_confirm (НЕ
	// закрывает задачу, FR E2). Payload — {summary}.
	MessageTypeAgentCompleted = "agent_completed"
	// MessageTypeError — ошибка → FSM failed. Payload — {code, message}.
	MessageTypeError = "error"
)

// Типы сообщений оркестратор → агент (топик TopicMachineCommands, protocol.md
// §4, таблица "Оркестратор → агент").
const (
	// MessageTypeTaskAssigned — поставить задачу (FR E1). Payload — {text}.
	MessageTypeTaskAssigned = "task_assigned"
	// MessageTypeUserAnswer — ответ пользователя; агент продолжает (FR F2).
	// Payload — {question_id, text}.
	MessageTypeUserAnswer = "user_answer"
	// MessageTypeCommandDecision — решение по согласованию (FR F3). Payload —
	// {request_id, decision: approve|reject}.
	MessageTypeCommandDecision = "command_decision"
	// MessageTypeCancel — отмена; безопасная остановка, приоритет сохранности
	// данных (FR E6). Payload — {}.
	MessageTypeCancel = "cancel"
	// MessageTypePing — проверка живости соединения. Payload — {}.
	MessageTypePing = "ping"
)

// MessageTypeTelegramNotification — единственный тип сообщения оркестратор →
// бот (топик TopicNotificationsTelegram, ADR 0001, тикеты 7.3/10.4, FR G1):
// уведомление пользователя, уже отформатированное оркестратором в готовый
// текст и адресованное конкретному Telegram-чату (см. TelegramNotificationPayload).
// В отличие от machine.commands/machine.events здесь один-единственный тип —
// бот не должен разбирать бизнес-смысл Kind (agent_question/agent_completed/…,
// см. orchestrator/internal/notify), только доставить готовый текст в чат
// (принцип «единый API», вся бизнес-логика — в оркестраторе, см.
// orchestrator/internal/notify/telegram).
const MessageTypeTelegramNotification = "telegram_notification"

// TelegramNotificationPayload — payload сообщения type ==
// MessageTypeTelegramNotification (тикеты 7.3/10.4, FR G1, Gherkin §6
// «Уведомление в Telegram»). Формируется orchestrator/internal/notify/telegram.Notifier
// ПОСЛЕ резолва привязки канала (channel_links, тикет 10.2) — бот получает уже
// готовый chat_id и текст, ему не нужен доступ к БД оркестратора (принцип
// «единый API»: только оркестратор владеет связью user_id ↔ telegram_user_id).
type TelegramNotificationPayload struct {
	// TelegramChatID — telegram_user_id получателя (channel_links.external_id,
	// тикет 10.2), он же chat_id личного чата с ботом (Telegram API: chat_id
	// приватного чата с пользователем численно равен его user_id).
	TelegramChatID int64 `json:"telegram_chat_id"`
	// Kind — вид доменного уведомления (notify.KindAgentQuestion и т.п., см.
	// orchestrator/internal/notify) — переносится для наблюдаемости/логов
	// бота; сама доставка (§4, выше) не различает Kind, шлёт готовый Text.
	Kind string `json:"kind"`
	// TaskID — задача, к которой относится уведомление (для контекста в
	// логах бота и потенциальных будущих команд вида /answer, тикет 10.3).
	TaskID string `json:"task_id"`
	// Text — готовый человекочитаемый текст уведомления на русском,
	// сформированный оркестратором (Kind + сырой Payload доменного события,
	// см. orchestrator/internal/notify/telegram.formatText) — бот отправляет
	// его как есть, без дополнительного форматирования.
	Text string `json:"text"`
}

// AckPayload — payload сообщения type == MessageTypeAck (protocol.md §4, §5):
// подтверждение агентом обработки команды из machine.commands, отправленное
// в ответ на конкретный конверт-команду. AckMessageID — это MessageID ИМЕННО
// того конверта-команды, который агент подтверждает (а не MessageID самого
// ack-конверта) — по нему мост оркестратора (тикет 3.4) сопоставляет ack с
// доставленной, но ещё не закоммиченной записью Redpanda и коммитит её
// offset (commit-after-ACK, §5). Дублирующийся или ссылающийся на неизвестный
// message_id ack — штатная ситуация at-least-once (переотправка/гонка) и
// должен безопасно игнорироваться стороной, которая его разбирает.
type AckPayload struct {
	// AckMessageID — message_id подтверждаемого конверта-команды.
	AckMessageID string `json:"ack_message_id"`
}

// HelloPayload — payload сообщения type == MessageTypeHello (protocol.md §4):
// первый кадр агента после WS-коннекта, по которому оркестратор опознаёт
// машину (FR B3, тикет 2.3/3.3). Это тот же набор полей, что разбирает
// orchestrator/internal/api.GetMachineWs на стороне сервера, и который должен
// собрать agent/internal/wsclient на стороне клиента — общий пакет bus
// гарантирует, что обе стороны говорят об одной и той же форме payload без
// дублирования структуры.
type HelloPayload struct {
	// UUID — секрет интеграции (plaintext), выданный владельцу при создании
	// интеграции (POST /integrations, тикет 2.2) и переданный агенту при
	// настройке (FR B2). Сервер ищет интеграцию по HMAC-отпечатку этого
	// значения (FR B6), сам секрет нигде, кроме hello, не передаётся.
	UUID string `json:"uuid"`
	// AgentVersion — версия бинаря агента (FR C5, контроль совместимости —
	// тикет 4.7). Тикетом 3.3 не валидируется, только переносится.
	AgentVersion string `json:"agent_version"`
	// Providers — список провайдеров, доступных этому агенту (claude,
	// claude-code, ...; EPIC 4.5). Тикетом 3.3 не валидируется.
	Providers []string `json:"providers"`
}

// TaskAssignedPayload — payload сообщения type == MessageTypeTaskAssigned
// (тикет 5.3, FR E1): текст задачи, которую агент должен выполнить.
type TaskAssignedPayload struct {
	Text string `json:"text"`
}

// AgentQuestionPayload — payload сообщения type == MessageTypeAgentQuestion
// (тикет 6.1, FR F1): вопрос агента пользователю, требующий решения перед
// продолжением задачи.
type AgentQuestionPayload struct {
	// QuestionID — идентификатор вопроса, присвоенный агентом; используется
	// для сопоставления с последующим ответом пользователя (ref_event_id).
	QuestionID string `json:"question_id"`
	Text       string `json:"text"`
}

// UserAnswerPayload — payload сообщения type == MessageTypeUserAnswer
// (тикет 6.1, FR F2): ответ пользователя на вопрос агента, идентифицированный
// question_id того же вопроса.
type UserAnswerPayload struct {
	QuestionID string `json:"question_id"`
	Text       string `json:"text"`
}

// ErrorPayload — payload сообщения type == MessageTypeError (тикет 5.8,
// FR E1, protocol.md §4): ошибка агента или машины, переводящая задачу в
// failed.
type ErrorPayload struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// AgentCompletedPayload — payload сообщения type == MessageTypeAgentCompleted
// (тикет 8.1, FR E2): агент сообщил о завершении работы, задача переходит в
// awaiting_confirm, но НЕ считается закрытой — закрытие требует явного
// подтверждения пользователя (тикеты 8.2/8.3).
type AgentCompletedPayload struct {
	Summary string `json:"summary"`
}

// AgentProgressPayload — payload события agent_progress (тикет 8.5, FR E6):
// человекочитаемый текст прогресса/предупреждения, показываемый в истории
// задачи (аудит, FR F4).
type AgentProgressPayload struct {
	Text string `json:"text"`
}

// CommandApprovalRequestPayload — payload сообщения type ==
// MessageTypeCommandApprovalRequest (тикет 4.5/6.1, FR F3): провайдер
// (agent/internal/provider/claudecode) перехватил запрос CLI на выполнение
// команды вне allowlist (control_request/can_use_tool) и просит решение
// пользователя перед тем, как ответить CLI и продолжить задачу.
type CommandApprovalRequestPayload struct {
	// RequestID — идентификатор control_request исходного протокола CLI;
	// используется для сопоставления с последующим CommandDecisionPayload
	// (ref_event_id) и передаётся Provider.Approve при разрешении.
	RequestID string `json:"request_id"`
	// Command — команда, которую CLI просит разрешить выполнить (например,
	// содержимое поля input.command запроса can_use_tool).
	Command string `json:"command"`
	// Reason — человекочитаемая причина, почему требуется согласование (для
	// отображения пользователю); тикетом 4.5 не детализируется.
	Reason string `json:"reason"`
}

// CommandDecisionPayload — payload сообщения type == MessageTypeCommandDecision
// (тикет 4.5/6.1, FR F3): решение пользователя по ранее запрошенному
// согласованию команды, идентифицированному request_id того же запроса.
type CommandDecisionPayload struct {
	RequestID string `json:"request_id"`
	// Decision — "approve" или "reject" (тот же словарь, что и
	// Provider.Approve, и таблица protocol.md §4).
	Decision string `json:"decision"`
}
