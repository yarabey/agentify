package task

import "fmt"

// Status — статус задачи FSM (docs/"Жизненный цикл задачи.md"), СТРОГО
// совпадает по значениям с CHECK-ограничением tasks.status
// (orchestrator/migrations/00003_tasks_and_events.sql).
type Status string

// Значения Status — девять статусов задачи из docs/"Жизненный цикл
// задачи.md" (Created/Queued/Running/WaitingUser/AwaitingConfirm/Completed/
// Failed/Cancelled/Stale), дословно совпадающие с CHECK-ограничением
// tasks.status.
const (
	StatusCreated         Status = "created"
	StatusQueued          Status = "queued"
	StatusRunning         Status = "running"
	StatusWaitingUser     Status = "waiting_user"
	StatusAwaitingConfirm Status = "awaiting_confirm"
	StatusCompleted       Status = "completed"
	StatusFailed          Status = "failed"
	StatusCancelled       Status = "cancelled"
	StatusStale           Status = "stale"
)

// Trigger — событие, вызывающее переход FSM. Где переход напрямую вызван
// сообщением протокола (docs/protocol.md, machine.events/machine.commands),
// значение совпадает с полем type этого сообщения; остальные — внутренние
// названия для переходов, инициируемых будущими тикетами оркестратора.
type Trigger string

const (
	// TriggerEnqueued — задача помещена в очередь сообщений (тикет 5.4).
	TriggerEnqueued Trigger = "enqueued"
	// TriggerTaskAccepted — машина забрала задачу (machine.events
	// type=task_accepted).
	TriggerTaskAccepted Trigger = "task_accepted"
	// TriggerAgentQuestion — агент задал вопрос (machine.events
	// type=agent_question, FR F1).
	TriggerAgentQuestion Trigger = "agent_question"
	// TriggerApprovalRequested — агент запросил согласование команды
	// (machine.events type=command_approval_request, FR F3).
	TriggerApprovalRequested Trigger = "command_approval_request"
	// TriggerUserAnswered — пользователь ответил на вопрос (тикет 6.1).
	TriggerUserAnswered Trigger = "user_answered"
	// TriggerCommandDecision — пользователь вынес решение по согласованию
	// команды, approve или reject (тикет 6.4/6.5); в обоих случаях задача
	// возвращается в running — машина обрабатывает конкретное решение сама.
	TriggerCommandDecision Trigger = "command_decision"
	// TriggerAgentCompleted — агент сообщил о завершении (machine.events
	// type=agent_completed, FR E2) — НЕ закрывает задачу, только
	// awaiting_confirm.
	TriggerAgentCompleted Trigger = "agent_completed"
	// TriggerUserConfirmed — пользователь явно подтвердил завершение
	// (тикет 8.2, FR E2).
	TriggerUserConfirmed Trigger = "user_confirmed"
	// TriggerCompletionRejected — пользователь отклонил результат, задача
	// возвращается в доработку (тикет 8.3).
	TriggerCompletionRejected Trigger = "completion_rejected"
	// TriggerAgentError — ошибка агента или машины (machine.events
	// type=error, тикет 5.8).
	TriggerAgentError Trigger = "error"
	// TriggerTimeout — машина пропала надолго, STALE_THRESHOLD (тикет 5.7,
	// FR E5).
	TriggerTimeout Trigger = "timeout"
	// TriggerMachineRecovered — машина вернулась, задача продолжается
	// (тикет 5.7, FR E5).
	TriggerMachineRecovered Trigger = "machine_recovered"
	// TriggerCancelRequested — пользователь отменил задачу (тикет 8.4, FR
	// E6).
	TriggerCancelRequested Trigger = "cancel_requested"
)

type transitionKey struct {
	from    Status
	trigger Trigger
}

// transitions — ЕДИНСТВЕННЫЙ источник допустимых переходов FSM, дословный
// перенос docs/"Жизненный цикл задачи.md" (mermaid stateDiagram-v2) в код.
// Created→Cancelled НАМЕРЕННО отсутствует: такого ребра нет в диаграмме
// (отменить можно только уже поставленную в очередь задачу, не Created).
var transitions = map[transitionKey]Status{
	{StatusCreated, TriggerEnqueued}:    StatusQueued,
	{StatusQueued, TriggerTaskAccepted}: StatusRunning,

	{StatusRunning, TriggerAgentQuestion}:       StatusWaitingUser,
	{StatusRunning, TriggerApprovalRequested}:   StatusWaitingUser,
	{StatusWaitingUser, TriggerUserAnswered}:    StatusRunning,
	{StatusWaitingUser, TriggerCommandDecision}: StatusRunning,

	{StatusRunning, TriggerAgentCompleted}:             StatusAwaitingConfirm,
	{StatusAwaitingConfirm, TriggerUserConfirmed}:      StatusCompleted,
	{StatusAwaitingConfirm, TriggerCompletionRejected}: StatusRunning,

	{StatusRunning, TriggerAgentError}: StatusFailed,

	{StatusRunning, TriggerTimeout}:        StatusStale,
	{StatusWaitingUser, TriggerTimeout}:    StatusStale,
	{StatusStale, TriggerMachineRecovered}: StatusRunning,

	{StatusQueued, TriggerCancelRequested}:          StatusCancelled,
	{StatusRunning, TriggerCancelRequested}:         StatusCancelled,
	{StatusWaitingUser, TriggerCancelRequested}:     StatusCancelled,
	{StatusAwaitingConfirm, TriggerCancelRequested}: StatusCancelled,
	{StatusStale, TriggerCancelRequested}:           StatusCancelled,
}

// NextStatus — единственная точка проверки допустимости перехода FSM (FR E1).
// Чистая функция без обращения к БД: одна и та же таблица используется и
// Transition (запись в БД), и тестами (табличная проверка всех переходов).
func NextStatus(current Status, trigger Trigger) (Status, error) {
	next, ok := transitions[transitionKey{current, trigger}]
	if !ok {
		return "", fmt.Errorf("task: недопустимый переход FSM: %s + %s", current, trigger)
	}
	return next, nil
}
