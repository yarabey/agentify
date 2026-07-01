// Package notify — доменное событие уведомления пользователя (FR G1, тикет
// 7.1 EPIC 7 "Уведомления").
//
// Назначение (бизнес): о вопросе агента и о важных сменах статуса задачи
// пользователь должен быть уведомлён (FR G1: "В MVP: в web — через
// постоянное соединение (WebSocket), и в Telegram"). Этот пакет описывает
// ТОЛЬКО общий доменный тип такого события (Notification) — по аналогии с
// internal/bus.Envelope: общий тип контракта, которым пользуются и
// производители события (сейчас — api.Server.handleAgentQuestion, тикет 7.1),
// и будущие потребители (реализации канала доставки), причём ни одна из
// сторон не знает о конкретной реализации другой.
//
// Что НЕ входит в этот пакет и в тикет 7.1: сама доставка уведомления
// пользователю. Реализация канала web (постоянное WS-соединение с клиентом,
// а не с машиной агента) — тикет 7.2; реализация канала Telegram — тикет 7.3;
// маршрутизация между активными каналами, когда их несколько одновременно, —
// тикет 7.4. Здесь нет и не должно быть транспортного кода — только
// доменный тип.
package notify

import (
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

// KindAgentQuestion — вид уведомления "агент задал вопрос по задаче" (FR G1,
// FR F1, тикет 6.1/7.1). Первый вид, сформированный в MVP тикета 7.1; с
// тикетом 9.5 к нему добавились KindCommandApprovalRequest и
// KindAgentCompleted — все важные смены статуса, о которых должен быть
// уведомлён пользователь (FR G1), формируют собственные константы Kind по
// мере появления соответствующих переходов FSM.
const KindAgentQuestion = "agent_question"

// KindAnswerReminder — вид уведомления "пользователь давно не отвечает на
// вопрос агента" (FR F5, тикет 6.7): формируется task.AnswerTimeoutWorker,
// когда самый свежий agent_question активной (waiting_user) задачи устарел
// дольше настраиваемого порога.
const KindAnswerReminder = "answer_reminder"

// KindCommandApprovalRequest — вид уведомления "агент запросил согласование
// команды вне allowlist" (FR F1/F3, тикет 6.4): формируется
// api.Server.handleCommandApprovalRequest (тикет 9.5) при успешном переходе
// running→waiting_user по кадру command_approval_request — пользователь
// должен узнать об этом в реальном времени, не дожидаясь перезагрузки
// страницы (Gherkin §5 «Команда вне allowlist требует согласования»).
const KindCommandApprovalRequest = "command_approval_request"

// KindAgentCompleted — вид уведомления "задача ожидает подтверждения
// завершения" (FR E2, тикет 8.1): формируется
// api.Server.handleAgentCompleted (тикет 9.5) при успешном переходе
// running→awaiting_confirm по кадру agent_completed — завершение задачи
// подтверждает только явное действие пользователя (FR E2), но само появление
// такого запроса должно быть видно в реальном времени (Gherkin §7 «Агент
// сообщил о завершении — задача ещё не закрыта»).
const KindAgentCompleted = "agent_completed"

// Notification — доменное событие уведомления (FR G1, тикет 7.1): пользователь
// (UserID) должен быть уведомлён о событии Kind по задаче TaskID. Payload —
// сырые данные события (например, RAW payload agent_question-кадра из
// protocol.md §4) для последующего форматирования конкретным каналом доставки
// (web WebSocket — тикет 7.2, Telegram — тикет 7.3); сам тип Notification НЕ
// знает и не должен знать о каналах доставки — это узкая граница между
// формированием уведомления (api.Server, см. Notifier) и его доставкой.
type Notification struct {
	TaskID    pgtype.UUID
	UserID    pgtype.UUID
	Kind      string
	Payload   []byte
	CreatedAt time.Time
}
