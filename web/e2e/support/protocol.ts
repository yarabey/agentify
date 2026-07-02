// web/e2e/support/protocol.ts — TS-зеркало протокола машина↔оркестратор
// (docs/protocol.md, internal/bus/*.go) для тестового драйвера "фейковой
// машины" (тикет 9.8, критерий выхода MVP, см. web/e2e/support/fakeMachine.ts).
//
// Назначение (бизнес): весь смысл 9.8 — прогнать web → оркестратор →
// МАШИНА → оркестратор → web через настоящий протокол (docs/protocol.md
// §1-§6), а не через мок на уровне HTTP. В проекте нет готового "тестового
// агента" (agent/internal/provider/* — обвязка над реальным CLI `claude`,
// исполнять которую в CI бессмысленно и невозможно детерминированно, см.
// обоснование выбора в web/e2e/README.md), поэтому здесь — независимая от
// agent/ реализация клиентской стороны WS-протокола специально для E2E:
// подключение к `/machine/ws` как обычная машина (hello по секрету
// интеграции) и обмен теми же JSON-конвертами, что описаны в
// docs/protocol.md §2/§4.
//
// Как устроено (тех): НЕ полное зеркало protocol.md — только те типы
// сообщений и поля payload, которые реально используют сценарии 9.8
// (web/e2e/happy-path.spec.ts, offline-catchup.spec.ts,
// safe-cancellation.spec.ts); `ping`/`error` не нужны. Источник правды
// остаётся internal/bus/*.go (Go) — этот файл синхронизируется руками при
// изменении протокола, тот же приём сознательного дублирования формы, что
// и у `clientNotificationFrame` в orchestrator/internal/bddsteps/world_test.go
// (там это обосновано так же: "тот символ неэкспортирован/в другом языке,
// экспортировать через границу Go↔TS нечем").

/** protocol.md §2, §7 — версия конверта, которую сверяет `authenticateMachineHello`. */
export const PROTOCOL_VERSION = "1";

/** protocol.md §4 — типы сообщений конверта, использующиеся сценариями 9.8. */
export const MessageType = {
  Hello: "hello",
  Ack: "ack",
  TaskAccepted: "task_accepted",
  AgentQuestion: "agent_question",
  AgentProgress: "agent_progress",
  AgentCompleted: "agent_completed",
  TaskAssigned: "task_assigned",
  UserAnswer: "user_answer",
  Cancel: "cancel",
} as const;

/** Конверт сообщения шины (protocol.md §2) — зеркало `bus.Envelope`. */
export interface Envelope {
  message_id: string;
  task_id: string | null;
  integration_id: string;
  type: string;
  seq: number;
  ts: string;
  protocol_version: string;
  payload: unknown;
}

/** payload `hello` (protocol.md §4) — зеркало `bus.HelloPayload`. */
export interface HelloPayload {
  uuid: string;
  agent_version: string;
  providers: string[];
}

/** payload `ack` (protocol.md §4/§5) — зеркало `bus.AckPayload`. */
export interface AckPayload {
  ack_message_id: string;
}

/** payload `task_assigned` (protocol.md §4) — зеркало `bus.TaskAssignedPayload`. */
export interface TaskAssignedPayload {
  text: string;
}

/** payload `agent_question` (protocol.md §4) — зеркало `bus.AgentQuestionPayload`. */
export interface AgentQuestionPayload {
  question_id: string;
  text: string;
}

/** payload `user_answer` (protocol.md §4) — зеркало `bus.UserAnswerPayload`. */
export interface UserAnswerPayload {
  question_id: string;
  text: string;
}

/** payload `agent_completed` (protocol.md §4) — зеркало `bus.AgentCompletedPayload`. */
export interface AgentCompletedPayload {
  summary: string;
}

/** payload `agent_progress` (protocol.md §4) — зеркало `bus.AgentProgressPayload`. */
export interface AgentProgressPayload {
  text: string;
}

/** Строит новый `message_id` (protocol.md §2 — глобально уникален, основа дедупа). */
export function newMessageID(): string {
  return crypto.randomUUID();
}

/** Текущая метка времени в формате RFC3339 (protocol.md §2, поле `ts`). */
export function nowRFC3339(): string {
  return new Date().toISOString();
}
