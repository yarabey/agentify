// web/e2e/support/fakeMachine.ts — тестовый двойник "машины" (агента) для
// E2E-сценариев тикета 9.8 (критерий выхода MVP).
//
// Назначение (бизнес): 9.8 требует, чтобы web-канал был провезён по ВСЕМУ
// жизненному циклу задачи (docs/Жизненный цикл задачи.md) через настоящий
// стек, включая машину — но реальный провайдер (agent/internal/provider/
// claudecode, claude) требует установленного CLI `claude`/ключ Anthropic API
// и НЕ детерминирован (реальная LLM), непригоден для CI. FakeMachine — это
// не мок HTTP-уровня (в отличие от orchestrator/internal/bddsteps, который
// умышленно тестирует только границу оркестратора — см.
// orchestrator/features/README.md), а РЕАЛЬНОЕ WS-соединение к
// `/machine/ws`, говорящее ровно тем же протоколом (docs/protocol.md
// §2/§4), что и настоящий агент (agent/internal/wsclient) — с точки зрения
// оркестратора неотличимо от машины на реальном железе.
//
// Архитектурное решение "почему НЕ переиспользовать agent/ бинарь целиком"
// (по аналогии с ADR-обоснованиями тикетов 10.3/7.3/7.4, см. AGENTS.md §5) —
// подробно в web/e2e/README.md; коротко: 1) единственный существующий
// провайдер, готовый эмулировать сценарий детерминированно, — сам агентский
// wsclient — НЕ реализует приём кадра `user_answer` от оркестратора
// (agent/internal/wsclient/wsclient.go, godoc пакета: "обработка
// user_answer/ping — EPIC 5.x/6.x, вне объёма") и ни один из встроенных
// провайдеров (claudecode/claude) не отправляет `agent_question` вообще —
// то есть кусок протокола, критичный именно для сценария "вопрос → ответ →
// продолжение" 9.8, не имеет production-реализации на стороне агента
// СЕГОДНЯ (см. finding в PR этого тикета); 2) даже будь он реализован,
// перекомпилировать/поднимать ещё один Go-процесс из Playwright сложнее и
// более хрупко, чем открыть одно WS-соединение прямо в тесте. Поэтому здесь
// — независимая, полностью детерминированная реализация клиентской стороны
// протокола, а не мок существующего кода.
//
// Как устроено (тех): FakeMachine.connect держит одно `ws` WebSocket-
// соединение (та же кромка, что описана в docs/protocol.md §1) — hello
// отправляется сразу после open (см. dial), как и в agent/internal/wsclient.
// Каждый входящий конверт-КОМАНДА (task_assigned/user_answer/cancel)
// немедленно подтверждается ack'ом (protocol.md §5 — "мост держит offset
// незакоммиченным, пока не придёт ack"; без этого шага 5.6/offline-catchup
// сценарий не смог бы наблюдать РЕАЛЬНУЮ доставку через Redpanda-мост) —
// эта часть НЕ входит в объём wsclient-пробела выше: ack поддерживается
// нормально что у агента, что здесь, пробел именно в бизнес-обработке
// user_answer. `waitForFrame` — единственная точка ожидания конкретного
// входящего типа кадра (используется тестами, чтобы дождаться
// `task_assigned`/`user_answer`, прежде чем реагировать сценарием).
import { WebSocket } from "ws";

import {
  type AckPayload,
  type AgentCompletedPayload,
  type AgentProgressPayload,
  type AgentQuestionPayload,
  type Envelope,
  type HelloPayload,
  MessageType,
  newMessageID,
  nowRFC3339,
  PROTOCOL_VERSION,
} from "./protocol";

/** Типы входящих (оркестратор → машина) конвертов, которые ack-аются автоматически (protocol.md §5). */
const COMMAND_FRAME_TYPES: ReadonlySet<string> = new Set([
  MessageType.TaskAssigned,
  MessageType.UserAnswer,
  MessageType.Cancel,
]);

/** Сколько FakeMachine.waitForFrame ждёт кадр нужного типа по умолчанию — с запасом на доставку через реальный Redpanda-мост (не просто внутрипроцессный вызов, как в orchestrator/internal/bddsteps). */
const DEFAULT_WAIT_MS = 20_000;

export class FakeMachine {
  private readonly ws: WebSocket;
  private readonly pending = new Map<string, Array<(env: Envelope) => void>>();
  private readonly buffered = new Map<string, Envelope[]>();
  private closed = false;
  private seq = 1;

  private constructor(ws: WebSocket) {
    this.ws = ws;
    this.ws.on("message", (data) => this.handleRaw(data.toString()));
  }

  /**
   * Открывает WS-соединение к `/machine/ws` и проходит hello-аутентификацию
   * по секрету интеграции (docs/protocol.md §4 "hello", тот же payload,
   * что и agent/internal/wsclient.sendHello). integrationSecret — это
   * `uuid`, показанный один раз при создании интеграции (тикет 2.2,
   * `IntegrationWithSecret.uuid`, см. web/e2e/support/ui.ts createIntegration).
   */
  static async connect(wsUrl: string, integrationSecret: string): Promise<FakeMachine> {
    const ws = new WebSocket(wsUrl);
    await new Promise<void>((resolve, reject) => {
      ws.once("open", () => resolve());
      ws.once("error", (err) => reject(err));
    });

    const machine = new FakeMachine(ws);
    const hello: Envelope = {
      message_id: newMessageID(),
      task_id: null,
      integration_id: "00000000-0000-0000-0000-000000000000",
      type: MessageType.Hello,
      seq: 1,
      ts: nowRFC3339(),
      protocol_version: PROTOCOL_VERSION,
      payload: {
        uuid: integrationSecret,
        agent_version: "e2e-fake-machine/0.0.0",
        providers: ["claude"],
      } satisfies HelloPayload,
    };
    ws.send(JSON.stringify(hello));
    return machine;
  }

  /** Разбирает и маршрутизирует один входящий кадр (см. годок класса). */
  private handleRaw(raw: string): void {
    let env: Envelope;
    try {
      env = JSON.parse(raw) as Envelope;
    } catch {
      return;
    }

    if (COMMAND_FRAME_TYPES.has(env.type)) {
      this.sendAck(env.message_id);
    }

    const waiters = this.pending.get(env.type);
    if (waiters && waiters.length > 0) {
      const resolve = waiters.shift()!;
      resolve(env);
      return;
    }
    const buf = this.buffered.get(env.type) ?? [];
    buf.push(env);
    this.buffered.set(env.type, buf);
  }

  /** Блокируется, пока не придёт кадр типа `type` (или предыдущий уже буферизованный не будет отдан), либо не истечёт timeoutMs. */
  async waitForFrame(type: string, timeoutMs = DEFAULT_WAIT_MS): Promise<Envelope> {
    const buf = this.buffered.get(type);
    if (buf && buf.length > 0) {
      return buf.shift()!;
    }
    return new Promise<Envelope>((resolve, reject) => {
      const timer = setTimeout(() => {
        const waiters = this.pending.get(type);
        if (waiters) {
          const idx = waiters.indexOf(wrapped);
          if (idx >= 0) waiters.splice(idx, 1);
        }
        reject(new Error(`FakeMachine.waitForFrame: не дождались кадра type=${type} за ${timeoutMs}мс`));
      }, timeoutMs);
      const wrapped = (env: Envelope): void => {
        clearTimeout(timer);
        resolve(env);
      };
      const waiters = this.pending.get(type) ?? [];
      waiters.push(wrapped);
      this.pending.set(type, waiters);
    });
  }

  private send(env: Envelope): void {
    this.ws.send(JSON.stringify(env));
  }

  private sendAck(ackMessageId: string): void {
    this.send({
      message_id: newMessageID(),
      task_id: null,
      integration_id: "",
      type: MessageType.Ack,
      seq: this.seq++,
      ts: nowRFC3339(),
      protocol_version: PROTOCOL_VERSION,
      payload: { ack_message_id: ackMessageId } satisfies AckPayload,
    });
  }

  /** `task_accepted` — задача принята в работу → FSM `running` (protocol.md §4, FR E1). */
  sendTaskAccepted(taskId: string): void {
    this.send({
      message_id: newMessageID(),
      task_id: taskId,
      integration_id: "",
      type: MessageType.TaskAccepted,
      seq: this.seq++,
      ts: nowRFC3339(),
      protocol_version: PROTOCOL_VERSION,
      payload: {},
    });
  }

  /** `agent_question` — вопрос пользователю → FSM `waiting_user` (protocol.md §4, FR F1). */
  sendAgentQuestion(taskId: string, questionId: string, text: string): void {
    this.send({
      message_id: newMessageID(),
      task_id: taskId,
      integration_id: "",
      type: MessageType.AgentQuestion,
      seq: this.seq++,
      ts: nowRFC3339(),
      protocol_version: PROTOCOL_VERSION,
      payload: { question_id: questionId, text } satisfies AgentQuestionPayload,
    });
  }

  /** `agent_progress` — прогресс/предупреждение, БЕЗ смены статуса (protocol.md §4, тикет 8.5, FR E6 — используется safe-cancellation.spec.ts как сигнал "довёл до безопасного состояния"). */
  sendAgentProgress(taskId: string, text: string): void {
    this.send({
      message_id: newMessageID(),
      task_id: taskId,
      integration_id: "",
      type: MessageType.AgentProgress,
      seq: this.seq++,
      ts: nowRFC3339(),
      protocol_version: PROTOCOL_VERSION,
      payload: { text } satisfies AgentProgressPayload,
    });
  }

  /** `agent_completed` — агент отчитался → FSM `awaiting_confirm`, НЕ закрывает задачу (protocol.md §4, тикет 8.1, FR E2). */
  sendAgentCompleted(taskId: string, summary: string): void {
    this.send({
      message_id: newMessageID(),
      task_id: taskId,
      integration_id: "",
      type: MessageType.AgentCompleted,
      seq: this.seq++,
      ts: nowRFC3339(),
      protocol_version: PROTOCOL_VERSION,
      payload: { summary } satisfies AgentCompletedPayload,
    });
  }

  /** Закрывает WS-соединение (тест должен вызывать в finally — иначе Playwright процесс держит открытый сокет до таймаута воркера). */
  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    await new Promise<void>((resolve) => {
      this.ws.once("close", () => resolve());
      this.ws.close();
      // На случай, если соединение уже мертво и "close" не придёт.
      setTimeout(resolve, 2_000);
    });
  }
}
