import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link, useParams } from "react-router-dom";

import { apiClient } from "@/api/client";
import type { components } from "@/api/schema";
import { Button } from "@/components/ui/button";
import { useNotificationContext } from "@/context/NotificationContext";
import { TASK_STATUS_LABELS } from "@/lib/taskStatus";

type TaskEvent = components["schemas"]["TaskEvent"];
type TaskStatus = components["schemas"]["TaskStatus"];

/**
 * Терминальные статусы задачи (`docs/Жизненный цикл задачи.md`) — из них нет
 * исходящих переходов FSM, задача больше не активна. Всё остальное (включая
 * `stale`) по диаграмме имеет переход в `cancelled`, т.е. активно.
 */
const TERMINAL_STATUSES: ReadonlySet<TaskStatus> = new Set([
  "completed",
  "failed",
  "cancelled",
]);

/**
 * Человекочитаемые заголовки типов событий журнала (тикет 9.4, FR H1,
 * Gherkin §10 «История»).
 */
const EVENT_TYPE_LABELS: Record<string, string> = {
  status_change: "Смена статуса",
  agent_question: "Вопрос агента",
  user_answer: "Ответ пользователя",
  command_approval_request: "Запрос согласования команды",
  user_decision: "Решение пользователя",
  agent_progress: "Прогресс агента",
  agent_completed: "Агент завершил",
  error: "Ошибка",
};

/**
 * `payload` события в контракте — свободный `object` без строгой типизации
 * по `type` (структура зависит от конкретного события), поэтому рендерим
 * best-effort: если находится знакомое текстовое поле — показываем его
 * человекочитаемо, иначе — сырой JSON в `<pre>`. Журнал read-only, поэтому
 * усложнять разбор по каждому `type` отдельно незачем.
 */
const RELEVANT_PAYLOAD_KEYS = [
  "text",
  "message",
  "command",
  "reason",
  "answer",
  "question",
  "status",
];

function describePayload(payload: TaskEvent["payload"]): string | null {
  if (!payload || typeof payload !== "object") {
    return null;
  }
  for (const key of RELEVANT_PAYLOAD_KEYS) {
    const value = (payload as Record<string, unknown>)[key];
    if (typeof value === "string" && value.length > 0) {
      return value;
    }
  }
  return null;
}

function TaskEventRow({ event }: { event: TaskEvent }): JSX.Element {
  const title = (event.type && EVENT_TYPE_LABELS[event.type]) || event.type || "Событие";
  const description = describePayload(event.payload);

  return (
    <li className="rounded-md border border-border p-4">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <span className="font-medium">
          #{event.seq} · {title}
        </span>
        <span className="text-sm text-muted-foreground">
          {event.created_at ? new Date(event.created_at).toLocaleString() : ""}
        </span>
      </div>
      {description ? (
        <p className="mt-2 text-sm">{description}</p>
      ) : (
        <pre className="mt-2 overflow-x-auto rounded-md bg-muted p-2 text-xs">
          {JSON.stringify(event.payload, null, 2)}
        </pre>
      )}
    </li>
  );
}

/**
 * Форма ответа на вопрос агента (тикет 8.8, FR H2, Gherkin §10 «Действия из
 * истории в web»). Показывается только когда статус задачи `waiting_user` И
 * последнее событие журнала — `agent_question` (см. `TaskDetailPage` — API
 * не отдаёт `ref_event_id`, поэтому «на какой вопрос отвечаем» определяется
 * позиционно: самый последний вопрос в упорядоченном по `seq` журнале).
 */
function AnswerQuestionForm({
  taskId,
  questionEventId,
  questionText,
  onAnswered,
}: {
  taskId: string;
  questionEventId: string;
  questionText: string | null;
  onAnswered: () => void;
}): JSX.Element {
  const [text, setText] = useState("");

  const answerMutation = useMutation({
    mutationFn: async () => {
      const result = await apiClient.POST("/tasks/{id}/answer", {
        params: { path: { id: taskId } },
        body: { question_id: questionEventId, text },
      });
      if (!result.response.ok) {
        throw new Error("failed to answer task question");
      }
      return result;
    },
    onSuccess: () => {
      setText("");
      onAnswered();
    },
  });

  return (
    <form
      className="flex flex-col gap-3 rounded-md border border-border p-4"
      onSubmit={(event) => {
        event.preventDefault();
        answerMutation.mutate();
      }}
      noValidate
    >
      <h2 className="text-lg font-semibold">Вопрос агента</h2>
      {questionText && <p className="text-sm">{questionText}</p>}
      <label className="flex flex-col gap-2" htmlFor="task-answer-text">
        <span className="text-sm">Ваш ответ</span>
        <textarea
          id="task-answer-text"
          rows={3}
          className="flex w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2"
          value={text}
          onChange={(event) => setText(event.target.value)}
          disabled={answerMutation.isPending}
        />
      </label>
      {answerMutation.isError && (
        <p role="alert" className="text-sm text-destructive">
          Не удалось отправить ответ. Попробуйте ещё раз.
        </p>
      )}
      <div>
        <Button type="submit" disabled={answerMutation.isPending}>
          {answerMutation.isPending ? "Отправляем…" : "Ответить"}
        </Button>
      </div>
    </form>
  );
}

/**
 * Блок действий над активной задачей (тикет 8.8, дополнено тикетом 9.5, FR
 * H2/F1/G1, Gherkin §5 «Команда вне allowlist требует согласования»/
 * «Отклонение команды» и §10 «Действия из истории в web»): отмена, ответ на
 * вопрос агента, явное подтверждение завершения, а теперь ещё и
 * одобрение/отклонение команды вне allowlist — в отличие от тикета 8.8, где
 * согласование команды было явно исключено, 9.5 явно включает его (FR F1:
 * «агент может задать вопрос ИЛИ запросить разрешение на команду»).
 * Отклонение результата на доработку по-прежнему вне объёма.
 */
function TaskActions({
  taskId,
  status,
  lastEvent,
  onChanged,
}: {
  taskId: string;
  status: TaskStatus | undefined;
  lastEvent: TaskEvent | undefined;
  onChanged: () => void;
}): JSX.Element | null {
  const isActive = status !== undefined && !TERMINAL_STATUSES.has(status);

  const cancelMutation = useMutation({
    mutationFn: async () => {
      const result = await apiClient.POST("/tasks/{id}/cancel", {
        params: { path: { id: taskId } },
      });
      if (!result.response.ok) {
        throw new Error("failed to cancel task");
      }
      return result;
    },
    onSuccess: onChanged,
  });

  const confirmMutation = useMutation({
    mutationFn: async () => {
      const result = await apiClient.POST("/tasks/{id}/confirm", {
        params: { path: { id: taskId } },
      });
      if (!result.response.ok) {
        throw new Error("failed to confirm task completion");
      }
      return result;
    },
    onSuccess: onChanged,
  });

  // canApprove/canReject используют `id` ПОСЛЕДНЕГО события журнала как
  // `request_id` — API-схема TaskEvent не отдаёт `ref_event_id`, тот же приём
  // позиционного определения "на какой запрос отвечаем", что и у canAnswer/
  // AnswerQuestionForm выше (самый свежий запрос в упорядоченном по `seq`
  // журнале).
  const approveMutation = useMutation({
    mutationFn: async () => {
      const result = await apiClient.POST("/tasks/{id}/approve", {
        params: { path: { id: taskId } },
        body: { request_id: lastEvent?.id ?? "", decision: "approve" },
      });
      if (!result.response.ok) {
        throw new Error("failed to approve command");
      }
      return result;
    },
    onSuccess: onChanged,
  });

  const rejectMutation = useMutation({
    mutationFn: async () => {
      const result = await apiClient.POST("/tasks/{id}/approve", {
        params: { path: { id: taskId } },
        body: { request_id: lastEvent?.id ?? "", decision: "reject" },
      });
      if (!result.response.ok) {
        throw new Error("failed to reject command");
      }
      return result;
    },
    onSuccess: onChanged,
  });

  const canCancel = isActive;
  const canConfirm = status === "awaiting_confirm";
  const canAnswer =
    status === "waiting_user" && lastEvent?.type === "agent_question";
  const canApprove =
    status === "waiting_user" &&
    lastEvent?.type === "command_approval_request";

  if (!canCancel && !canConfirm && !canAnswer && !canApprove) {
    return null;
  }

  return (
    <div className="flex flex-col gap-4">
      {(canCancel || canConfirm) && (
        <div className="flex flex-wrap gap-2">
          {canConfirm && (
            <Button
              disabled={confirmMutation.isPending}
              onClick={() => confirmMutation.mutate()}
            >
              {confirmMutation.isPending
                ? "Подтверждаем…"
                : "Подтвердить завершение"}
            </Button>
          )}
          {canCancel && (
            <Button
              variant="destructive"
              disabled={cancelMutation.isPending}
              onClick={() => {
                if (window.confirm("Отменить задачу?")) {
                  cancelMutation.mutate();
                }
              }}
            >
              {cancelMutation.isPending ? "Отменяем…" : "Отменить"}
            </Button>
          )}
        </div>
      )}
      {confirmMutation.isError && (
        <p role="alert" className="text-sm text-destructive">
          Не удалось подтвердить завершение. Попробуйте ещё раз.
        </p>
      )}
      {cancelMutation.isError && (
        <p role="alert" className="text-sm text-destructive">
          Не удалось отменить задачу. Попробуйте ещё раз.
        </p>
      )}
      {canAnswer && lastEvent && (
        <AnswerQuestionForm
          taskId={taskId}
          questionEventId={lastEvent.id ?? ""}
          questionText={describePayload(lastEvent.payload)}
          onAnswered={onChanged}
        />
      )}
      {canApprove && lastEvent && (
        <div className="flex flex-col gap-3 rounded-md border border-border p-4">
          <h2 className="text-lg font-semibold">
            Согласование команды
          </h2>
          {describePayload(lastEvent.payload) && (
            <p className="text-sm">{describePayload(lastEvent.payload)}</p>
          )}
          <div className="flex flex-wrap gap-2">
            <Button
              disabled={approveMutation.isPending}
              onClick={() => approveMutation.mutate()}
            >
              {approveMutation.isPending ? "Одобряем…" : "Одобрить"}
            </Button>
            <Button
              variant="destructive"
              disabled={rejectMutation.isPending}
              onClick={() => rejectMutation.mutate()}
            >
              {rejectMutation.isPending ? "Отклоняем…" : "Отклонить"}
            </Button>
          </div>
          {approveMutation.isError && (
            <p role="alert" className="text-sm text-destructive">
              Не удалось одобрить команду. Попробуйте ещё раз.
            </p>
          )}
          {rejectMutation.isError && (
            <p role="alert" className="text-sm text-destructive">
              Не удалось отклонить команду. Попробуйте ещё раз.
            </p>
          )}
        </div>
      )}
    </div>
  );
}

/**
 * Карточка задачи с журналом событий (`/tasks/{id}`, тикеты 9.4 + 8.8 + 9.5,
 * FR H1, H2, F1, G1, E2, Gherkin §10 «История»/«Действия из истории в web» и
 * §5 «Вопрос-ответ и согласование команд»).
 *
 * Интерактивные действия (отмена, ответ на вопрос агента, подтверждение
 * завершения, одобрение/отклонение команды вне allowlist) доступны условно по
 * текущему статусу задачи, см. `TaskActions`. Отклонение результата на
 * доработку — отдельный механизм, вне объёма.
 *
 * Живое обновление по WebSocket (тикет 9.5 «Живой диалог»): `lastFrame` из
 * `NotificationContext` (единственное WS-соединение пользователя, см. этот
 * контекст и `Layout`) сравнивается с открытой задачей (`id` из URL) — если
 * совпало, вызывается та же инвалидация react-query, что и после успешных
 * действий (`handleActionSuccess`), без перезагрузки страницы.
 */
export function TaskDetailPage(): JSX.Element {
  const { id } = useParams<{ id: string }>();
  const queryClient = useQueryClient();
  const { lastFrame } = useNotificationContext();

  const taskQuery = useQuery({
    queryKey: ["tasks", id],
    queryFn: async () => {
      const { data, error, response } = await apiClient.GET("/tasks/{id}", {
        params: { path: { id: id ?? "" } },
      });
      if (response.status === 404) {
        // Не ошибка транспорта — просто нет такой задачи (удалена/неверный id).
        return null;
      }
      if (error || !data) {
        throw error ?? new Error("failed to load task");
      }
      return data;
    },
    enabled: Boolean(id),
  });

  const eventsQuery = useQuery({
    queryKey: ["tasks", id, "events"],
    queryFn: async () => {
      const { data, error } = await apiClient.GET("/tasks/{id}/events", {
        params: { path: { id: id ?? "" } },
      });
      if (error) {
        throw error;
      }
      return data ?? [];
    },
    enabled: Boolean(id) && Boolean(taskQuery.data),
  });

  function handleActionSuccess() {
    void queryClient.invalidateQueries({ queryKey: ["tasks", id] });
    void queryClient.invalidateQueries({ queryKey: ["tasks", id, "events"] });
    void queryClient.invalidateQueries({ queryKey: ["tasks"] });
  }

  // Живой диалог (тикет 9.5): любое уведомление по WS про ЭТУ задачу
  // (новый вопрос, запрос согласования команды, подтверждение завершения)
  // триггерит тот же рефетч, что и успешное действие пользователя — карточка
  // обновляется, пока вкладка открыта на нужной задаче, без перезагрузки.
  useEffect(() => {
    if (lastFrame && lastFrame.task_id === id) {
      handleActionSuccess();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps -- реагируем строго на новый lastFrame/смену открытой задачи, handleActionSuccess пересоздаётся каждый рендер
  }, [lastFrame, id]);

  const lastEvent = eventsQuery.data?.[eventsQuery.data.length - 1];

  return (
    <section className="flex flex-col gap-6">
      <Link
        to="/tasks"
        className="text-sm text-muted-foreground hover:underline"
      >
        ← Назад к списку задач
      </Link>

      {taskQuery.isLoading && (
        <p className="text-muted-foreground">Загрузка…</p>
      )}
      {taskQuery.isError && (
        <p className="text-sm text-destructive">Не удалось загрузить задачу.</p>
      )}
      {taskQuery.isSuccess && taskQuery.data === null && (
        <p className="text-muted-foreground">Задача не найдена.</p>
      )}

      {taskQuery.data && (
        <>
          <div className="rounded-md border border-border p-4">
            <h1 className="text-2xl font-bold">Задача</h1>
            <p className="mt-2">{taskQuery.data.text}</p>
            <dl className="mt-4 grid grid-cols-[auto,1fr] gap-x-4 gap-y-1 text-sm text-muted-foreground">
              <dt>Статус</dt>
              <dd>
                {taskQuery.data.status
                  ? TASK_STATUS_LABELS[taskQuery.data.status]
                  : "—"}
              </dd>
              <dt>Машина</dt>
              <dd className="break-all font-mono">
                {taskQuery.data.integration_id}
              </dd>
              <dt>Создана</dt>
              <dd>
                {taskQuery.data.created_at
                  ? new Date(taskQuery.data.created_at).toLocaleString()
                  : "—"}
              </dd>
              <dt>Обновлена</dt>
              <dd>
                {taskQuery.data.updated_at
                  ? new Date(taskQuery.data.updated_at).toLocaleString()
                  : "—"}
              </dd>
            </dl>
          </div>

          {id && (
            <TaskActions
              taskId={id}
              status={taskQuery.data.status}
              lastEvent={lastEvent}
              onChanged={handleActionSuccess}
            />
          )}

          <div>
            <h2 className="text-lg font-semibold">Журнал событий</h2>
            {eventsQuery.isLoading && (
              <p className="mt-2 text-muted-foreground">Загрузка…</p>
            )}
            {eventsQuery.isError && (
              <p className="mt-2 text-sm text-destructive">
                Не удалось загрузить журнал событий.
              </p>
            )}
            {eventsQuery.data && eventsQuery.data.length === 0 && (
              <p className="mt-2 text-muted-foreground">Событий пока нет.</p>
            )}
            {eventsQuery.data && eventsQuery.data.length > 0 && (
              <ul className="mt-2 flex flex-col gap-3">
                {eventsQuery.data.map((event) => (
                  <TaskEventRow key={event.id} event={event} />
                ))}
              </ul>
            )}
          </div>
        </>
      )}
    </section>
  );
}
