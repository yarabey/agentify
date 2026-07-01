import { useQuery } from "@tanstack/react-query";
import { Link, useParams } from "react-router-dom";

import { apiClient } from "@/api/client";
import type { components } from "@/api/schema";
import { TASK_STATUS_LABELS } from "@/lib/taskStatus";

type TaskEvent = components["schemas"]["TaskEvent"];

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
 * Карточка задачи с журналом событий (`/tasks/{id}`, тикет 9.4, FR H1,
 * Gherkin §10 «История»).
 *
 * Read-only: интерактивные действия над задачей (ответ на вопрос агента,
 * согласование/отклонение команды, отмена, подтверждение/отклонение
 * завершения — `POST /tasks/{id}/answer|approve|reject|cancel|confirm`)
 * сознательно не реализованы — это отдельный тикет 8.8 «Действия из истории
 * в web». Живое обновление по WebSocket (авто-refetch по приходу события) —
 * тикет 9.5 «Живой диалог»; здесь — обычный react-query без подписки на WS,
 * данные актуальны на момент открытия карточки.
 */
export function TaskDetailPage(): JSX.Element {
  const { id } = useParams<{ id: string }>();

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
