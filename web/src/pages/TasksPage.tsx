import { useMemo, useState } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { useForm } from "react-hook-form";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { z } from "zod";

import { apiClient } from "@/api/client";
import type { components } from "@/api/schema";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { TASK_STATUS_LABELS } from "@/lib/taskStatus";

type Task = components["schemas"]["Task"];
type TaskStatus = components["schemas"]["TaskStatus"];
type Integration = components["schemas"]["Integration"];

/** React Query ключ списка задач — единая точка инвалидации после постановки. */
const TASKS_QUERY_KEY = ["tasks"] as const;
const INTEGRATIONS_QUERY_KEY = ["integrations"] as const;

/** Общие стили нативного `<select>` — в проекте нет своего select-примитива
 * (см. `components/ui/`), поэтому оформляем как `Input` (тикет 9.3 уже
 * обходится нативным `window.confirm()` вместо модалки по той же причине —
 * без новых UI-зависимостей). */
const selectClassName =
  "flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50";

const taskFormSchema = z.object({
  integration_id: z.string().min(1, "Выберите машину"),
  text: z.string().min(1, "Введите текст запроса"),
});

type TaskFormValues = z.infer<typeof taskFormSchema>;

/**
 * Форма постановки задачи (тикет 9.4, FR E1; Gherkin §4 «Постановка задачи»,
 * «Защита от двойной отправки»).
 *
 * `Idempotency-Key` генерируется один раз на ПОПЫТКУ постановки — при
 * монтировании формы (она размонтируется/монтируется заново при
 * закрытии/открытии, см. `TasksPage`), а не на каждый клик Submit. Если бы
 * ключ генерировался на клик, случайный повторный клик (двойное нажатие,
 * сетевой ретрай) отправил бы второй POST с НОВЫМ ключом и создал бы вторую
 * задачу вместо срабатывания дедупа на бэкенде (FR E7).
 */
function CreateTaskForm({
  integrations,
  onCreated,
  onCancel,
}: {
  integrations: Integration[];
  onCreated: (created: Task) => void;
  onCancel: () => void;
}): JSX.Element {
  const [formError, setFormError] = useState<string | null>(null);
  const [idempotencyKey] = useState(() => crypto.randomUUID());
  const {
    register,
    handleSubmit,
    formState: { errors, isSubmitting },
  } = useForm<TaskFormValues>({
    resolver: zodResolver(taskFormSchema),
  });

  const onSubmit = handleSubmit(async (values) => {
    setFormError(null);
    const { data, error } = await apiClient.POST("/tasks", {
      params: { header: { "Idempotency-Key": idempotencyKey } },
      body: { integration_id: values.integration_id, text: values.text },
    });
    if (error || !data) {
      setFormError("Не удалось поставить задачу. Попробуйте ещё раз.");
      return;
    }
    onCreated(data);
  });

  return (
    <form
      className="flex flex-col gap-4 rounded-md border border-border p-4"
      onSubmit={onSubmit}
      noValidate
    >
      <h2 className="text-lg font-semibold">Новая задача</h2>
      <div className="flex flex-col gap-2">
        <Label htmlFor="task-integration">Машина</Label>
        <select
          id="task-integration"
          className={selectClassName}
          {...register("integration_id")}
        >
          <option value="">Выберите машину…</option>
          {integrations.map((integration) => (
            <option key={integration.id} value={integration.id}>
              {integration.name}
            </option>
          ))}
        </select>
        {errors.integration_id && (
          <p className="text-sm text-destructive">
            {errors.integration_id.message}
          </p>
        )}
      </div>
      <div className="flex flex-col gap-2">
        <Label htmlFor="task-text">Текст запроса</Label>
        <textarea
          id="task-text"
          rows={4}
          className="flex w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2"
          {...register("text")}
        />
        {errors.text && (
          <p className="text-sm text-destructive">{errors.text.message}</p>
        )}
      </div>
      {formError && (
        <p role="alert" className="text-sm text-destructive">
          {formError}
        </p>
      )}
      <div className="flex gap-2">
        <Button type="submit" disabled={isSubmitting}>
          {isSubmitting ? "Ставим…" : "Поставить задачу"}
        </Button>
        <Button type="button" variant="outline" onClick={onCancel}>
          Отмена
        </Button>
      </div>
    </form>
  );
}

/**
 * Строка списка задач — краткая карточка со ссылкой на `/tasks/{id}`
 * (журнал событий и подробности — там, см. `TaskDetailPage`, тикет 9.4).
 */
function TaskRow({
  task,
  integrationName,
}: {
  task: Task;
  integrationName: string;
}): JSX.Element {
  return (
    <li className="rounded-md border border-border p-4 transition-colors hover:bg-accent/50">
      <Link to={`/tasks/${task.id}`} className="flex flex-col gap-2">
        <p className="font-medium">{task.text}</p>
        <div className="flex flex-wrap items-center gap-x-2 gap-y-1 text-sm text-muted-foreground">
          <span>{integrationName}</span>
          <span aria-hidden="true">·</span>
          <span>{task.status ? TASK_STATUS_LABELS[task.status] : "—"}</span>
          <span aria-hidden="true">·</span>
          <span>
            {task.created_at
              ? new Date(task.created_at).toLocaleString()
              : "—"}
          </span>
        </div>
      </Link>
    </li>
  );
}

/**
 * Экран задач (`/tasks`, домашний экран web-канала, тикет 9.4, FR E1, H1;
 * Gherkin §4 «Постановка задачи», §10 «История»).
 *
 * Назначение (бизнес): единственное место, где пользователь ставит новую
 * задачу машине (FR E1) и видит список/историю уже поставленных задач с их
 * статусом (FR H1) — переход в карточку конкретной задачи (журнал событий)
 * см. `TaskDetailPage`.
 *
 * Как устроено (тех): тот же паттерн, что `IntegrationsPage` (тикет 9.3) —
 * `useQuery` для списка (`GET /tasks`), `invalidateQueries` после успешной
 * постановки вместо ручного локального стейта списка. Интерактивные
 * действия над задачей (ответ на вопрос агента, согласование команды,
 * отмена, подтверждение завершения) и живое обновление списка/карточки по
 * WebSocket — вне объёма этого тикета (см. тикеты 8.8 и 9.5
 * соответственно); здесь — только постановка, список и обычный
 * react-query-рефетч по действию пользователя.
 */
export function TasksPage(): JSX.Element {
  const queryClient = useQueryClient();
  const [isCreateFormOpen, setIsCreateFormOpen] = useState(false);
  const [statusFilter, setStatusFilter] = useState<TaskStatus | "">("");

  const integrationsQuery = useQuery({
    queryKey: INTEGRATIONS_QUERY_KEY,
    queryFn: async () => {
      const { data, error } = await apiClient.GET("/integrations");
      if (error) {
        throw error;
      }
      return data ?? [];
    },
  });

  const tasksQuery = useQuery({
    queryKey: [...TASKS_QUERY_KEY, statusFilter],
    queryFn: async () => {
      const { data, error } = await apiClient.GET("/tasks", {
        params: { query: statusFilter ? { status: statusFilter } : {} },
      });
      if (error) {
        throw error;
      }
      return data ?? [];
    },
  });

  const integrationNameById = useMemo(() => {
    const map = new Map<string, string>();
    for (const integration of integrationsQuery.data ?? []) {
      if (integration.id) {
        map.set(integration.id, integration.name ?? integration.id);
      }
    }
    return map;
  }, [integrationsQuery.data]);

  function invalidateTasks() {
    void queryClient.invalidateQueries({ queryKey: TASKS_QUERY_KEY });
  }

  return (
    <section className="flex flex-col gap-6">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div>
          <h1 className="text-2xl font-bold">Задачи</h1>
          <p className="mt-2 text-muted-foreground">
            Постановка задач машинам и история их выполнения.
          </p>
        </div>
        {!isCreateFormOpen && (
          <Button onClick={() => setIsCreateFormOpen(true)}>
            Новая задача
          </Button>
        )}
      </div>

      {isCreateFormOpen && (
        <CreateTaskForm
          integrations={integrationsQuery.data ?? []}
          onCreated={() => {
            setIsCreateFormOpen(false);
            invalidateTasks();
          }}
          onCancel={() => setIsCreateFormOpen(false)}
        />
      )}

      <div className="flex items-center gap-2">
        <Label htmlFor="task-status-filter">Статус</Label>
        <select
          id="task-status-filter"
          className={`${selectClassName} w-auto`}
          value={statusFilter}
          onChange={(event) =>
            setStatusFilter(event.target.value as TaskStatus | "")
          }
        >
          <option value="">Все</option>
          {(Object.entries(TASK_STATUS_LABELS) as [TaskStatus, string][]).map(
            ([value, label]) => (
              <option key={value} value={value}>
                {label}
              </option>
            ),
          )}
        </select>
      </div>

      {tasksQuery.isLoading && (
        <p className="text-muted-foreground">Загрузка…</p>
      )}
      {tasksQuery.isError && (
        <p className="text-sm text-destructive">
          Не удалось загрузить список задач.
        </p>
      )}
      {tasksQuery.data && tasksQuery.data.length === 0 && (
        <p className="text-muted-foreground">
          Задач пока нет — поставьте первую.
        </p>
      )}

      {tasksQuery.data && tasksQuery.data.length > 0 && (
        <ul className="flex flex-col gap-3">
          {tasksQuery.data.map((task) => (
            <TaskRow
              key={task.id}
              task={task}
              integrationName={
                task.integration_id
                  ? (integrationNameById.get(task.integration_id) ??
                    task.integration_id)
                  : "—"
              }
            />
          ))}
        </ul>
      )}
    </section>
  );
}
