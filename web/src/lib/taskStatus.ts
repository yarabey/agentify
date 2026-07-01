import type { components } from "@/api/schema";

type TaskStatus = components["schemas"]["TaskStatus"];

/**
 * Человекочитаемые подписи статусов задачи (тикет 9.4, FR H1) — маппинг из
 * `docs/Жизненный цикл задачи.md`. Общий модуль, т.к. нужен и списку задач
 * (`TasksPage`), и карточке задачи (`TaskDetailPage`).
 */
export const TASK_STATUS_LABELS: Record<TaskStatus, string> = {
  created: "Создана",
  queued: "В очереди",
  running: "Выполняется",
  waiting_user: "Ожидает ответа пользователя",
  awaiting_confirm: "Ожидает подтверждения",
  completed: "Завершена",
  failed: "Провалена",
  cancelled: "Отменена",
  stale: "Зависла (таймаут)",
};
