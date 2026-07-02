import { useNotificationContext } from "@/context/NotificationContext";

/**
 * Виды уведомлений (`orchestrator/internal/notify/notify.go`, `Kind`), для
 * которых у нас есть человекочитаемый текст. Значения строго совпадают со
 * значениями на бэкенде — FR G1:
 *   - `notify.KindAgentQuestion` (тикет 7.1/6.1, FR F1);
 *   - `notify.KindCommandApprovalRequest` (тикет 9.5/6.4, FR F1/F3);
 *   - `notify.KindAgentCompleted` (тикет 9.5/8.1, FR E2).
 */
const KIND_AGENT_QUESTION = "agent_question";
const KIND_COMMAND_APPROVAL_REQUEST = "command_approval_request";
const KIND_AGENT_COMPLETED = "agent_completed";

function notificationText(kind: string): string {
  if (kind === KIND_AGENT_QUESTION) {
    return "Агент задал вопрос по задаче";
  }
  if (kind === KIND_COMMAND_APPROVAL_REQUEST) {
    return "Агент запросил согласование команды";
  }
  if (kind === KIND_AGENT_COMPLETED) {
    return "Задача ожидает вашего подтверждения завершения";
  }
  return "Новое уведомление";
}

/**
 * Уведомления web-канала без перезагрузки страницы (тикет 7.2, дополнено
 * тикетом 9.5 "Живой диалог", FR F1/G1/E2, §6 «Уведомление в web по
 * WebSocket»).
 *
 * Назначение (бизнес): пока вкладка открыта, пользователь видит уведомление
 * («агент задал вопрос по задаче», «запросил согласование команды», «задача
 * ожидает подтверждения завершения») сразу, без обновления страницы —
 * приёмочный Gherkin-сценарий тикета («Дано у меня открыт веб-интерфейс /
 * Когда агент задаёт вопрос / Тогда я вижу уведомление в web без
 * перезагрузки»). История уведомлений, отметка «прочитано», переход по клику
 * на задачу — вне объёма этого тикета (см. docs/MVP_TICKETS.md).
 *
 * Как устроено (тех): само WS-соединение и разбор кадров теперь живут в
 * `NotificationContext` (тикет 9.5) — общий для этого баннера и
 * `TaskDetailPage` (который реагирует на те же кадры для live-рефетча, не
 * открывая второе соединение). Этот компонент — чистый потребитель
 * `useNotificationContext()`, только рендерит список и умеет `dismiss`.
 */
export function NotificationBanner(): JSX.Element | null {
  const { notifications, dismiss } = useNotificationContext();

  if (notifications.length === 0) {
    return null;
  }

  return (
    <div className="fixed right-4 top-4 z-50 flex w-full max-w-sm flex-col gap-2">
      {notifications.map((item) => (
        <div
          key={item.id}
          role="status"
          className="flex items-center justify-between gap-3 rounded-md border border-border bg-card px-4 py-3 text-sm text-card-foreground shadow-md"
        >
          <span>{notificationText(item.kind)}</span>
          <button
            type="button"
            aria-label="Закрыть уведомление"
            onClick={() => dismiss(item.id)}
            className="text-muted-foreground hover:text-foreground"
          >
            ×
          </button>
        </div>
      ))}
    </div>
  );
}
