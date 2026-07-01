import { useEffect, useRef, useState } from "react";

import { useAuth } from "@/context/AuthContext";
import { useWebSocket } from "@/hooks/useWebSocket";
import { getAccessToken } from "@/lib/tokenStore";

/**
 * Виды уведомлений (`orchestrator/internal/notify/notify.go`, `Kind`), для
 * которых у нас есть человекочитаемый текст. Значение строго совпадает со
 * значением на бэкенде (`notify.KindAgentQuestion`) — тикет 7.2, FR G1.
 */
const KIND_AGENT_QUESTION = "agent_question";

/** Разобранный кадр уведомления, приходящий от `/ws` (см. `client_ws.go`, `clientNotificationFrame`). */
interface NotificationFrame {
  kind: string;
  task_id: string;
  created_at: string;
}

/** Уведомление, показанное в UI, с локальным ключом для React-списка. */
interface NotificationItem {
  id: string;
  kind: string;
  taskId: string;
  createdAt: string;
}

/** Строит `ws(s)://<текущий origin>/api/ws` — тот же `/api`-префикс, что и REST (`src/api/client.ts`), Caddy снимает его на пути к оркестратору. */
function buildWsUrl(): string {
  const wsScheme = window.location.protocol === "https:" ? "wss:" : "ws:";
  return `${wsScheme}//${window.location.host}/api/ws`;
}

function notificationText(kind: string): string {
  if (kind === KIND_AGENT_QUESTION) {
    return "Агент задал вопрос по задаче";
  }
  return "Новое уведомление";
}

function parseNotificationFrame(data: unknown): NotificationFrame | null {
  if (typeof data !== "string") {
    return null;
  }
  try {
    const parsed = JSON.parse(data) as Partial<NotificationFrame>;
    if (
      typeof parsed.kind !== "string" ||
      typeof parsed.task_id !== "string" ||
      typeof parsed.created_at !== "string"
    ) {
      return null;
    }
    return {
      kind: parsed.kind,
      task_id: parsed.task_id,
      created_at: parsed.created_at,
    };
  } catch {
    return null;
  }
}

/**
 * Уведомления web-канала без перезагрузки страницы (тикет 7.2, FR G1, §6
 * «Уведомление в web по WebSocket»).
 *
 * Назначение (бизнес): пока вкладка открыта, пользователь видит уведомление
 * («агент задал вопрос по задаче» и т.п.) сразу, без обновления страницы —
 * приёмочный Gherkin-сценарий тикета («Дано у меня открыт веб-интерфейс /
 * Когда агент задаёт вопрос / Тогда я вижу уведомление в web без
 * перезагрузки»). История уведомлений, отметка «прочитано», переход по клику
 * на задачу — вне объёма этого тикета (см. docs/MVP_TICKETS.md).
 *
 * Как устроено (тех): переиспользует `useWebSocket` (тикет 9.1) — соединение
 * открывается только когда пользователь вошёл (`useAuth().isAuthenticated`) И
 * есть access-токен (`getAccessToken()`); иначе в хук передаётся `null` и он
 * не подключается вовсе. Бэкенд не может прочитать `Authorization`-заголовок
 * на WS-хендшейке (браузерный WebSocket API его не поддерживает) — поэтому
 * сразу после `status === "open"` первым кадром отправляется
 * `{"type":"auth","access_token":...}` (см. `authenticateClientWS` в
 * `orchestrator/internal/api/client_ws.go`). Все последующие кадры сервера —
 * уже готовые уведомления (`clientNotificationFrame`: `kind`/`task_id`/
 * `created_at`), они просто накапливаются в списке до явного закрытия
 * пользователем (простой `useState`, без сторонней toast-библиотеки).
 *
 * Компонент рендерится внутри `Layout` (тикет 9.1) — это единственная общая
 * оболочка приватных экранов, не размонтирующаяся при навигации между ними, и
 * размонтирующаяся при логауте/редиректе на `/login` вместе с остальным
 * `Layout` (см. `App.tsx`, `RequireAuth`), что автоматически закрывает
 * соединение.
 */
export function NotificationBanner(): JSX.Element | null {
  const { isAuthenticated } = useAuth();
  const [notifications, setNotifications] = useState<NotificationItem[]>([]);

  const accessToken = isAuthenticated ? getAccessToken() : null;
  const url = accessToken ? buildWsUrl() : null;
  const { status, lastMessage, send } = useWebSocket(url);

  // Отправляем auth-кадр ровно один раз на каждое новое открытие соединения
  // (при реконнекте `useWebSocket` сам создаст новый `status: "open"`).
  const authSentForRef = useRef<string | null>(null);
  useEffect(() => {
    if (status === "open" && accessToken && authSentForRef.current !== url) {
      send(JSON.stringify({ type: "auth", access_token: accessToken }));
      authSentForRef.current = url;
    }
    if (status !== "open") {
      authSentForRef.current = null;
    }
  }, [status, accessToken, url, send]);

  useEffect(() => {
    const frame = parseNotificationFrame(lastMessage);
    if (!frame) {
      return;
    }
    setNotifications((prev) => [
      ...prev,
      {
        id: `${frame.task_id}-${frame.created_at}-${prev.length}`,
        kind: frame.kind,
        taskId: frame.task_id,
        createdAt: frame.created_at,
      },
    ]);
    // eslint-disable-next-line react-hooks/exhaustive-deps -- реагируем строго на новое сообщение, не на смену setNotifications
  }, [lastMessage]);

  function dismiss(id: string): void {
    setNotifications((prev) => prev.filter((item) => item.id !== id));
  }

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
