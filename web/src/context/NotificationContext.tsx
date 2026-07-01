import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";

import { useAuth } from "@/context/AuthContext";
import { useWebSocket } from "@/hooks/useWebSocket";
import { getAccessToken } from "@/lib/tokenStore";

/** Разобранный кадр уведомления, приходящий от `/ws` (см. `client_ws.go`, `clientNotificationFrame`). */
export interface NotificationFrame {
  kind: string;
  task_id: string;
  created_at: string;
}

/** Уведомление, показанное в UI, с локальным ключом для React-списка. */
export interface NotificationItem {
  id: string;
  kind: string;
  taskId: string;
  createdAt: string;
}

interface NotificationContextValue {
  /** Накопленные непрочитанные уведомления (для баннера, тикет 7.2). */
  notifications: NotificationItem[];
  /** Убрать уведомление из списка (клик по крестику баннера). */
  dismiss: (id: string) => void;
  /**
   * Последний распарсенный кадр целиком, включая `task_id` (тикет 9.5) — чтобы
   * экраны вроде `TaskDetailPage` могли сравнить его с открытой задачей и
   * рефетчнуть данные без перезагрузки страницы. `null`, пока не пришло ни
   * одного кадра за время жизни соединения.
   */
  lastFrame: NotificationFrame | null;
}

const NotificationContext = createContext<NotificationContextValue | null>(
  null,
);

/** Строит `ws(s)://<текущий origin>/api/ws` — тот же `/api`-префикс, что и REST (`src/api/client.ts`), Caddy снимает его на пути к оркестратору. */
function buildWsUrl(): string {
  const wsScheme = window.location.protocol === "https:" ? "wss:" : "ws:";
  return `${wsScheme}//${window.location.host}/api/ws`;
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
 * Единственное WS-соединение `/ws` пользователя, разделяемое всеми экранами
 * (тикет 9.5, "Живой диалог", FR F1/G1/E2).
 *
 * Назначение (бизнес): раньше (тикет 7.2) соединение открывал только
 * `NotificationBanner` — этого хватало для баннера, но `TaskDetailPage`
 * (тикет 9.4/8.8) тоже должен обновляться в реальном времени (новый вопрос,
 * запрос согласования команды, подтверждение завершения), не открывая
 * ВТОРОЕ независимое соединение на того же пользователя (лишняя нагрузка на
 * `ClientConnHub`, см. `orchestrator/internal/api/client_ws.go`). Этот
 * контекст — единственный владелец соединения; и баннер, и карточка задачи
 * читают из него.
 *
 * Как устроено (тех): переиспользует `useWebSocket` (тикет 9.1); auth-хэндшейк
 * — тот же, что был в `NotificationBanner` (тикет 7.2): соединение открывается
 * только когда пользователь вошёл И есть access-токен, сразу после
 * `status === "open"` первым кадром отправляется
 * `{"type":"auth","access_token":...}` (браузерный WebSocket API не умеет
 * прокинуть `Authorization`-заголовок на хендшейке), `authSentForRef`
 * предохраняет от повторной отправки auth на одно и то же соединение.
 *
 * Провайдер монтируется в `Layout` (тикет 9.1) — на том же уровне жизненного
 * цикла, на котором раньше жил `NotificationBanner`: живёт, пока пользователь
 * залогинен, закрывается вместе с `Layout` при логауте/редиректе на `/login`.
 */
export function NotificationProvider({
  children,
}: {
  children: ReactNode;
}): JSX.Element {
  const { isAuthenticated } = useAuth();
  const [notifications, setNotifications] = useState<NotificationItem[]>([]);
  const [lastFrame, setLastFrame] = useState<NotificationFrame | null>(null);

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
    setLastFrame(frame);
    setNotifications((prev) => [
      ...prev,
      {
        id: `${frame.task_id}-${frame.created_at}-${prev.length}`,
        kind: frame.kind,
        taskId: frame.task_id,
        createdAt: frame.created_at,
      },
    ]);
    // eslint-disable-next-line react-hooks/exhaustive-deps -- реагируем строго на новое сообщение, не на смену setNotifications/setLastFrame
  }, [lastMessage]);

  const dismiss = useCallback((id: string) => {
    setNotifications((prev) => prev.filter((item) => item.id !== id));
  }, []);

  const value = useMemo<NotificationContextValue>(
    () => ({ notifications, dismiss, lastFrame }),
    [notifications, dismiss, lastFrame],
  );

  return (
    <NotificationContext.Provider value={value}>
      {children}
    </NotificationContext.Provider>
  );
}

/** Доступ к состоянию уведомлений; должен использоваться внутри `NotificationProvider`. */
export function useNotificationContext(): NotificationContextValue {
  const ctx = useContext(NotificationContext);
  if (!ctx) {
    throw new Error(
      "useNotificationContext() должен вызываться внутри <NotificationProvider>",
    );
  }
  return ctx;
}
