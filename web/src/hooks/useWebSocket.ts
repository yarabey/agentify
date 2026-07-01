import { useCallback, useEffect, useRef, useState } from "react";

/** Статус соединения, отдаваемый {@link useWebSocket}. */
export type WebSocketStatus = "connecting" | "open" | "closed" | "error";

const INITIAL_BACKOFF_MS = 1_000;
const MAX_BACKOFF_MS = 30_000;

/**
 * Переиспользуемый WS-хук с реконнектом и экспоненциальным бэкоффом
 * (тикет 9.1, каркас PWA, FR D1/F1/G1).
 *
 * Назначение (бизнес): общая инфраструктура для "живой трубы" web-канала —
 * доставки уведомлений и диалога с агентом в реальном времени, пока вкладка
 * открыта (см. docs/01_tech_stack_and_architecture.md, "Web push в MVP вне
 * объёма... уведомления в web — только по WebSocket при открытой вкладке").
 * Сам эндпоинт "web по WebSocket" ещё не существует на бэкенде — это
 * отдельный будущий тикет **7.2**; хук будет подключён к реальному URL там же
 * и в тикете **9.5** ("Живой диалог"). В этом тикете хук НЕ подключается ни к
 * одному экрану — только экспортируется готовым к использованию.
 *
 * Как устроено (тех): принимает `url` (может быть `null`, чтобы намеренно не
 * коннектиться, например пока пользователь не залогинен); при получении
 * ненулевого `url` открывает `WebSocket`, отслеживает открытие/закрытие/ошибку
 * и переподключается с экспоненциальным бэкоффом (база 1с, максимум 30с,
 * счётчик попыток сбрасывается при успешном `onopen`). Таймеры и сокет
 * закрываются при размонтировании компонента или смене `url`, чтобы не текли
 * соединения/реконнект-таймеры при навигации между экранами.
 */
export interface UseWebSocketResult {
  /** Текущий статус соединения. */
  status: WebSocketStatus;
  /** Последнее полученное сообщение (сырые данные события `message`), либо `null`, если сообщений ещё не было. */
  lastMessage: MessageEvent["data"] | null;
  /** Отправить сообщение через сокет; no-op, если соединение не открыто. */
  send: (data: string | ArrayBufferLike | Blob | ArrayBufferView) => void;
}

export function useWebSocket(url: string | null): UseWebSocketResult {
  const [status, setStatus] = useState<WebSocketStatus>(
    url ? "connecting" : "closed",
  );
  const [lastMessage, setLastMessage] = useState<MessageEvent["data"] | null>(
    null,
  );

  const socketRef = useRef<WebSocket | null>(null);
  const reconnectTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const attemptRef = useRef(0);
  // Флаг "компонент/эффект ещё жив" — предохраняет от установки состояния и
  // от планирования реконнекта после cleanup (смена url/размонтирование).
  const activeRef = useRef(true);

  const send = useCallback<UseWebSocketResult["send"]>((data) => {
    const socket = socketRef.current;
    if (socket && socket.readyState === WebSocket.OPEN) {
      socket.send(data);
    }
  }, []);

  useEffect(() => {
    activeRef.current = true;

    if (!url) {
      setStatus("closed");
      return () => {
        activeRef.current = false;
      };
    }

    const clearReconnectTimer = () => {
      if (reconnectTimerRef.current !== null) {
        clearTimeout(reconnectTimerRef.current);
        reconnectTimerRef.current = null;
      }
    };

    const connect = () => {
      if (!activeRef.current) return;

      setStatus("connecting");
      const socket = new WebSocket(url);
      socketRef.current = socket;

      socket.onopen = () => {
        if (!activeRef.current) return;
        attemptRef.current = 0; // успешное открытие сбрасывает бэкофф
        setStatus("open");
      };

      socket.onmessage = (event) => {
        if (!activeRef.current) return;
        setLastMessage(event.data);
      };

      socket.onerror = () => {
        if (!activeRef.current) return;
        setStatus("error");
      };

      socket.onclose = () => {
        socketRef.current = null;
        if (!activeRef.current) return;
        setStatus("closed");

        const delay = Math.min(
          INITIAL_BACKOFF_MS * 2 ** attemptRef.current,
          MAX_BACKOFF_MS,
        );
        attemptRef.current += 1;
        reconnectTimerRef.current = setTimeout(connect, delay);
      };
    };

    connect();

    return () => {
      activeRef.current = false;
      clearReconnectTimer();
      const socket = socketRef.current;
      socketRef.current = null;
      if (socket) {
        // Снимаем обработчики, чтобы закрытие "старого" сокета не пыталось
        // переподключиться/обновить состояние уже отмонтированного хука.
        socket.onopen = null;
        socket.onmessage = null;
        socket.onerror = null;
        socket.onclose = null;
        socket.close();
      }
    };
  }, [url]);

  return { status, lastMessage, send };
}
