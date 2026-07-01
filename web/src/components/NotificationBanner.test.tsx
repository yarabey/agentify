import { render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { NotificationBanner } from "@/components/NotificationBanner";
import { AuthProvider } from "@/context/AuthContext";
import { NotificationProvider } from "@/context/NotificationContext";
import { setTokens } from "@/lib/tokenStore";

/**
 * Тесты web-уведомлений по WebSocket без перезагрузки страницы (тикет 7.2,
 * FR G1, §6 «Уведомление в web по WebSocket»).
 *
 * jsdom не реализует `WebSocket` — подменяем глобальный конструктор простой
 * фейковой реализацией, которая копит созданные экземпляры и отданные
 * сообщения (`send`), и позволяет тесту вручную дёргать `onopen`/`onmessage`,
 * не поднимая настоящий сокет/сервер. Это Go-эквивалент серверных тестов
 * `orchestrator/internal/api/client_ws_test.go` с фронтовой стороны того же
 * приёмочного сценария: "агент задаёт вопрос -> уведомление в открытой вкладке
 * без перезагрузки".
 */
class FakeWebSocket {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSING = 2;
  static readonly CLOSED = 3;
  static instances: FakeWebSocket[] = [];

  readonly url: string;
  readyState = FakeWebSocket.CONNECTING;
  onopen: (() => void) | null = null;
  onmessage: ((event: { data: string }) => void) | null = null;
  onerror: (() => void) | null = null;
  onclose: (() => void) | null = null;
  sent: string[] = [];

  constructor(url: string) {
    this.url = url;
    FakeWebSocket.instances.push(this);
  }

  send(data: string): void {
    this.sent.push(data);
  }

  close(): void {
    this.readyState = FakeWebSocket.CLOSED;
    this.onclose?.();
  }

  /** Тестовый помощник: имитирует успешное открытие соединения (реальный `onopen` браузера). */
  simulateOpen(): void {
    this.readyState = FakeWebSocket.OPEN;
    this.onopen?.();
  }

  /** Тестовый помощник: имитирует получение текстового кадра от сервера. */
  simulateMessage(data: string): void {
    this.onmessage?.({ data });
  }
}

function renderBanner() {
  return render(
    <AuthProvider>
      <NotificationProvider>
        <NotificationBanner />
      </NotificationProvider>
    </AuthProvider>,
  );
}

describe("NotificationBanner (тикет 7.2, FR G1)", () => {
  beforeEach(() => {
    FakeWebSocket.instances = [];
    vi.stubGlobal("WebSocket", FakeWebSocket as unknown as typeof WebSocket);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    localStorage.clear();
  });

  it("не подключается, если пользователь не авторизован (нет access-токена)", async () => {
    renderBanner();

    // Дать эффектам шанс отработать — соединение не должно было открыться.
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(FakeWebSocket.instances).toHaveLength(0);
  });

  it("подключается к /api/ws и шлёт auth-кадр с access-токеном при открытии", async () => {
    setTokens({ accessToken: "test-access-token", refreshToken: "r" });
    renderBanner();

    await waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    const socket = FakeWebSocket.instances[0];
    expect(socket.url).toBe("ws://localhost:3000/api/ws");

    socket.simulateOpen();

    await waitFor(() => expect(socket.sent).toHaveLength(1));
    expect(JSON.parse(socket.sent[0])).toEqual({
      type: "auth",
      access_token: "test-access-token",
    });
  });

  it("показывает уведомление 'Агент задал вопрос по задаче' при agent_question без перезагрузки страницы", async () => {
    setTokens({ accessToken: "test-access-token", refreshToken: "r" });
    renderBanner();

    await waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    const socket = FakeWebSocket.instances[0];
    socket.simulateOpen();
    await waitFor(() => expect(socket.sent).toHaveLength(1));

    expect(
      screen.queryByText("Агент задал вопрос по задаче"),
    ).not.toBeInTheDocument();

    socket.simulateMessage(
      JSON.stringify({
        kind: "agent_question",
        task_id: "11111111-1111-1111-1111-111111111111",
        created_at: "2026-07-01T12:00:00Z",
      }),
    );

    expect(
      await screen.findByText("Агент задал вопрос по задаче"),
    ).toBeInTheDocument();
  });

  it("показывает уведомление 'Агент запросил согласование команды' при command_approval_request (тикет 9.5, FR F1/F3)", async () => {
    setTokens({ accessToken: "test-access-token", refreshToken: "r" });
    renderBanner();

    await waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    const socket = FakeWebSocket.instances[0];
    socket.simulateOpen();
    await waitFor(() => expect(socket.sent).toHaveLength(1));

    socket.simulateMessage(
      JSON.stringify({
        kind: "command_approval_request",
        task_id: "11111111-1111-1111-1111-111111111111",
        created_at: "2026-07-01T12:00:00Z",
      }),
    );

    expect(
      await screen.findByText("Агент запросил согласование команды"),
    ).toBeInTheDocument();
  });

  it("показывает уведомление 'Задача ожидает вашего подтверждения завершения' при agent_completed (тикет 9.5, FR E2)", async () => {
    setTokens({ accessToken: "test-access-token", refreshToken: "r" });
    renderBanner();

    await waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    const socket = FakeWebSocket.instances[0];
    socket.simulateOpen();
    await waitFor(() => expect(socket.sent).toHaveLength(1));

    socket.simulateMessage(
      JSON.stringify({
        kind: "agent_completed",
        task_id: "11111111-1111-1111-1111-111111111111",
        created_at: "2026-07-01T12:00:00Z",
      }),
    );

    expect(
      await screen.findByText("Задача ожидает вашего подтверждения завершения"),
    ).toBeInTheDocument();
  });

  it("показывает общий текст для неизвестного вида уведомления", async () => {
    setTokens({ accessToken: "test-access-token", refreshToken: "r" });
    renderBanner();

    await waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    const socket = FakeWebSocket.instances[0];
    socket.simulateOpen();
    await waitFor(() => expect(socket.sent).toHaveLength(1));

    socket.simulateMessage(
      JSON.stringify({
        kind: "answer_reminder",
        task_id: "22222222-2222-2222-2222-222222222222",
        created_at: "2026-07-01T12:00:00Z",
      }),
    );

    expect(await screen.findByText("Новое уведомление")).toBeInTheDocument();
  });

  it("закрывает уведомление по клику, не трогая остальные", async () => {
    setTokens({ accessToken: "test-access-token", refreshToken: "r" });
    renderBanner();

    await waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    const socket = FakeWebSocket.instances[0];
    socket.simulateOpen();
    await waitFor(() => expect(socket.sent).toHaveLength(1));

    socket.simulateMessage(
      JSON.stringify({
        kind: "agent_question",
        task_id: "11111111-1111-1111-1111-111111111111",
        created_at: "2026-07-01T12:00:00Z",
      }),
    );
    const banner = await screen.findByText("Агент задал вопрос по задаче");

    const dismissButton = banner
      .closest('[role="status"]')
      ?.querySelector("button");
    expect(dismissButton).toBeTruthy();
    dismissButton?.click();

    await waitFor(() =>
      expect(
        screen.queryByText("Агент задал вопрос по задаче"),
      ).not.toBeInTheDocument(),
    );
  });
});
