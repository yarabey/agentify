import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";

import { apiClient } from "@/api/client";
import { AuthProvider } from "@/context/AuthContext";
import { getAccessToken, getRefreshToken } from "@/lib/tokenStore";
import { LoginPage } from "@/pages/LoginPage";

/**
 * Тесты флоу логина (тикет 9.2, DoD "RTL — флоу логина").
 *
 * `apiClient.POST` мокается напрямую (`vi.spyOn`), а не глобальный `fetch`:
 * `openapi-fetch` захватывает `globalThis.fetch` в момент создания клиента
 * (см. `node_modules/openapi-fetch/src/index.js`, `fetch: baseFetch =
 * globalThis.fetch`) — на момент выполнения тела теста `client.ts` уже
 * импортирован (статические импорты исполняются раньше кода теста), поэтому
 * подмена глобального `fetch` в теле теста на сам вызов уже не повлияла бы.
 * Мок на уровне `apiClient.POST` — простой и надёжный способ подставить
 * ответ бэкенда без борьбы с этим порядком инициализации; сквозной
 * авто-refresh-мидларь (реальный `fetch`) отдельно проверяется в
 * `src/api/client.test.ts`.
 */
function renderLoginPage() {
  const queryClient = new QueryClient();
  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter initialEntries={["/login"]}>
        <AuthProvider>
          <Routes>
            <Route path="/login" element={<LoginPage />} />
            <Route path="/tasks" element={<div>Экран задач (заглушка)</div>} />
          </Routes>
        </AuthProvider>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

function fillLoginForm(username: string, password: string) {
  fireEvent.change(screen.getByLabelText("Имя пользователя"), {
    target: { value: username },
  });
  fireEvent.change(screen.getByLabelText("Пароль"), {
    target: { value: password },
  });
}

describe("LoginPage (тикет 9.2, FR A3)", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    localStorage.clear();
  });

  it("успешный логин сохраняет токены и переводит на /tasks", async () => {
    vi.spyOn(apiClient, "POST").mockResolvedValue({
      data: {
        access_token: "issued-access-token",
        refresh_token: "issued-refresh-token",
        expires_in: 900,
      },
      error: undefined,
      response: new Response(null, { status: 200 }),
    } as never);

    renderLoginPage();
    fillLoginForm("alice", "correct-horse-battery-staple");
    fireEvent.click(screen.getByRole("button", { name: "Войти" }));

    await waitFor(() => {
      expect(
        screen.getByText("Экран задач (заглушка)"),
      ).toBeInTheDocument();
    });

    expect(apiClient.POST).toHaveBeenCalledWith(
      "/auth/login",
      expect.objectContaining({
        body: { username: "alice", password: "correct-horse-battery-staple" },
      }),
    );
    expect(getAccessToken()).toBe("issued-access-token");
    expect(getRefreshToken()).toBe("issued-refresh-token");
  });

  it("неверные креды показывают общее сообщение, не различая логин/пароль", async () => {
    vi.spyOn(apiClient, "POST").mockResolvedValue({
      data: undefined,
      error: { code: "invalid_credentials", message: "неверные учётные данные" },
      response: new Response(null, { status: 401 }),
    } as never);

    renderLoginPage();
    fillLoginForm("alice", "wrong-password");
    fireEvent.click(screen.getByRole("button", { name: "Войти" }));

    expect(
      await screen.findByText("Неверный логин или пароль"),
    ).toBeInTheDocument();

    // Остаёмся на экране логина, токены не сохранены.
    expect(screen.queryByText("Экран задач (заглушка)")).not.toBeInTheDocument();
    expect(getAccessToken()).toBeNull();
    expect(getRefreshToken()).toBeNull();
  });

  it("не отправляет форму с пустыми полями (клиентская валидация)", async () => {
    const postSpy = vi.spyOn(apiClient, "POST");

    renderLoginPage();
    fireEvent.click(screen.getByRole("button", { name: "Войти" }));

    expect(await screen.findByText("Введите имя пользователя")).toBeInTheDocument();
    expect(screen.getByText("Введите пароль")).toBeInTheDocument();
    expect(postSpy).not.toHaveBeenCalled();
  });
});
