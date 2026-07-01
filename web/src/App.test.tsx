import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it } from "vitest";

import { App } from "@/App";
import { AuthProvider } from "@/context/AuthContext";
import { setTokens } from "@/lib/tokenStore";

/**
 * Тест рендера оболочки (тикет 9.1, DoD "Vitest — рендер оболочки"; guard'ы
 * из тикета 9.2 добавили измерение "авторизован/нет" — `renderApp` теперь
 * явно управляет им через `authenticated`, т.к. `/`, `/tasks`,
 * `/integrations`, `/settings` теперь под `RequireAuth`, а `/login`,
 * `/register` — под `RequireGuest`, см. `App.tsx`).
 */
function renderApp(initialPath = "/", { authenticated = false } = {}) {
  if (authenticated) {
    setTokens({ accessToken: "test-access", refreshToken: "test-refresh" });
  }
  const queryClient = new QueryClient();
  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter initialEntries={[initialPath]}>
        <AuthProvider>
          <App />
        </AuthProvider>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe("App shell", () => {
  afterEach(() => {
    localStorage.clear();
  });

  it("рендерит шапку с навигацией и домашний экран задач по умолчанию (авторизован)", () => {
    renderApp("/", { authenticated: true });

    expect(screen.getByText("Agentify")).toBeInTheDocument();
    expect(
      screen.getByRole("link", { name: "Задачи" }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("link", { name: "Интеграции" }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("link", { name: "Настройки" }),
    ).toBeInTheDocument();

    // "/" редиректит на домашний экран "/tasks".
    expect(
      screen.getByRole("heading", { name: "Задачи" }),
    ).toBeInTheDocument();
  });

  it("рендерит экран /login вне общей навигационной оболочки (гость)", () => {
    renderApp("/login");

    expect(
      screen.getByRole("heading", { name: "Вход" }),
    ).toBeInTheDocument();
    expect(screen.queryByText("Agentify")).not.toBeInTheDocument();
  });

  it("рендерит заглушки /integrations и /settings (авторизован)", () => {
    const { unmount } = renderApp("/integrations", { authenticated: true });
    expect(
      screen.getByRole("heading", { name: "Интеграции" }),
    ).toBeInTheDocument();
    unmount();

    renderApp("/settings", { authenticated: true });
    expect(
      screen.getByRole("heading", { name: "Настройки" }),
    ).toBeInTheDocument();
  });

  it("редиректит неавторизованного пользователя с приватных экранов на /login (FR A3)", () => {
    renderApp("/integrations");

    expect(
      screen.getByRole("heading", { name: "Вход" }),
    ).toBeInTheDocument();
    expect(
      screen.queryByRole("heading", { name: "Интеграции" }),
    ).not.toBeInTheDocument();
  });

  it("редиректит уже авторизованного пользователя с /login на /tasks (RequireGuest)", () => {
    renderApp("/login", { authenticated: true });

    expect(
      screen.getByRole("heading", { name: "Задачи" }),
    ).toBeInTheDocument();
    expect(
      screen.queryByRole("heading", { name: "Вход" }),
    ).not.toBeInTheDocument();
  });

  it("рендерит экран /register вне общей навигационной оболочки (гость)", () => {
    renderApp("/register");

    expect(
      screen.getByRole("heading", { name: "Регистрация" }),
    ).toBeInTheDocument();
    expect(screen.queryByText("Agentify")).not.toBeInTheDocument();
  });
});
