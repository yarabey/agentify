import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it } from "vitest";

import { App } from "@/App";

/**
 * Тест рендера оболочки (тикет 9.1, DoD "Vitest — рендер оболочки"):
 * проверяет, что `App` (обёрнутый в те же провайдеры, что и в проде —
 * QueryClientProvider + роутер, тут MemoryRouter вместо BrowserRouter)
 * рендерится без ошибок, показывает навигацию по всем заглушкам экранов и
 * по умолчанию открывает домашний экран `/tasks`.
 */
function renderApp(initialPath = "/") {
  const queryClient = new QueryClient();
  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter initialEntries={[initialPath]}>
        <App />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe("App shell", () => {
  it("рендерит шапку с навигацией и домашний экран задач по умолчанию", () => {
    renderApp("/");

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

  it("рендерит экран /login вне общей навигационной оболочки", () => {
    renderApp("/login");

    expect(
      screen.getByRole("heading", { name: "Вход" }),
    ).toBeInTheDocument();
    expect(screen.queryByText("Agentify")).not.toBeInTheDocument();
  });

  it("рендерит заглушки /integrations и /settings", () => {
    const { unmount } = renderApp("/integrations");
    expect(
      screen.getByRole("heading", { name: "Интеграции" }),
    ).toBeInTheDocument();
    unmount();

    renderApp("/settings");
    expect(
      screen.getByRole("heading", { name: "Настройки" }),
    ).toBeInTheDocument();
  });
});
