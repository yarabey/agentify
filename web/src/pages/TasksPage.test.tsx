import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";

import { apiClient } from "@/api/client";
import { TasksPage } from "@/pages/TasksPage";

/**
 * Тесты экрана задач (тикет 9.4, DoD "постановка задачи"; рендер журнала
 * событий — отдельно, `TaskDetailPage.test.tsx`).
 *
 * Как и в `IntegrationsPage.test.tsx` — мокаем `apiClient.<METHOD>` напрямую
 * (`vi.spyOn`), а не глобальный `fetch`: `openapi-fetch` захватывает
 * `globalThis.fetch` в момент создания клиента, до выполнения тела теста.
 */
function renderTasksPage() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter>
        <TasksPage />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

function jsonResponse(status: number) {
  return new Response(null, { status });
}

const INTEGRATION = {
  id: "11111111-1111-1111-1111-111111111111",
  name: "Домашний сервер",
  status: "online" as const,
  created_at: "2026-07-01T00:00:00Z",
};

describe("TasksPage (тикет 9.4, FR E1, H1)", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("постановка задачи отправляет POST /tasks с Idempotency-Key и телом задачи", async () => {
    vi.spyOn(apiClient, "GET").mockImplementation(async (path: unknown) => {
      if (path === "/integrations") {
        return {
          data: [INTEGRATION],
          error: undefined,
          response: jsonResponse(200),
        } as never;
      }
      return {
        data: [],
        error: undefined,
        response: jsonResponse(200),
      } as never;
    });
    const postSpy = vi.spyOn(apiClient, "POST").mockResolvedValue({
      data: {
        id: "22222222-2222-2222-2222-222222222222",
        integration_id: INTEGRATION.id,
        text: "Проверь дисковое место",
        status: "queued",
        created_at: "2026-07-01T00:00:00Z",
        updated_at: "2026-07-01T00:00:00Z",
      },
      error: undefined,
      response: jsonResponse(201),
    } as never);

    renderTasksPage();

    fireEvent.click(await screen.findByRole("button", { name: "Новая задача" }));

    fireEvent.change(await screen.findByLabelText("Машина"), {
      target: { value: INTEGRATION.id },
    });
    fireEvent.change(screen.getByLabelText("Текст запроса"), {
      target: { value: "Проверь дисковое место" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Поставить задачу" }));

    await waitFor(() => {
      expect(postSpy).toHaveBeenCalledWith(
        "/tasks",
        expect.objectContaining({
          params: {
            header: { "Idempotency-Key": expect.any(String) },
          },
          body: {
            integration_id: INTEGRATION.id,
            text: "Проверь дисковое место",
          },
        }),
      );
    });

    // Форма закрывается после успешной постановки — кнопка "Новая задача"
    // снова доступна для следующей попытки (со своим новым ключом).
    expect(
      await screen.findByRole("button", { name: "Новая задача" }),
    ).toBeInTheDocument();
  });

  it("список отражает разные статусы задач человекочитаемо", async () => {
    vi.spyOn(apiClient, "GET").mockImplementation(async (path: unknown) => {
      if (path === "/integrations") {
        return {
          data: [INTEGRATION],
          error: undefined,
          response: jsonResponse(200),
        } as never;
      }
      return {
        data: [
          {
            id: "33333333-3333-3333-3333-333333333333",
            integration_id: INTEGRATION.id,
            text: "Обнови пакеты",
            status: "running",
            created_at: "2026-07-01T00:00:00Z",
            updated_at: "2026-07-01T00:00:00Z",
          },
          {
            id: "44444444-4444-4444-4444-444444444444",
            integration_id: INTEGRATION.id,
            text: "Почисти логи",
            status: "waiting_user",
            created_at: "2026-07-01T00:00:00Z",
            updated_at: "2026-07-01T00:00:00Z",
          },
        ],
        error: undefined,
        response: jsonResponse(200),
      } as never;
    });

    renderTasksPage();

    expect(await screen.findByText("Обнови пакеты")).toBeInTheDocument();
    // "Выполняется"/"Ожидает ответа пользователя" также встречаются как
    // варианты фильтра по статусу (<option>) — проверяем счётчик, а не
    // единственность.
    expect(screen.getAllByText("Выполняется").length).toBeGreaterThan(0);
    expect(screen.getByText("Почисти логи")).toBeInTheDocument();
    expect(
      screen.getAllByText("Ожидает ответа пользователя").length,
    ).toBeGreaterThan(0);
    expect(screen.getAllByText("Домашний сервер").length).toBeGreaterThan(0);
  });

  it("пустой список задач показывает подсказку создать первую", async () => {
    vi.spyOn(apiClient, "GET").mockResolvedValue({
      data: [],
      error: undefined,
      response: jsonResponse(200),
    } as never);

    renderTasksPage();

    expect(
      await screen.findByText("Задач пока нет — поставьте первую."),
    ).toBeInTheDocument();
  });
});
