import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";

import { apiClient } from "@/api/client";
import { TaskDetailPage } from "@/pages/TaskDetailPage";

/**
 * Тесты карточки задачи с журналом событий (тикет 9.4, DoD "рендер журнала").
 * Мокаем `apiClient.GET` через `mockImplementation`, различая эндпоинты по
 * первому аргументу (шаблон пути) — так же, как в
 * `IntegrationsPage.test.tsx` ("повторный показ UUID запрашивает GET
 * /integrations/{id}").
 */
const TASK_ID = "11111111-1111-1111-1111-111111111111";

function renderTaskDetailPage(taskId = TASK_ID) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter initialEntries={[`/tasks/${taskId}`]}>
        <Routes>
          <Route path="/tasks/:id" element={<TaskDetailPage />} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

function jsonResponse(status: number) {
  return new Response(null, { status });
}

const TASK = {
  id: TASK_ID,
  integration_id: "22222222-2222-2222-2222-222222222222",
  text: "Проверь дисковое место",
  status: "running" as const,
  created_at: "2026-07-01T00:00:00Z",
  updated_at: "2026-07-01T01:00:00Z",
};

describe("TaskDetailPage (тикет 9.4, FR H1, рендер журнала)", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("рендерит карточку задачи и журнал с разнотипными событиями", async () => {
    vi.spyOn(apiClient, "GET").mockImplementation(async (path: unknown) => {
      if (path === "/tasks/{id}") {
        return {
          data: TASK,
          error: undefined,
          response: jsonResponse(200),
        } as never;
      }
      if (path === "/tasks/{id}/events") {
        return {
          data: [
            {
              id: "e1",
              seq: 1,
              type: "status_change",
              payload: { status: "queued" },
              created_at: "2026-07-01T00:00:00Z",
            },
            {
              id: "e2",
              seq: 2,
              type: "agent_question",
              payload: { question: "Какую версию ставить?" },
              created_at: "2026-07-01T00:05:00Z",
            },
            {
              id: "e3",
              seq: 3,
              type: "command_approval_request",
              payload: { command: "apt upgrade -y" },
              created_at: "2026-07-01T00:06:00Z",
            },
            {
              id: "e4",
              seq: 4,
              type: "error",
              payload: { code: 500, details: { retryable: true } },
              created_at: "2026-07-01T00:07:00Z",
            },
          ],
          error: undefined,
          response: jsonResponse(200),
        } as never;
      }
      throw new Error(`неожиданный путь в тесте: ${String(path)}`);
    });

    renderTaskDetailPage();

    expect(await screen.findByText("Проверь дисковое место")).toBeInTheDocument();
    expect(screen.getByText("Выполняется")).toBeInTheDocument();

    expect(await screen.findByText(/Смена статуса/)).toBeInTheDocument();
    expect(screen.getByText(/Вопрос агента/)).toBeInTheDocument();
    expect(screen.getByText("Какую версию ставить?")).toBeInTheDocument();
    expect(screen.getByText(/Запрос согласования команды/)).toBeInTheDocument();
    expect(screen.getByText("apt upgrade -y")).toBeInTheDocument();
    expect(screen.getByText(/Ошибка/)).toBeInTheDocument();
    // Событие error без знакомого текстового поля падает на JSON-фолбэк.
    expect(screen.getByText(/"code": 500/)).toBeInTheDocument();

    // Интерактивных действий над задачей быть не должно (тикет 8.8, вне объёма 9.4).
    expect(
      screen.queryByRole("button", { name: /ответить|согласовать|отклонить|отменить|подтвердить/i }),
    ).not.toBeInTheDocument();
  });

  it("несуществующая задача показывает понятное сообщение (404)", async () => {
    vi.spyOn(apiClient, "GET").mockImplementation(async (path: unknown) => {
      if (path === "/tasks/{id}") {
        return {
          data: undefined,
          error: { error: "not found" },
          response: jsonResponse(404),
        } as never;
      }
      return {
        data: [],
        error: undefined,
        response: jsonResponse(200),
      } as never;
    });

    renderTaskDetailPage("99999999-9999-9999-9999-999999999999");

    expect(await screen.findByText("Задача не найдена.")).toBeInTheDocument();
  });
});
