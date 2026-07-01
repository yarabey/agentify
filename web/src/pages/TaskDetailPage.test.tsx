import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";

import { apiClient } from "@/api/client";
import { TaskDetailPage } from "@/pages/TaskDetailPage";

/**
 * Тесты карточки задачи с журналом событий (тикет 9.4, DoD "рендер журнала")
 * и действий над активной задачей (тикет 8.8, FR H2, Gherkin §10 «Действия
 * из истории в web»). Мокаем `apiClient.GET`/`apiClient.POST` через
 * `mockImplementation`, различая эндпоинты по первому аргументу (шаблон
 * пути) — так же, как в `IntegrationsPage.test.tsx` ("повторный показ UUID
 * запрашивает GET /integrations/{id}").
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

/** Собирает мок `apiClient.GET`, отдающий заданную задачу и журнал событий. */
function mockGet(task: Record<string, unknown>, events: Record<string, unknown>[]) {
  return vi.spyOn(apiClient, "GET").mockImplementation(async (path: unknown) => {
    if (path === "/tasks/{id}") {
      return { data: task, error: undefined, response: jsonResponse(200) } as never;
    }
    if (path === "/tasks/{id}/events") {
      return { data: events, error: undefined, response: jsonResponse(200) } as never;
    }
    throw new Error(`неожиданный путь в тесте: ${String(path)}`);
  });
}

const ACTION_BUTTON_NAME_RE = /ответить|отменить|подтвердить/i;

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

    // Задача активна (running), но последнее событие журнала — не
    // agent_question, поэтому доступна только «Отменить» (тикет 8.8, FR H2).
    expect(
      await screen.findByRole("button", { name: "Отменить" }),
    ).toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: "Подтвердить завершение" }),
    ).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Ответить" })).not.toBeInTheDocument();
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

describe("TaskDetailPage: действия из истории (тикет 8.8, FR H2, Gherkin §10)", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("waiting_user + последнее событие agent_question: доступны «Ответить» и «Отменить», ответ уходит с question_id последнего события", async () => {
    mockGet(
      { ...TASK, status: "waiting_user" },
      [
        {
          id: "e1",
          seq: 1,
          type: "status_change",
          payload: { status: "running" },
          created_at: "2026-07-01T00:00:00Z",
        },
        {
          id: "e2",
          seq: 2,
          type: "agent_question",
          payload: { question: "Какую версию ставить?" },
          created_at: "2026-07-01T00:05:00Z",
        },
      ],
    );
    const postSpy = vi
      .spyOn(apiClient, "POST")
      .mockResolvedValue({
        data: undefined,
        error: undefined,
        response: jsonResponse(202),
      } as never);

    renderTaskDetailPage();

    expect(
      await screen.findByRole("button", { name: "Отменить" }),
    ).toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: "Подтвердить завершение" }),
    ).not.toBeInTheDocument();

    expect(await screen.findAllByText("Какую версию ставить?")).toHaveLength(2);
    fireEvent.change(screen.getByLabelText("Ваш ответ"), {
      target: { value: "Последнюю LTS" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Ответить" }));

    await waitFor(() =>
      expect(postSpy).toHaveBeenCalledWith(
        "/tasks/{id}/answer",
        expect.objectContaining({
          params: { path: { id: TASK_ID } },
          body: { question_id: "e2", text: "Последнюю LTS" },
        }),
      ),
    );
  });

  it("waiting_user + последнее событие command_approval_request: формы «Ответить» нет (согласование команды вне объёма 8.8)", async () => {
    mockGet(
      { ...TASK, status: "waiting_user" },
      [
        {
          id: "e1",
          seq: 1,
          type: "command_approval_request",
          payload: { command: "apt upgrade -y" },
          created_at: "2026-07-01T00:00:00Z",
        },
      ],
    );

    renderTaskDetailPage();

    expect(
      await screen.findByRole("button", { name: "Отменить" }),
    ).toBeInTheDocument();
    expect(screen.queryByLabelText("Ваш ответ")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Ответить" })).not.toBeInTheDocument();
  });

  it("awaiting_confirm: доступна «Подтвердить завершение», клик вызывает POST /tasks/{id}/confirm", async () => {
    mockGet({ ...TASK, status: "awaiting_confirm" }, []);
    const postSpy = vi
      .spyOn(apiClient, "POST")
      .mockResolvedValue({
        data: undefined,
        error: undefined,
        response: jsonResponse(200),
      } as never);

    renderTaskDetailPage();

    const confirmButton = await screen.findByRole("button", {
      name: "Подтвердить завершение",
    });
    fireEvent.click(confirmButton);

    await waitFor(() =>
      expect(postSpy).toHaveBeenCalledWith(
        "/tasks/{id}/confirm",
        expect.objectContaining({ params: { path: { id: TASK_ID } } }),
      ),
    );
  });

  it.each(["completed", "cancelled", "failed"] as const)(
    "терминальный статус %s: ни одно из действий не показано",
    async (status) => {
      mockGet({ ...TASK, status }, [
        {
          id: "e1",
          seq: 1,
          type: "agent_question",
          payload: { question: "Какую версию ставить?" },
          created_at: "2026-07-01T00:00:00Z",
        },
      ]);

      renderTaskDetailPage();

      await screen.findByText("Проверь дисковое место");
      expect(
        screen.queryByRole("button", { name: ACTION_BUTTON_NAME_RE }),
      ).not.toBeInTheDocument();
      expect(screen.queryByLabelText("Ваш ответ")).not.toBeInTheDocument();
    },
  );

  it("«Отменить» требует подтверждения через window.confirm — без подтверждения запрос не уходит", async () => {
    mockGet({ ...TASK, status: "running" }, []);
    const postSpy = vi
      .spyOn(apiClient, "POST")
      .mockResolvedValue({
        data: undefined,
        error: undefined,
        response: jsonResponse(202),
      } as never);

    renderTaskDetailPage();

    const cancelButton = await screen.findByRole("button", { name: "Отменить" });

    vi.spyOn(window, "confirm").mockReturnValue(false);
    fireEvent.click(cancelButton);
    expect(postSpy).not.toHaveBeenCalled();

    vi.spyOn(window, "confirm").mockReturnValue(true);
    fireEvent.click(cancelButton);
    await waitFor(() =>
      expect(postSpy).toHaveBeenCalledWith(
        "/tasks/{id}/cancel",
        expect.objectContaining({ params: { path: { id: TASK_ID } } }),
      ),
    );
  });
});
