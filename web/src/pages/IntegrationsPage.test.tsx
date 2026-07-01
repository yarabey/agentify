import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { apiClient } from "@/api/client";
import { IntegrationsPage } from "@/pages/IntegrationsPage";

/**
 * Тесты экрана интеграций (тикет 9.3, DoD "создание показывает UUID;
 * статус отражается").
 *
 * Как и в `LoginPage.test.tsx` — мокаем `apiClient.<METHOD>` напрямую
 * (`vi.spyOn`), а не глобальный `fetch`: `openapi-fetch` захватывает
 * `globalThis.fetch` в момент создания клиента, до выполнения тела теста.
 */
function renderIntegrationsPage() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return render(
    <QueryClientProvider client={queryClient}>
      <IntegrationsPage />
    </QueryClientProvider>,
  );
}

function jsonResponse(status: number) {
  return new Response(null, { status });
}

describe("IntegrationsPage (тикет 9.3, FR B1-B5)", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("создание интеграции показывает возвращённый UUID пользователю", async () => {
    vi.spyOn(apiClient, "GET").mockResolvedValue({
      data: [],
      error: undefined,
      response: jsonResponse(200),
    } as never);

    const postSpy = vi.spyOn(apiClient, "POST").mockResolvedValue({
      data: {
        id: "11111111-1111-1111-1111-111111111111",
        name: "Домашний сервер",
        status: "offline",
        created_at: "2026-07-01T00:00:00Z",
        uuid: "22222222-2222-2222-2222-222222222222",
      },
      error: undefined,
      response: jsonResponse(201),
    } as never);

    renderIntegrationsPage();

    fireEvent.click(
      await screen.findByRole("button", { name: "Создать интеграцию" }),
    );
    fireEvent.change(screen.getByLabelText("Название"), {
      target: { value: "Домашний сервер" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Создать" }));

    expect(
      await screen.findByText("22222222-2222-2222-2222-222222222222"),
    ).toBeInTheDocument();

    expect(postSpy).toHaveBeenCalledWith(
      "/integrations",
      expect.objectContaining({
        body: { name: "Домашний сервер", ip_hint: undefined },
      }),
    );
  });

  it("список отражает статус онлайн/оффлайн для разных интеграций", async () => {
    vi.spyOn(apiClient, "GET").mockResolvedValue({
      data: [
        {
          id: "11111111-1111-1111-1111-111111111111",
          name: "Онлайн-машина",
          status: "online",
          created_at: "2026-07-01T00:00:00Z",
        },
        {
          id: "22222222-2222-2222-2222-222222222222",
          name: "Оффлайн-машина",
          status: "offline",
          created_at: "2026-07-01T00:00:00Z",
        },
      ],
      error: undefined,
      response: jsonResponse(200),
    } as never);

    renderIntegrationsPage();

    expect(await screen.findByText("Онлайн-машина")).toBeInTheDocument();
    expect(screen.getByText("Оффлайн-машина")).toBeInTheDocument();
    expect(screen.getByText("Онлайн")).toBeInTheDocument();
    expect(screen.getByText("Оффлайн")).toBeInTheDocument();
  });

  it("повторный показ UUID запрашивает GET /integrations/{id}", async () => {
    vi.spyOn(apiClient, "GET").mockImplementation(async (path: unknown) => {
      if (path === "/integrations") {
        return {
          data: [
            {
              id: "11111111-1111-1111-1111-111111111111",
              name: "Домашний сервер",
              status: "online",
              created_at: "2026-07-01T00:00:00Z",
            },
          ],
          error: undefined,
          response: jsonResponse(200),
        } as never;
      }
      return {
        data: {
          id: "11111111-1111-1111-1111-111111111111",
          name: "Домашний сервер",
          status: "online",
          created_at: "2026-07-01T00:00:00Z",
          uuid: "33333333-3333-3333-3333-333333333333",
        },
        error: undefined,
        response: jsonResponse(200),
      } as never;
    });

    renderIntegrationsPage();

    fireEvent.click(
      await screen.findByRole("button", { name: "Показать UUID" }),
    );

    expect(
      await screen.findByText(/33333333-3333-3333-3333-333333333333/),
    ).toBeInTheDocument();
  });

  it("редактирование интеграции отправляет PATCH и обновляет список", async () => {
    const getSpy = vi.spyOn(apiClient, "GET").mockResolvedValue({
      data: [
        {
          id: "11111111-1111-1111-1111-111111111111",
          name: "Старое имя",
          status: "online",
          created_at: "2026-07-01T00:00:00Z",
        },
      ],
      error: undefined,
      response: jsonResponse(200),
    } as never);
    const patchSpy = vi.spyOn(apiClient, "PATCH").mockResolvedValue({
      data: undefined,
      error: undefined,
      response: jsonResponse(200),
    } as never);

    renderIntegrationsPage();

    fireEvent.click(
      await screen.findByRole("button", { name: "Редактировать" }),
    );
    fireEvent.change(screen.getByLabelText("Название"), {
      target: { value: "Новое имя" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Сохранить" }));

    await waitFor(() => {
      expect(patchSpy).toHaveBeenCalledWith(
        "/integrations/{id}",
        expect.objectContaining({
          params: { path: { id: "11111111-1111-1111-1111-111111111111" } },
          body: { name: "Новое имя", ip_hint: undefined },
        }),
      );
    });
    await waitFor(() => {
      expect(getSpy).toHaveBeenCalledTimes(2);
    });
  });

  it("удаление с 204 не требует эскалации подтверждения", async () => {
    vi.spyOn(window, "confirm").mockReturnValue(true);
    const getSpy = vi.spyOn(apiClient, "GET").mockResolvedValue({
      data: [
        {
          id: "11111111-1111-1111-1111-111111111111",
          name: "Домашний сервер",
          status: "offline",
          created_at: "2026-07-01T00:00:00Z",
        },
      ],
      error: undefined,
      response: jsonResponse(200),
    } as never);
    const deleteSpy = vi.spyOn(apiClient, "DELETE").mockResolvedValue({
      data: undefined,
      error: undefined,
      response: jsonResponse(204),
    } as never);

    renderIntegrationsPage();

    fireEvent.click(await screen.findByRole("button", { name: "Удалить" }));

    await waitFor(() => {
      expect(deleteSpy).toHaveBeenCalledTimes(1);
    });
    expect(window.confirm).toHaveBeenCalledTimes(1);
    await waitFor(() => {
      expect(getSpy).toHaveBeenCalledTimes(2);
    });
  });

  it("удаление с 409 показывает второе предупреждение и повторяет с confirm=true", async () => {
    const confirmSpy = vi
      .spyOn(window, "confirm")
      .mockReturnValueOnce(true) // первое подтверждение удаления
      .mockReturnValueOnce(true); // подтверждение отмены активных задач
    vi.spyOn(apiClient, "GET").mockResolvedValue({
      data: [
        {
          id: "11111111-1111-1111-1111-111111111111",
          name: "Домашний сервер",
          status: "online",
          created_at: "2026-07-01T00:00:00Z",
        },
      ],
      error: undefined,
      response: jsonResponse(200),
    } as never);
    const deleteSpy = vi
      .spyOn(apiClient, "DELETE")
      .mockResolvedValueOnce({
        data: undefined,
        error: undefined,
        response: jsonResponse(409),
      } as never)
      .mockResolvedValueOnce({
        data: undefined,
        error: undefined,
        response: jsonResponse(204),
      } as never);

    renderIntegrationsPage();

    fireEvent.click(await screen.findByRole("button", { name: "Удалить" }));

    await waitFor(() => {
      expect(deleteSpy).toHaveBeenCalledTimes(2);
    });
    expect(confirmSpy).toHaveBeenCalledTimes(2);
    expect(deleteSpy).toHaveBeenNthCalledWith(
      2,
      "/integrations/{id}",
      expect.objectContaining({
        params: {
          path: { id: "11111111-1111-1111-1111-111111111111" },
          query: { confirm: true },
        },
      }),
    );
  });
});
