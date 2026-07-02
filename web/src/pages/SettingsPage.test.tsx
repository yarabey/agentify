import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";

import { apiClient } from "@/api/client";
import { AuthProvider } from "@/context/AuthContext";
import { getAccessToken, setTokens } from "@/lib/tokenStore";
import { SettingsPage } from "@/pages/SettingsPage";

/**
 * Тесты экрана настроек (тикет 9.6, FR A2, D3; приёмка: «код генерится и
 * отображается»).
 *
 * Как и в `IntegrationsPage.test.tsx`/`LoginPage.test.tsx` — мокаем
 * `apiClient.<METHOD>` напрямую (`vi.spyOn`), а не глобальный `fetch`.
 * `/login` смонтирован рядом, чтобы проверить редирект после выхода
 * (`LogoutSection`).
 */
function jsonResponse(status: number) {
  return new Response(null, { status });
}

function renderSettingsPage() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  setTokens({ accessToken: "test-access", refreshToken: "test-refresh" });
  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter initialEntries={["/settings"]}>
        <AuthProvider>
          <Routes>
            <Route path="/settings" element={<SettingsPage />} />
            <Route path="/login" element={<div>Экран входа (заглушка)</div>} />
          </Routes>
        </AuthProvider>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

/** Ответ `GET /admin/registration-token` для не-администратора (403). */
function mockNonAdminRegistrationToken() {
  vi.spyOn(apiClient, "GET").mockResolvedValue({
    data: undefined,
    error: { code: "admin_required", message: "доступно только администратору" },
    response: jsonResponse(403),
  } as never);
}

describe("SettingsPage (тикет 9.6, FR A2, D3)", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    localStorage.clear();
  });

  it("администратор видит действующий токен регистрации (FR A2)", async () => {
    vi.spyOn(apiClient, "GET").mockResolvedValue({
      data: { token: "current-registration-secret" },
      error: undefined,
      response: jsonResponse(200),
    } as never);

    renderSettingsPage();

    expect(
      await screen.findByText("current-registration-secret"),
    ).toBeInTheDocument();
  });

  it("не-администратор не видит секцию токена регистрации (403 → секция скрыта)", async () => {
    mockNonAdminRegistrationToken();

    renderSettingsPage();

    // Дожидаемся, что запрос токена регистрации отработал (секция перестала
    // показывать "Загрузка…"), прежде чем утверждать отсутствие заголовка.
    await waitFor(() => {
      expect(screen.queryByText("Загрузка…")).not.toBeInTheDocument();
    });
    expect(screen.queryByText("Токен регистрации")).not.toBeInTheDocument();
  });

  it("клик по «Привязать Telegram» запрашивает код и показывает его пользователю", async () => {
    mockNonAdminRegistrationToken();
    const postSpy = vi.spyOn(apiClient, "POST").mockResolvedValue({
      data: { code: "ABCD2345", expires_at: "2026-07-02T12:15:00Z" },
      error: undefined,
      response: jsonResponse(201),
    } as never);

    renderSettingsPage();

    fireEvent.click(
      await screen.findByRole("button", { name: "Привязать Telegram" }),
    );

    expect(await screen.findByText(/ABCD2345/)).toBeInTheDocument();
    expect(postSpy).toHaveBeenCalledWith("/channels/telegram/link-code");
  });

  it("ошибка генерации кода показывает сообщение и не рендерит код", async () => {
    mockNonAdminRegistrationToken();
    vi.spyOn(apiClient, "POST").mockResolvedValue({
      data: undefined,
      error: { code: "internal", message: "внутренняя ошибка" },
      response: jsonResponse(500),
    } as never);

    renderSettingsPage();

    fireEvent.click(
      await screen.findByRole("button", { name: "Привязать Telegram" }),
    );

    expect(
      await screen.findByText(
        "Не удалось сгенерировать код. Попробуйте ещё раз.",
      ),
    ).toBeInTheDocument();
    expect(screen.queryByRole("status")).not.toBeInTheDocument();
  });

  it("выход завершает сессию (очищает токены) и переводит на /login", async () => {
    mockNonAdminRegistrationToken();
    vi.spyOn(apiClient, "POST").mockResolvedValue({
      data: undefined,
      error: undefined,
      response: jsonResponse(204),
    } as never);

    renderSettingsPage();

    fireEvent.click(await screen.findByRole("button", { name: "Выйти" }));

    await waitFor(() => {
      expect(screen.getByText("Экран входа (заглушка)")).toBeInTheDocument();
    });
    expect(getAccessToken()).toBeNull();
  });
});
