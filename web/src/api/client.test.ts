import { act, renderHook, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { apiClient } from "@/api/client";
import { AuthProvider, useAuth } from "@/context/AuthContext";
import { getAccessToken, getRefreshToken, setTokens } from "@/lib/tokenStore";

/**
 * Тесты авто-refresh по 401 (тикет 9.2, DoD "авто-refresh по 401",
 * FR A3). Мидларь регистрируется на `apiClient` один раз при импорте
 * `src/api/client.ts` (см. `apiClient.use({...})`), поэтому здесь просто
 * дёргаем реальный `apiClient.GET/POST` с подставным `fetch` — per-call
 * `fetch`-override, который поддерживает `openapi-fetch` (см.
 * `node_modules/openapi-fetch/src/index.js`, `fetch = baseFetch`), позволяет
 * не бороться с тем, что `createClient()` захватывает `globalThis.fetch`
 * один раз при создании клиента (до того, как тело теста успеет его
 * подменить).
 */

/**
 * Нормализует вызов мока `fetch` к единому виду — независимо от того,
 * вызван ли он как `fetch(request: Request)` (так делает openapi-fetch для
 * исходного/повторного запроса) или как `fetch(url: string, init)` (так
 * `client.ts` вызывает `/auth/refresh` напрямую, см. `performRefresh` —
 * это не `Request`-объект, а обычные `url` + `init`, оба валидны для
 * настоящего `fetch`, но мок должен уметь читать оба варианта одинаково).
 */
function normalizeFetchCall(
  input: RequestInfo | URL,
  init?: RequestInit,
): { url: string; headers: Headers; text: () => Promise<string> } {
  if (input instanceof Request) {
    return {
      url: input.url,
      headers: input.headers,
      text: () => input.clone().text(),
    };
  }
  const url = input instanceof URL ? input.toString() : input;
  return {
    url,
    headers: new Headers(init?.headers),
    text: async () => (typeof init?.body === "string" ? init.body : ""),
  };
}

describe("apiClient — авто-Authorization и авто-refresh по 401 (тикет 9.2, FR A3)", () => {
  afterEach(() => {
    localStorage.clear();
  });

  it("добавляет Authorization: Bearer <access> на защищённые запросы", async () => {
    setTokens({ accessToken: "access-1", refreshToken: "refresh-1" });

    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const { url, headers } = normalizeFetchCall(input, init);
      expect(url).toContain("/integrations");
      expect(headers.get("Authorization")).toBe("Bearer access-1");
      return new Response("[]", {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    });

    const { data, error } = await apiClient.GET("/integrations" as never, {
      fetch: fetchMock,
    } as never);

    expect(error).toBeUndefined();
    expect(data).toEqual([]);
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("не добавляет Authorization на /auth/login", async () => {
    setTokens({ accessToken: "access-1", refreshToken: "refresh-1" });

    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const { headers } = normalizeFetchCall(input, init);
      expect(headers.has("Authorization")).toBe(false);
      return new Response(
        JSON.stringify({ access_token: "a", refresh_token: "b" }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      );
    });

    await apiClient.POST("/auth/login" as never, {
      body: { username: "alice", password: "secret" },
      fetch: fetchMock,
    } as never);

    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("401 -> авто-refresh -> повтор исходного запроса успешен ровно с одним вызовом /auth/refresh", async () => {
    setTokens({ accessToken: "expired-access", refreshToken: "valid-refresh" });

    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const { url, headers, text } = normalizeFetchCall(input, init);

      if (url.includes("/auth/refresh")) {
        const body = JSON.parse(await text()) as { refresh_token: string };
        expect(body.refresh_token).toBe("valid-refresh");
        return new Response(
          JSON.stringify({
            access_token: "new-access",
            refresh_token: "new-refresh",
          }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        );
      }

      if (url.includes("/integrations")) {
        if (headers.get("Authorization") === "Bearer new-access") {
          return new Response("[]", {
            status: 200,
            headers: { "Content-Type": "application/json" },
          });
        }
        return new Response(
          JSON.stringify({ code: "unauthorized", message: "expired" }),
          { status: 401, headers: { "Content-Type": "application/json" } },
        );
      }

      throw new Error(`неожиданный URL в тесте: ${url}`);
    });

    const { data, error, response } = await apiClient.GET(
      "/integrations" as never,
      { fetch: fetchMock } as never,
    );

    expect(response.status).toBe(200);
    expect(error).toBeUndefined();
    expect(data).toEqual([]);
    expect(getAccessToken()).toBe("new-access");
    expect(getRefreshToken()).toBe("new-refresh");

    const refreshCalls = fetchMock.mock.calls.filter(([input]) =>
      normalizeFetchCall(input).url.includes("/auth/refresh"),
    );
    expect(refreshCalls).toHaveLength(1);
  });

  it("single-flight: два параллельных 401 вызывают /auth/refresh ровно один раз", async () => {
    setTokens({ accessToken: "expired-access", refreshToken: "valid-refresh" });

    let refreshCallCount = 0;
    // Резолвер refresh откладываем вручную, чтобы гарантированно поймать
    // момент, когда ОБА запроса уже получили свой 401 и оба претендуют на
    // обновление токена — именно тут наивная реализация без single-flight
    // отправила бы второй параллельный /auth/refresh со старым (уже
    // отозванным после первого запроса) refresh-токеном и сломала бы второй
    // запрос (см. orchestrator/internal/api/auth.go — ротация немедленная).
    let resolveRefresh: (() => void) | undefined;
    const refreshGate = new Promise<void>((resolve) => {
      resolveRefresh = resolve;
    });

    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const { url, headers } = normalizeFetchCall(input, init);

      if (url.includes("/auth/refresh")) {
        refreshCallCount += 1;
        await refreshGate;
        return new Response(
          JSON.stringify({
            access_token: "new-access",
            refresh_token: "new-refresh",
          }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        );
      }

      if (url.includes("/integrations")) {
        if (headers.get("Authorization") === "Bearer new-access") {
          return new Response("[]", {
            status: 200,
            headers: { "Content-Type": "application/json" },
          });
        }
        return new Response(
          JSON.stringify({ code: "unauthorized", message: "expired" }),
          { status: 401, headers: { "Content-Type": "application/json" } },
        );
      }

      throw new Error(`неожиданный URL в тесте: ${url}`);
    });

    const call1 = apiClient.GET("/integrations" as never, {
      fetch: fetchMock,
    } as never);
    const call2 = apiClient.GET("/integrations" as never, {
      fetch: fetchMock,
    } as never);

    // Даём обоим запросам дойти до 401 и начать refresh, прежде чем его отпустить.
    await waitFor(() => expect(refreshCallCount).toBeGreaterThan(0));
    await Promise.resolve();
    resolveRefresh?.();

    const [result1, result2] = await Promise.all([call1, call2]);

    expect(result1.response.status).toBe(200);
    expect(result2.response.status).toBe(200);
    expect(refreshCallCount).toBe(1);
  });

  it("неудачный refresh очищает токены и синхронно переводит isAuthenticated в false", async () => {
    setTokens({ accessToken: "expired-access", refreshToken: "revoked-refresh" });

    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const { url } = normalizeFetchCall(input, init);
      if (url.includes("/auth/refresh")) {
        return new Response(
          JSON.stringify({
            code: "invalid_refresh_token",
            message: "refresh-токен недействителен",
          }),
          { status: 401, headers: { "Content-Type": "application/json" } },
        );
      }
      if (url.includes("/integrations")) {
        return new Response(
          JSON.stringify({ code: "unauthorized", message: "expired" }),
          { status: 401, headers: { "Content-Type": "application/json" } },
        );
      }
      throw new Error(`неожиданный URL в тесте: ${url}`);
    });

    const { result } = renderHook(() => useAuth(), {
      wrapper: AuthProvider,
    });
    expect(result.current.isAuthenticated).toBe(true);

    await act(async () => {
      await apiClient.GET("/integrations" as never, {
        fetch: fetchMock,
      } as never);
    });

    expect(getAccessToken()).toBeNull();
    expect(getRefreshToken()).toBeNull();
    await waitFor(() => expect(result.current.isAuthenticated).toBe(false));
  });
});
