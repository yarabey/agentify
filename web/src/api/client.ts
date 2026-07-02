import createClient from "openapi-fetch";

import type { paths } from "./schema";
import {
  clearTokens,
  getAccessToken,
  getRefreshToken,
  setTokens,
  type TokenPair,
} from "@/lib/tokenStore";

/**
 * Типобезопасный REST-клиент оркестратора (тикет 9.1, FR D1).
 *
 * Назначение (бизнес): единая точка входа для всех запросов web-канала к
 * оркестратору (логин, интеграции, задачи, диалог) — те же операции, что
 * REST API уже отдаёт Telegram-боту (см. docs/01_tech_stack_and_architecture.md,
 * "И web, и Telegram-бот ходят в один и тот же REST API").
 *
 * Как устроено (тех): `openapi-fetch` — тонкая обёртка над `fetch`,
 * типизированная сгенерённой схемой `./schema.ts` (`paths`, `openapi-typescript`
 * из `api/openapi.yaml`, см. `npm run gen:api`). Руками сюда типы не пишем —
 * только конфигурация клиента.
 *
 * `baseUrl` по умолчанию — относительный `/api`: в проде front-Caddy
 * (`deploy/Caddyfile`) проксирует `/api/*` на оркестратор и снимает префикс,
 * поэтому web и API всегда на одном origin и CORS не нужен. Для локальной
 * разработки (`vite dev` отдельно от оркестратора) можно переопределить через
 * `VITE_API_BASE_URL` (например, `http://localhost:8081`).
 *
 * Мидларь ниже (тикет 9.2, FR A3) добавляет два сквозных поведения без
 * участия вызывающего кода экранов:
 *   1. `Authorization: Bearer <access>` — на все запросы, кроме
 *      auth-эндпоинтов (см. `AUTH_EXEMPT_PATHS`).
 *   2. Авто-refresh по 401: истёкший access незаметно для вызывающего кода
 *      меняется на новый, исходный запрос повторяется один раз.
 */
export const apiClient = createClient<paths>({
  baseUrl: import.meta.env.VITE_API_BASE_URL ?? "/api",
});

/**
 * OpenAPI-шаблоны путей (не итоговый URL — `schemaPath` из мидлвари
 * openapi-fetch), для которых не действует общая auth-логика ниже:
 *   - `/auth/register`, `/auth/login` — публичные, Authorization-заголовок
 *     для них бессмысленен (токена ещё нет), а их собственный 401/403 — это
 *     ошибка кредов/токена регистрации, а не протухшая сессия;
 *   - `/auth/refresh` — сам является частью refresh-логики; авто-retry на
 *     его собственный 401 привёл бы к рекурсии.
 * Сравнение по `schemaPath`, а не по `request.url`, не зависит от `baseUrl`
 * (`/api` в проде, произвольный `VITE_API_BASE_URL` в деве).
 */
const AUTH_EXEMPT_PATHS = new Set<string>([
  "/auth/register",
  "/auth/login",
  "/auth/refresh",
]);

/**
 * Клоны исходных запросов, которые ещё можно безопасно отправить повторно
 * (если у запроса есть тело — оно ещё не прочитано), на случай авто-refresh
 * по 401. Ключ — `id`, уникальный на каждый вызов (генерируется openapi-fetch
 * внутри `coreFetch`, см. `openapi-fetch/src/index.js`).
 *
 * Клонировать нужно именно в `onRequest`, ДО того как запрос уйдёт в
 * `fetch()`: после отправки поток тела запроса становится "использованным"
 * (`disturbed`), и повторный `request.clone()` в `onResponse` бросил бы
 * исключение для запросов с телом (POST/PUT/PATCH). Кладём клон здесь,
 * забираем и чистим в `onResponse`/`onError`.
 */
const pendingRetryRequests = new Map<string, Request>();

/**
 * Single-flight promise активного `POST /auth/refresh`.
 *
 * Критично (см. `orchestrator/internal/api/auth.go`): бэкенд ротирует
 * refresh-токен немедленно при обмене — старый отзывается ДО выдачи новой
 * пары. Если несколько запросов одновременно получат 401 (например, две
 * панели web одновременно дёргают защищённые эндпоинты истёкшим access), и
 * каждый наивно вызовет `/auth/refresh` параллельно со старым refresh-токеном
 * — только первый вызов успеет обменять токен, остальные получат 401 на уже
 * отозванный refresh и не восстановятся. Поэтому все параллельные 401
 * дожидаются ОДНОГО начатого обновления вместо того, чтобы каждый начинал своё.
 */
let refreshPromise: Promise<TokenPair> | null = null;

async function performRefresh(
  refreshToken: string,
  fetchImpl: typeof fetch,
  baseUrl: string,
): Promise<TokenPair> {
  const response = await fetchImpl(`${baseUrl}/auth/refresh`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ refresh_token: refreshToken }),
  });
  if (!response.ok) {
    throw new Error(`auth/refresh failed: ${response.status}`);
  }
  const data = (await response.json()) as {
    access_token: string;
    refresh_token: string;
  };
  return { accessToken: data.access_token, refreshToken: data.refresh_token };
}

/** Возвращает уже начатый refresh либо начинает новый (single-flight). */
function refreshTokens(
  refreshToken: string,
  fetchImpl: typeof fetch,
  baseUrl: string,
): Promise<TokenPair> {
  if (!refreshPromise) {
    refreshPromise = performRefresh(refreshToken, fetchImpl, baseUrl).finally(
      () => {
        refreshPromise = null;
      },
    );
  }
  return refreshPromise;
}

apiClient.use({
  onRequest({ request, schemaPath, id }) {
    // См. godoc pendingRetryRequests — клон нужен до отправки запроса.
    pendingRetryRequests.set(id, request.clone());

    if (AUTH_EXEMPT_PATHS.has(schemaPath)) {
      return undefined;
    }
    const accessToken = getAccessToken();
    if (!accessToken) {
      return undefined;
    }
    const headers = new Headers(request.headers);
    headers.set("Authorization", `Bearer ${accessToken}`);
    return new Request(request, { headers });
  },

  async onResponse({ response, schemaPath, id, options }) {
    const retryableRequest = pendingRetryRequests.get(id);
    pendingRetryRequests.delete(id);

    if (
      response.status !== 401 ||
      AUTH_EXEMPT_PATHS.has(schemaPath) ||
      !retryableRequest
    ) {
      return undefined;
    }

    const refreshToken = getRefreshToken();
    if (!refreshToken) {
      // Нет refresh-токена (не вошли или уже разлогинены) — повторять
      // запрос бессмысленно, отдаём исходный 401 как есть.
      return undefined;
    }

    try {
      const newTokens = await refreshTokens(
        refreshToken,
        options.fetch,
        options.baseUrl,
      );
      setTokens(newTokens);

      const retryHeaders = new Headers(retryableRequest.headers);
      retryHeaders.set("Authorization", `Bearer ${newTokens.accessToken}`);
      const retryRequest = new Request(retryableRequest, {
        headers: retryHeaders,
      });
      // Через голый fetch, а не apiClient — иначе повторный запрос снова
      // прошёл бы через эту же мидларь (лишний виток, а при новом 401 — риск
      // рекурсии). Результат становится ответом исходного вызова для кода
      // экрана (openapi-fetch просто использует response, который вернула
      // onResponse-мидларь).
      return await options.fetch(retryRequest);
    } catch {
      // Refresh не удался (сеть или невалидный/отозванный refresh) — сессия
      // на этом клиенте больше не восстановима: чистим токены (это же
      // синхронно оповестит AuthContext через onTokensCleared) и отдаём
      // исходный 401 вызывающему коду как есть.
      clearTokens();
      return undefined;
    }
  },

  onError({ id }) {
    pendingRetryRequests.delete(id);
  },
});
