import createClient from "openapi-fetch";

import type { paths } from "./schema";

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
 */
export const apiClient = createClient<paths>({
  baseUrl: import.meta.env.VITE_API_BASE_URL ?? "/api",
});
