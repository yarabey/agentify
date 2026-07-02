// web/e2e/playwright.config.ts — конфигурация Playwright для сквозного E2E
// (тикет 9.8, критерий выхода MVP).
//
// Назначение (бизнес/тех): сценарии в этом каталоге НЕ поднимают свой
// собственный сервер (в отличие от типичного Playwright webServer-конфига)
// — они бьют в уже поднятый ПОЛНЫЙ стек (`make run-local`/`make e2e`,
// deploy/docker-compose.yml: Postgres+Redpanda+orchestrator+bot+web+caddy),
// потому что сам смысл 9.8 — проверить путь через настоящую очередь
// сообщений и настоящий мост Redpanda→WS (docs/protocol.md), а не через
// Vite dev-сервер с замоканным бэкендом. baseURL — единственная точка входа
// снаружи, фронтовый Caddy (см. web/e2e/support/env.ts, deploy/Caddyfile).
//
// workers=1: сценарии сериализованы намеренно — общий бэкенд БЕЗ TRUNCATE
// между тестами (в отличие от orchestrator/internal/bddsteps, тикет 11.2,
// который поднимает testcontainers Postgres и чистит между сценариями);
// изоляция здесь другая — каждый тест заводит СВОИХ пользователя/интеграцию
// со случайным именем (см. support/ui.ts), но общий процесс FakeMachine на
// один WS порт интеграции — не проблема (у каждого теста своя интеграция),
// параллелизм отключён просто для предсказуемости прогонов на слабом
// dev/CI-стенде, не по необходимости.
import { defineConfig, devices } from "@playwright/test";

import { baseUrl } from "./support/env";

export default defineConfig({
  testDir: ".",
  testMatch: /.*\.spec\.ts/,
  timeout: 90_000,
  expect: { timeout: 15_000 },
  fullyParallel: false,
  workers: 1,
  retries: process.env.CI ? 1 : 0,
  reporter: [["list"]],
  outputDir: "./test-results",
  use: {
    baseURL: baseUrl(),
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
    video: "retain-on-failure",
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
});
