/// <reference types="vitest/config" />
import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";
import { VitePWA } from "vite-plugin-pwa";
import { pwaManifest } from "./src/pwaManifest";

// Конфигурация Vite web-канала (тикет 9.1, FR D1).
//
// Назначение (бизнес): собрать installable PWA-оболочку (роутинг + TanStack
// Query + Tailwind/shadcn), которую раздаёт Caddy в проде (web/Dockerfile,
// web/Caddyfile). Реальные экраны (auth/интеграции/задачи/настройки) —
// отдельные тикеты 9.2-9.6, здесь только каркас.
//
// Как устроено (тех):
//   * `@vitejs/plugin-react` — Fast Refresh для React 18.
//   * `vite-plugin-pwa` генерирует service worker (registerType: 'autoUpdate' —
//     новая версия оболочки подхватывается без ручного подтверждения
//     пользователем) и `manifest.webmanifest` из `pwaManifest` (см.
//     src/pwaManifest.ts — вынесен в отдельный модуль, чтобы его же
//     переиспользовал юнит-тест валидности манифеста без реальной сборки).
//   * Алиас `@` -> `src` зеркалит `paths` в tsconfig.app.json.
//   * Блок `test` (через `vitest/config`) — Vitest с jsdom-окружением и
//     setup-файлом для jest-dom матчеров (см. src/test/setup.ts).
//   * `test.env.VITE_API_BASE_URL` (тикет 9.2): под jsdom `fetch`/`Request`
//     (Node/undici) не резолвят относительные URL против `window.location`,
//     как это делает браузер — `new Request("/api/...")` иначе падает с
//     "Failed to parse URL". Задаём абсолютный `baseUrl` только для тестов
//     здесь (а не в `.env.test`), т.к. `.env.*` в `.gitignore` (см. AGENTS.md
//     §8) — файл не закоммитился бы и тесты ломались бы на чистом клоне/CI.
//     В проде эта переменная не используется (там Caddy на одном origin).
export default defineConfig({
  plugins: [
    react(),
    VitePWA({
      registerType: "autoUpdate",
      manifest: pwaManifest,
      // Оффлайн-оболочка (оболочка приложения кэшируется на уровне SW), сами
      // API-запросы в MVP не кэшируем — актуальность данных важнее оффлайн-CRUD.
      workbox: {
        globPatterns: ["**/*.{js,css,html,svg,ico,png,woff2}"],
      },
    }),
  ],
  resolve: {
    alias: {
      "@": fileURLToPath(new URL("./src", import.meta.url)),
    },
  },
  test: {
    environment: "jsdom",
    setupFiles: ["./src/test/setup.ts"],
    css: true,
    env: {
      VITE_API_BASE_URL: "http://localhost/api",
    },
  },
});
