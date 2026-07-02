import type { ManifestOptions } from "vite-plugin-pwa";

/**
 * PWA-манифест web-канала оркестратора (тикет 9.1, FR D1).
 *
 * Назначение (бизнес): делает web-канал устанавливаемым (installable) на
 * телефоне/десктопе и задаёт оффлайн-оболочку (standalone-режим без адресной
 * строки браузера) — часть требования D1 "web как полноценный канал наравне
 * с Telegram-ботом".
 *
 * Как устроено (тех): вынесен в отдельный модуль (а не инлайн в
 * vite.config.ts), чтобы один и тот же объект использовали и
 * `VitePWA({ manifest: pwaManifest })` при сборке, и юнит-тест
 * (src/pwaManifest.test.ts), проверяющий валидность обязательных полей без
 * реальной Vite-сборки. Иконка — `public/icon.svg` (SVG-плейсхолдер,
 * `sizes: 'any'` покрывает любое разрешение экрана).
 */
export const pwaManifest: Partial<ManifestOptions> = {
  name: "Agentify",
  short_name: "Agentify",
  description:
    "Agentify — постановка задач агенту, диалог и история из браузера.",
  start_url: "/tasks",
  scope: "/",
  display: "standalone",
  theme_color: "#4338ca",
  background_color: "#ffffff",
  lang: "ru",
  icons: [
    {
      src: "icon.svg",
      sizes: "any",
      type: "image/svg+xml",
      purpose: "any",
    },
  ],
};
