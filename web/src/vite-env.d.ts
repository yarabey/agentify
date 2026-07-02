/// <reference types="vite/client" />

interface ImportMetaEnv {
  /** Базовый URL REST API оркестратора; по умолчанию относительный /api (см. src/api/client.ts). */
  readonly VITE_API_BASE_URL?: string;
  /**
   * Username Telegram-бота (без @) для сборки кликабельного deep-link на
   * экране «Настройки» (тикет 9.6, FR D3, см. src/pages/SettingsPage.tsx).
   * Опционален: без него секция привязки Telegram по-прежнему показывает код
   * и инструкцию `/start <code>`, просто без готовой ссылки.
   */
  readonly VITE_TELEGRAM_BOT_USERNAME?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
