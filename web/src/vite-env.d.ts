/// <reference types="vite/client" />

interface ImportMetaEnv {
  /** Базовый URL REST API оркестратора; по умолчанию относительный /api (см. src/api/client.ts). */
  readonly VITE_API_BASE_URL?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
