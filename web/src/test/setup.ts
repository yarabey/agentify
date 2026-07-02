import { cleanup } from "@testing-library/react";
import { afterEach } from "vitest";

import "@testing-library/jest-dom/vitest";

// Vitest setup-файл (тикет 9.1): подключает jest-dom матчеры
// (toBeInTheDocument и т.п.) ко всем тестам через vite.config.ts ->
// test.setupFiles. `cleanup()` после каждого теста нужен явно — глобальные
// хуки Testing Library (`globals: true`) намеренно не включены, чтобы
// импорты `describe`/`it`/`expect` оставались явными.
afterEach(() => {
  cleanup();
});
