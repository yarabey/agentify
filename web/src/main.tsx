import React from "react";
import ReactDOM from "react-dom/client";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter } from "react-router-dom";

import { App } from "@/App";
import "@/index.css";

/**
 * Точка входа web-канала (тикет 9.1). Оборачивает `App` в провайдеры,
 * общие для всего приложения: `QueryClientProvider` (TanStack Query —
 * серверное состояние для будущих экранов 9.2-9.6) и `BrowserRouter`
 * (клиентский роутинг; SPA-фолбэк на index.html обеспечивает web/Caddyfile).
 */
const queryClient = new QueryClient();

ReactDOM.createRoot(document.getElementById("root") as HTMLElement).render(
  <React.StrictMode>
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <App />
      </BrowserRouter>
    </QueryClientProvider>
  </React.StrictMode>,
);
