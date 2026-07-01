import React from "react";
import ReactDOM from "react-dom/client";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter } from "react-router-dom";

import { App } from "@/App";
import { AuthProvider } from "@/context/AuthContext";
import "@/index.css";

/**
 * Точка входа web-канала (тикет 9.1, `AuthProvider` — тикет 9.2). Оборачивает
 * `App` в провайдеры, общие для всего приложения: `QueryClientProvider`
 * (TanStack Query — серверное состояние для будущих экранов 9.3-9.6),
 * `BrowserRouter` (клиентский роутинг; SPA-фолбэк на index.html обеспечивает
 * web/Caddyfile) и `AuthProvider` (состояние аутентификации, FR A3) —
 * намеренно внутри `BrowserRouter`, чтобы экраны/гварды могли пользоваться
 * и `useAuth()`, и хуками роутера (`useNavigate`/`useLocation`) в одном дереве.
 */
const queryClient = new QueryClient();

ReactDOM.createRoot(document.getElementById("root") as HTMLElement).render(
  <React.StrictMode>
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <AuthProvider>
          <App />
        </AuthProvider>
      </BrowserRouter>
    </QueryClientProvider>
  </React.StrictMode>,
);
