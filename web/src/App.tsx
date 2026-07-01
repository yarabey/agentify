import { Navigate, Route, Routes } from "react-router-dom";

import { Layout } from "@/components/Layout";
import { IntegrationsPage } from "@/pages/IntegrationsPage";
import { LoginPage } from "@/pages/LoginPage";
import { SettingsPage } from "@/pages/SettingsPage";
import { TasksPage } from "@/pages/TasksPage";

/**
 * Корневой компонент web-канала — маршрутизация оболочки (тикет 9.1, "каркас
 * PWA", FR D1). Назначение: связывает shell (`Layout`, шапка+навигация) с
 * экранами-заглушками будущих тикетов. `/login` рендерится вне `Layout`
 * (экран входа без общей навигации, тикет 9.2); `/`, `/integrations`,
 * `/settings` и домашний `/tasks` — внутри `Layout` через `<Outlet />`.
 * Провайдеры (QueryClientProvider, роутер) подключаются на уровень выше, в
 * `main.tsx` (и в тестах — своя обёртка, см. App.test.tsx), чтобы `App` можно
 * было переиспользовать и с `BrowserRouter`, и с `MemoryRouter`.
 */
export function App(): JSX.Element {
  return (
    <Routes>
      <Route path="/login" element={<LoginPage />} />
      <Route element={<Layout />}>
        <Route index element={<Navigate to="/tasks" replace />} />
        <Route path="/tasks" element={<TasksPage />} />
        <Route path="/integrations" element={<IntegrationsPage />} />
        <Route path="/settings" element={<SettingsPage />} />
      </Route>
    </Routes>
  );
}
