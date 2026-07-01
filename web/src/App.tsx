import { Navigate, Outlet, Route, Routes, useLocation } from "react-router-dom";

import { Layout } from "@/components/Layout";
import { useAuth } from "@/context/AuthContext";
import { IntegrationsPage } from "@/pages/IntegrationsPage";
import { LoginPage } from "@/pages/LoginPage";
import { RegisterPage } from "@/pages/RegisterPage";
import { SettingsPage } from "@/pages/SettingsPage";
import { TaskDetailPage } from "@/pages/TaskDetailPage";
import { TasksPage } from "@/pages/TasksPage";

/**
 * Guard приватных экранов (тикет 9.2, FR A3, A4): без входа (`useAuth()`)
 * делать здесь нечего — редиректит на `/login`, сохраняя исходный путь в
 * `state.from`, чтобы `LoginPage` при желании мог вернуть пользователя туда
 * же после успешного входа.
 */
function RequireAuth(): JSX.Element {
  const { isAuthenticated } = useAuth();
  const location = useLocation();

  if (!isAuthenticated) {
    return <Navigate to="/login" state={{ from: location }} replace />;
  }
  return <Outlet />;
}

/**
 * Обратный guard для `/login` и `/register`: уже вошедшему пользователю
 * незачем снова видеть форму входа/регистрации — уводим на домашний
 * экран `/tasks`.
 */
function RequireGuest(): JSX.Element {
  const { isAuthenticated } = useAuth();

  if (isAuthenticated) {
    return <Navigate to="/tasks" replace />;
  }
  return <Outlet />;
}

/**
 * Корневой компонент web-канала — маршрутизация оболочки (тикет 9.1, "каркас
 * PWA", FR D1; guard'ы и `/register` — тикет 9.2, FR A1/A3). Назначение:
 * связывает shell (`Layout`, шапка+навигация) с экранами. `/login` и
 * `/register` рендерятся вне `Layout` (экраны входа/регистрации без общей
 * навигации) и доступны только гостю (`RequireGuest`); `/`, `/tasks`,
 * `/tasks/:id`, `/integrations`, `/settings` — внутри `Layout` через
 * `<Outlet />` и только авторизованному пользователю (`RequireAuth`). Провайдеры
 * (QueryClientProvider, роутер, AuthProvider) подключаются на уровень выше, в
 * `main.tsx` (и в тестах — своя обёртка, см. App.test.tsx), чтобы `App` можно
 * было переиспользовать и с `BrowserRouter`, и с `MemoryRouter`.
 */
export function App(): JSX.Element {
  return (
    <Routes>
      <Route element={<RequireGuest />}>
        <Route path="/login" element={<LoginPage />} />
        <Route path="/register" element={<RegisterPage />} />
      </Route>
      <Route element={<RequireAuth />}>
        <Route element={<Layout />}>
          <Route index element={<Navigate to="/tasks" replace />} />
          <Route path="/tasks" element={<TasksPage />} />
          <Route path="/tasks/:id" element={<TaskDetailPage />} />
          <Route path="/integrations" element={<IntegrationsPage />} />
          <Route path="/settings" element={<SettingsPage />} />
        </Route>
      </Route>
    </Routes>
  );
}
