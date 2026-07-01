import { NavLink, Outlet } from "react-router-dom";

import { NotificationBanner } from "@/components/NotificationBanner";
import { buttonVariants } from "@/components/ui/button";
import { NotificationProvider } from "@/context/NotificationContext";
import { cn } from "@/lib/utils";

const NAV_ITEMS = [
  { to: "/tasks", label: "Задачи" },
  { to: "/integrations", label: "Интеграции" },
  { to: "/settings", label: "Настройки" },
];

/**
 * Оболочка (shell) web-канала — шапка с навигацией + область экрана (тикет
 * 9.1, "каркас PWA", FR D1). Назначение: общая для всех экранов рамка
 * (навигация между /tasks, /integrations, /settings; /login — отдельно, вне
 * shell, см. App.tsx), которую рендерит react-router-dom через `<Outlet />`.
 * Реальный контент экранов — тикеты 9.2-9.6; здесь только заглушки.
 *
 * `<NotificationProvider>` (тикет 7.2, дополнено тикетом 9.5) держит здесь
 * единственное WS-соединение `/ws` пользователя — `Layout` рендерится только
 * под `RequireAuth` (см. App.tsx) и не размонтируется при навигации между
 * приватными экранами, а размонтируется вместе со всем остальным при
 * логауте/редиректе на `/login`, что и закрывает соединение без отдельной
 * логики очистки здесь. `<NotificationBanner />` и экраны внутри `<Outlet />`
 * (например, `TaskDetailPage`, тикет 9.5) читают из одного и того же
 * контекста, не открывая по второму соединению на экран.
 */
export function Layout(): JSX.Element {
  return (
    <NotificationProvider>
      <div className="min-h-screen bg-background text-foreground">
        <NotificationBanner />
        <header className="border-b border-border">
          <div className="container flex items-center justify-between py-4">
            <span className="text-lg font-semibold">Agentify</span>
            <nav className="flex items-center gap-2">
              {NAV_ITEMS.map((item) => (
                <NavLink
                  key={item.to}
                  to={item.to}
                  className={({ isActive }) =>
                    cn(
                      buttonVariants({
                        variant: isActive ? "secondary" : "ghost",
                        size: "sm",
                      }),
                    )
                  }
                >
                  {item.label}
                </NavLink>
              ))}
            </nav>
          </div>
        </header>
        <main className="container py-6">
          <Outlet />
        </main>
      </div>
    </NotificationProvider>
  );
}
