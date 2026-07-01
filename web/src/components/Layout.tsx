import { NavLink, Outlet } from "react-router-dom";

import { buttonVariants } from "@/components/ui/button";
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
 */
export function Layout(): JSX.Element {
  return (
    <div className="min-h-screen bg-background text-foreground">
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
  );
}
