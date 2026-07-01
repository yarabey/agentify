import { Button } from "@/components/ui/button";

/**
 * Заглушка домашнего экрана — списка задач (`/tasks`, домашняя страница
 * web-канала). Назначение: место для будущей постановки/списка/диалога по
 * задачам (FR D/E/F/H) — полноценная реализация запланирована в тикете
 * **9.4**. Кнопка ниже — не действие, а подтверждение того, что связка
 * Tailwind + shadcn/ui реально работает (тикет 9.1), а не только
 * сконфигурирована.
 */
export function TasksPage(): JSX.Element {
  return (
    <section>
      <h1 className="text-2xl font-bold">Задачи</h1>
      <p className="mt-2 text-muted-foreground">
        Экран задач будет реализован в тикете 9.4.
      </p>
      <Button className="mt-4" disabled>
        Новая задача
      </Button>
    </section>
  );
}
