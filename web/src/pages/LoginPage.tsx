/**
 * Заглушка экрана логина (`/login`). Назначение: место для будущей формы
 * входа (react-hook-form + zod, генерённый API-клиент `src/api/client.ts`) —
 * полноценная реализация экрана запланирована в тикете **9.2**. Здесь только
 * заголовок, чтобы shell-тест (тикет 9.1) мог проверить, что маршрут
 * рендерится без ошибок.
 */
export function LoginPage(): JSX.Element {
  return (
    <section>
      <h1 className="text-2xl font-bold">Вход</h1>
      <p className="mt-2 text-muted-foreground">
        Экран входа будет реализован в тикете 9.2.
      </p>
    </section>
  );
}
