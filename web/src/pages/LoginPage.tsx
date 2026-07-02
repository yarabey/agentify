import { useState } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { useForm } from "react-hook-form";
import { Link, useNavigate } from "react-router-dom";
import { z } from "zod";

import { apiClient } from "@/api/client";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useAuth } from "@/context/AuthContext";

/**
 * Схема формы логина (тикет 9.2, FR A3). Валидация — только "поле не пустое":
 * бэкенд (`LoginRequest`, `orchestrator/internal/api/types.gen.go`) не
 * навязывает формат username/password при логине (в отличие от регистрации),
 * дублировать серверные правила здесь незачем.
 */
const loginSchema = z.object({
  username: z.string().min(1, "Введите имя пользователя"),
  password: z.string().min(1, "Введите пароль"),
});

type LoginFormValues = z.infer<typeof loginSchema>;

/**
 * Экран входа (`/login`, тикет 9.2, FR A3; Gherkin §1).
 *
 * Назначение (бизнес): единственный способ получить пару токенов после того,
 * как аккаунт уже создан отдельным шагом — регистрацией (`RegisterPage`) НЕ
 * логинит автоматически (см. её docstring). Ошибка неверных кредов
 * показывается одним общим сообщением: бэкенд намеренно не раскрывает, что
 * именно неверно — username или пароль (единый `invalid_credentials`, см.
 * `orchestrator/internal/api/auth.go`), и экран следует тому же принципу.
 *
 * Как устроено (тех): `react-hook-form` + `zod` — валидация "поле не
 * пустое" на клиенте до отправки; `POST /auth/login` через `apiClient`
 * (`src/api/client.ts`); при успехе — `AuthContext.login()` сохраняет пару
 * токенов и переводит `isAuthenticated` в `true`, после чего редирект на
 * домашний экран `/tasks` (`RequireGuest` в `App.tsx` в любом случае не
 * пустит уже вошедшего пользователя обратно на `/login`).
 */
export function LoginPage(): JSX.Element {
  const auth = useAuth();
  const navigate = useNavigate();
  const [formError, setFormError] = useState<string | null>(null);

  const {
    register,
    handleSubmit,
    formState: { errors, isSubmitting },
  } = useForm<LoginFormValues>({
    resolver: zodResolver(loginSchema),
  });

  const onSubmit = handleSubmit(async (values) => {
    setFormError(null);
    const { data, error } = await apiClient.POST("/auth/login", {
      body: values,
    });
    if (error || !data) {
      setFormError("Неверный логин или пароль");
      return;
    }
    auth.login({
      accessToken: data.access_token,
      refreshToken: data.refresh_token,
    });
    navigate("/tasks", { replace: true });
  });

  return (
    <section className="mx-auto flex min-h-screen max-w-sm flex-col justify-center gap-6 px-4">
      <div>
        <h1 className="text-2xl font-bold">Вход</h1>
        <p className="mt-2 text-muted-foreground">
          Войдите с уже зарегистрированным аккаунтом.
        </p>
      </div>

      <form className="flex flex-col gap-4" onSubmit={onSubmit} noValidate>
        <div className="flex flex-col gap-2">
          <Label htmlFor="login-username">Имя пользователя</Label>
          <Input
            id="login-username"
            autoComplete="username"
            {...register("username")}
          />
          {errors.username && (
            <p className="text-sm text-destructive">
              {errors.username.message}
            </p>
          )}
        </div>

        <div className="flex flex-col gap-2">
          <Label htmlFor="login-password">Пароль</Label>
          <Input
            id="login-password"
            type="password"
            autoComplete="current-password"
            {...register("password")}
          />
          {errors.password && (
            <p className="text-sm text-destructive">
              {errors.password.message}
            </p>
          )}
        </div>

        {formError && (
          <p role="alert" className="text-sm text-destructive">
            {formError}
          </p>
        )}

        <Button type="submit" disabled={isSubmitting}>
          {isSubmitting ? "Входим…" : "Войти"}
        </Button>
      </form>

      <p className="text-sm text-muted-foreground">
        Нет аккаунта?{" "}
        <Link
          className="font-medium text-primary underline-offset-4 hover:underline"
          to="/register"
        >
          Зарегистрироваться
        </Link>
      </p>
    </section>
  );
}
