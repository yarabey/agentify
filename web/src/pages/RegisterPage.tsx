import { useEffect, useRef, useState } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { useForm } from "react-hook-form";
import { Link, useNavigate } from "react-router-dom";
import { z } from "zod";

import { apiClient } from "@/api/client";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

/**
 * Схема формы регистрации (тикет 9.2, FR A1). Валидация — "поле не пустое":
 * все три поля обязательны в `RegisterRequest`
 * (`orchestrator/internal/api/types.gen.go`); реальная проверка токена
 * регистрации/занятости username — на бэкенде (403/409 соответственно).
 */
const registerSchema = z.object({
  registration_token: z.string().min(1, "Введите токен регистрации"),
  username: z.string().min(1, "Введите имя пользователя"),
  password: z.string().min(1, "Введите пароль"),
});

type RegisterFormValues = z.infer<typeof registerSchema>;

/** Задержка перед авто-переходом на /login после успешной регистрации. */
const REDIRECT_DELAY_MS = 1200;

/**
 * Экран регистрации по токену (`/register`, тикет 9.2, FR A1; Gherkin §1
 * «Успешная регистрация по валидному токену», «Регистрация без токена
 * запрещена»).
 *
 * Назначение (бизнес): регистрация доступна только с валидным
 * `registration_token` — без него/с невалидным бэкенд отвечает 403
 * (`orchestrator/internal/api/`, тикет 1.2). Важно: успешная регистрация
 * **не логинит автоматически** — бэкенд не возвращает пару токенов на
 * `201 Created` (см. `openapi.yaml` `/auth/register`), поэтому после успеха
 * экран лишь показывает сообщение и переводит пользователя на `/login`
 * логиниться отдельным шагом (ровно так описано в Gherkin-сценарии).
 *
 * Как устроено (тех): `react-hook-form` + `zod`; `POST /auth/register` через
 * `apiClient`. Ошибка (невалидный/неактивный токен регистрации, занятое имя
 * пользователя) показывается одним общим сообщением — экран не пытается
 * различить 403 vs 409 для пользователя, т.к. секретность самого факта
 * занятости username в закрытой системе не критична, но и уточнять причину
 * отказа без явной необходимости незачем.
 */
export function RegisterPage(): JSX.Element {
  const navigate = useNavigate();
  const [formError, setFormError] = useState<string | null>(null);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);
  const redirectTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    return () => {
      if (redirectTimer.current) {
        clearTimeout(redirectTimer.current);
      }
    };
  }, []);

  const {
    register,
    handleSubmit,
    formState: { errors, isSubmitting },
  } = useForm<RegisterFormValues>({
    resolver: zodResolver(registerSchema),
  });

  const onSubmit = handleSubmit(async (values) => {
    setFormError(null);
    setSuccessMessage(null);
    const { error } = await apiClient.POST("/auth/register", {
      body: values,
    });
    if (error) {
      setFormError(
        "Не удалось зарегистрироваться. Проверьте токен регистрации и имя пользователя.",
      );
      return;
    }
    setSuccessMessage("Аккаунт создан. Сейчас вы перейдёте на экран входа.");
    redirectTimer.current = setTimeout(() => {
      navigate("/login");
    }, REDIRECT_DELAY_MS);
  });

  return (
    <section className="mx-auto flex min-h-screen max-w-sm flex-col justify-center gap-6 px-4">
      <div>
        <h1 className="text-2xl font-bold">Регистрация</h1>
        <p className="mt-2 text-muted-foreground">
          Нужен действующий токен регистрации (закрытый доступ).
        </p>
      </div>

      <form className="flex flex-col gap-4" onSubmit={onSubmit} noValidate>
        <div className="flex flex-col gap-2">
          <Label htmlFor="register-token">Токен регистрации</Label>
          <Input
            id="register-token"
            {...register("registration_token")}
          />
          {errors.registration_token && (
            <p className="text-sm text-destructive">
              {errors.registration_token.message}
            </p>
          )}
        </div>

        <div className="flex flex-col gap-2">
          <Label htmlFor="register-username">Имя пользователя</Label>
          <Input
            id="register-username"
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
          <Label htmlFor="register-password">Пароль</Label>
          <Input
            id="register-password"
            type="password"
            autoComplete="new-password"
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
        {successMessage && (
          <p role="status" className="text-sm text-primary">
            {successMessage}
          </p>
        )}

        <Button type="submit" disabled={isSubmitting}>
          {isSubmitting ? "Регистрируем…" : "Зарегистрироваться"}
        </Button>
      </form>

      <p className="text-sm text-muted-foreground">
        Уже есть аккаунт?{" "}
        <Link
          className="font-medium text-primary underline-offset-4 hover:underline"
          to="/login"
        >
          Войти
        </Link>
      </p>
    </section>
  );
}
