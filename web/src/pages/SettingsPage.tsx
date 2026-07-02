import { useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";

import { apiClient } from "@/api/client";
import { Button } from "@/components/ui/button";
import { useAuth } from "@/context/AuthContext";

/**
 * React Query ключ статуса «показывает ли этот аккаунт токен регистрации»
 * (тикет 9.6, FR A2). Отдельный от других ключей (`INTEGRATIONS_QUERY_KEY` и
 * т.п.) — своя независимая инвалидация не нужна: значение неизменно за время
 * сессии в MVP.
 */
const ADMIN_REGISTRATION_TOKEN_QUERY_KEY = ["admin-registration-token"] as const;

/**
 * Результат опроса `GET /admin/registration-token` в форме, удобной для
 * рендера: различает «я администратор, вот токен» и «я не администратор»
 * (403) БЕЗ превращения второго случая в ошибку запроса (тикет 9.6, FR A2,
 * Gherkin §1 «Администратор видит токен регистрации» — обратный сценарий,
 * «не-администратор не видит», тоже штатный, не сбойный).
 */
type AdminRegistrationTokenResult =
  | { isAdmin: true; token: string }
  | { isAdmin: false };

/**
 * Секция «Токен регистрации» экрана «Настройки» (тикет 9.6, FR A2, Gherkin §1
 * «Администратор видит токен регистрации»).
 *
 * Назначение (бизнес): доступ в систему закрытый — новый аккаунт заводится
 * только по действующему токену регистрации (тикет 1.2, `POST
 * /auth/register`); кто-то должен этот токен видеть, чтобы поделиться им с
 * новым пользователем. Показ — уже готовый эндпоинт `GET
 * /admin/registration-token` (тикет 1.6) — здесь только переиспользуется, не
 * дублируется.
 *
 * Как устроено (тех): фронт не знает заранее, администратор ли текущий
 * пользователь (в JWT нет claim `is_admin`, см. `internal/auth/jwt.go`) —
 * узнаёт это ПО ФАКТУ ответа: `200` → администратор, показываем токен; `403`
 * → не администратор, секция вообще не рендерится (тихо, без баннера с
 * "недоступно" — сам факт видимости заблокированной функции — тоже лишняя
 * информация). Любая другая ошибка (сеть/500) — секция тоже не рендерится:
 * не блокирующая часть экрана, не стоит пугать пользователя, у которого,
 * скорее всего, и не было бы доступа.
 */
function RegistrationTokenSection(): JSX.Element | null {
  const query = useQuery<AdminRegistrationTokenResult>({
    queryKey: ADMIN_REGISTRATION_TOKEN_QUERY_KEY,
    queryFn: async () => {
      const { data, response } = await apiClient.GET(
        "/admin/registration-token",
      );
      if (response.status === 403) {
        return { isAdmin: false };
      }
      if (!data) {
        throw new Error("не удалось получить токен регистрации");
      }
      return { isAdmin: true, token: data.token ?? "" };
    },
    retry: false,
  });

  if (query.isLoading) {
    return (
      <section className="flex flex-col gap-2 rounded-md border border-border p-4">
        <h2 className="text-lg font-semibold">Токен регистрации</h2>
        <p className="text-muted-foreground">Загрузка…</p>
      </section>
    );
  }

  if (!query.data || !query.data.isAdmin) {
    return null;
  }

  return (
    <section className="flex flex-col gap-3 rounded-md border border-border p-4">
      <div>
        <h2 className="text-lg font-semibold">Токен регистрации</h2>
        <p className="text-sm text-muted-foreground">
          Доступно только администратору (FR A2). Поделитесь этим токеном с
          новым пользователем — он понадобится при регистрации.
        </p>
      </div>
      <p className="break-all rounded-md bg-muted p-2 font-mono text-sm">
        {query.data.token}
      </p>
    </section>
  );
}

/** Успешный результат `POST /channels/telegram/link-code` (тикет 9.6). */
interface LinkCodeResult {
  code: string;
  expiresAt: string;
}

/**
 * Username Telegram-бота для сборки кликабельного deep-link
 * (`https://t.me/<bot>?start=<code>`, FR D3). Необязателен: без него секция
 * всё равно показывает код и инструкцию набрать `/start <code>` руками —
 * ровно то, что бот принимает в любом случае (см. `bot/start.go`). Тот же
 * принцип опционального `VITE_*`-конфига со graceful-деградацией, что у
 * `VITE_API_BASE_URL` (`src/api/client.ts`).
 */
const TELEGRAM_BOT_USERNAME = import.meta.env.VITE_TELEGRAM_BOT_USERNAME as
  | string
  | undefined;

/**
 * Секция «Привязать Telegram» экрана «Настройки» (тикет 9.6, FR D3, Gherkin
 * §6 «Уведомления» — привязка Telegram-аккаунта).
 *
 * Назначение (бизнес): прежде чем бот сможет действовать от имени
 * пользователя (тикет 10.3) или доставлять ему уведомления (тикет 10.4),
 * нужно один раз связать telegram_user_id с аккаунтом. Кнопка запрашивает
 * одноразовый код (`POST /channels/telegram/link-code`, выпускается СТРОГО
 * на аккаунт вызывающего — FR A2) и показывает его вместе с инструкцией
 * отправить боту `/start <code>`; обмен кода на саму привязку — на стороне
 * бота (тикет 10.2), эта кнопка привязку не создаёт.
 *
 * Как устроено (тех): `useMutation` — код не запрашивается на монтировании
 * экрана (в отличие от токена регистрации), а только по явному действию
 * пользователя: код одноразовый и с TTL, бессмысленно тратить его молча при
 * каждом заходе в «Настройки».
 */
function TelegramLinkSection(): JSX.Element {
  const [error, setError] = useState<string | null>(null);

  const mutation = useMutation({
    mutationFn: async (): Promise<LinkCodeResult> => {
      const { data, error: reqError } = await apiClient.POST(
        "/channels/telegram/link-code",
      );
      if (reqError || !data?.code || !data.expires_at) {
        throw reqError ?? new Error("не удалось выпустить код привязки");
      }
      return { code: data.code, expiresAt: data.expires_at };
    },
    onMutate: () => setError(null),
    onError: () =>
      setError("Не удалось сгенерировать код. Попробуйте ещё раз."),
  });

  const result = mutation.data;
  const deepLink =
    result && TELEGRAM_BOT_USERNAME
      ? `https://t.me/${TELEGRAM_BOT_USERNAME}?start=${result.code}`
      : null;

  return (
    <section className="flex flex-col gap-3 rounded-md border border-border p-4">
      <div>
        <h2 className="text-lg font-semibold">Telegram</h2>
        <p className="text-sm text-muted-foreground">
          Привяжите Telegram-аккаунт, чтобы получать уведомления и ставить
          задачи из чата с ботом.
        </p>
      </div>

      <div>
        <Button
          onClick={() => mutation.mutate()}
          disabled={mutation.isPending}
        >
          {mutation.isPending ? "Генерируем…" : "Привязать Telegram"}
        </Button>
      </div>

      {error && (
        <p role="alert" className="text-sm text-destructive">
          {error}
        </p>
      )}

      {result && (
        <div
          role="status"
          className="flex flex-col gap-2 rounded-md bg-muted p-3"
        >
          <p>
            Отправьте боту команду:{" "}
            <span className="break-all font-mono text-sm">
              /start {result.code}
            </span>
          </p>
          {deepLink && (
            <a
              className="text-sm font-medium text-primary underline-offset-4 hover:underline"
              href={deepLink}
              target="_blank"
              rel="noreferrer"
            >
              Открыть чат с ботом
            </a>
          )}
          <p className="text-xs text-muted-foreground">
            Код действителен до {new Date(result.expiresAt).toLocaleString()}.
          </p>
        </div>
      )}
    </section>
  );
}

/**
 * Секция «Сессия» экрана «Настройки» (тикет 9.6) — кнопка выхода,
 * анонсированная ещё в `AuthContext` (см. его godoc, "будущая кнопка «Выйти»
 * в Settings"). Переиспользует `AuthContext.logout()` (тикет 9.2) — та же
 * best-effort `POST /auth/logout` + гарантированная локальная очистка
 * токенов, которой в MVP больше неоткуда было бы вызваться в UI.
 */
function LogoutSection(): JSX.Element {
  const auth = useAuth();
  const navigate = useNavigate();
  const [isLoggingOut, setIsLoggingOut] = useState(false);

  async function handleLogout() {
    setIsLoggingOut(true);
    try {
      await auth.logout();
      navigate("/login", { replace: true });
    } finally {
      setIsLoggingOut(false);
    }
  }

  return (
    <section className="flex flex-col gap-3 rounded-md border border-border p-4">
      <h2 className="text-lg font-semibold">Сессия</h2>
      <div>
        <Button
          variant="outline"
          disabled={isLoggingOut}
          onClick={() => {
            void handleLogout();
          }}
        >
          {isLoggingOut ? "Выходим…" : "Выйти"}
        </Button>
      </div>
    </section>
  );
}

/**
 * Экран настроек (`/settings`, тикет 9.6, FR A2, D3).
 *
 * Назначение (бизнес): единственное место, где пользователь видит токен
 * регистрации (если он администратор, FR A2) и привязывает Telegram-аккаунт
 * (FR D3) — обе функции переиспользуют уже готовые эндпоинты (тикеты 1.6 и
 * 9.6 соответственно), экран только их собирает вместе с выходом из сессии.
 *
 * Как устроено (тех): три независимые секции — `RegistrationTokenSection`
 * (условно видима только администратору), `TelegramLinkSection`,
 * `LogoutSection`; каждая сама владеет своим сетевым состоянием, экран не
 * держит общего стейта между ними.
 */
export function SettingsPage(): JSX.Element {
  return (
    <section className="flex flex-col gap-6">
      <div>
        <h1 className="text-2xl font-bold">Настройки</h1>
        <p className="mt-2 text-muted-foreground">
          Администрирование аккаунта и привязка внешних каналов.
        </p>
      </div>

      <RegistrationTokenSection />
      <TelegramLinkSection />
      <LogoutSection />
    </section>
  );
}
