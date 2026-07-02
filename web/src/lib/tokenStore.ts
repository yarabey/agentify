/**
 * Локальное хранилище пары токенов web-канала (тикет 9.2, FR A3).
 *
 * Назначение (бизнес): после логина/refresh пара токенов (access — короткий
 * JWT, refresh — непрозрачная строка, см. `orchestrator/internal/api/auth.go`)
 * должна пережить перезагрузку вкладки, иначе пользователь разлогинивался бы
 * при каждом обновлении страницы. Храним в `localStorage` под явными ключами.
 *
 * Как устроено (тех): это намеренно НЕ React-модуль — простые функции без
 * хуков и без импорта React. `src/api/client.ts` (мидларь HTTP-клиента,
 * авто-Authorization-заголовок и авто-refresh по 401) читает/пишет токены
 * отсюда напрямую; если бы вместо этого он зависел от React-контекста
 * (`AuthContext`), получился бы цикл модулей (`client.ts` -> `AuthContext.tsx`
 * -> `client.ts`, т.к. `AuthContext` сам ходит в `apiClient` при логине/логауте).
 *
 * Синхронизация с React-состоянием: когда `client.ts` вызывает `clearTokens()`
 * (авто-refresh не удался — refresh истёк/отозван/сеть недоступна), React
 * должен узнать об этом синхронно, чтобы `AuthContext.isAuthenticated` не
 * разошёлся с реальностью (токенов уже нет, но UI ещё считает пользователя
 * вошедшим). Для этого `clearTokens()` шлёт `CustomEvent` на `window`, на
 * который подписывается `AuthContext` через `onTokensCleared()`.
 */

const ACCESS_TOKEN_KEY = "agentify.accessToken";
const REFRESH_TOKEN_KEY = "agentify.refreshToken";

/** Имя `window`-события, которое `clearTokens()` шлёт после очистки. */
const TOKENS_CLEARED_EVENT = "agentify:auth:cleared";

/** Пара токенов, как её выдаёт `POST /auth/login` / `POST /auth/refresh`. */
export interface TokenPair {
  accessToken: string;
  refreshToken: string;
}

/** Текущий access-токен из `localStorage` или `null`, если пользователь не вошёл. */
export function getAccessToken(): string | null {
  return localStorage.getItem(ACCESS_TOKEN_KEY);
}

/** Текущий refresh-токен из `localStorage` или `null`, если пользователь не вошёл. */
export function getRefreshToken(): string | null {
  return localStorage.getItem(REFRESH_TOKEN_KEY);
}

/** Сохраняет пару токенов (успешный логин или ротация при refresh). */
export function setTokens(pair: TokenPair): void {
  localStorage.setItem(ACCESS_TOKEN_KEY, pair.accessToken);
  localStorage.setItem(REFRESH_TOKEN_KEY, pair.refreshToken);
}

/**
 * Удаляет оба токена и оповещает подписчиков (`onTokensCleared`).
 *
 * Вызывается и явным logout'ом (`AuthContext.logout`), и мидларью
 * HTTP-клиента при неудачном авто-refresh (`src/api/client.ts`) — в обоих
 * случаях сессия на этом клиенте закончена.
 */
export function clearTokens(): void {
  localStorage.removeItem(ACCESS_TOKEN_KEY);
  localStorage.removeItem(REFRESH_TOKEN_KEY);
  window.dispatchEvent(new Event(TOKENS_CLEARED_EVENT));
}

/**
 * Подписка на факт очистки токенов. Возвращает функцию отписки (для
 * `useEffect` cleanup в `AuthContext`).
 */
export function onTokensCleared(callback: () => void): () => void {
  window.addEventListener(TOKENS_CLEARED_EVENT, callback);
  return () => window.removeEventListener(TOKENS_CLEARED_EVENT, callback);
}
