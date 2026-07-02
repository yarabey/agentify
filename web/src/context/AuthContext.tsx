import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";

import { apiClient } from "@/api/client";
import {
  clearTokens,
  getAccessToken,
  getRefreshToken,
  onTokensCleared,
  setTokens,
  type TokenPair,
} from "@/lib/tokenStore";

interface AuthContextValue {
  /** Есть ли сохранённый access-токен прямо сейчас (тикет 9.2, FR A3). */
  isAuthenticated: boolean;
  /** Сохраняет пару токенов после успешного `POST /auth/login`. */
  login: (tokens: TokenPair) => void;
  /** Best-effort `POST /auth/logout` + гарантированная локальная очистка. */
  logout: () => Promise<void>;
}

const AuthContext = createContext<AuthContextValue | null>(null);

/**
 * Провайдер аутентификации web-канала (тикет 9.2, FR A3).
 *
 * Назначение (бизнес): единый источник правды "вошёл ли пользователь" для
 * роутинга (`RequireAuth`/`RequireGuest` в `App.tsx`) и экранов (LoginPage,
 * RegisterPage, будущая кнопка "Выйти" в Settings, тикет 9.6).
 *
 * Как устроено (тех): `isAuthenticated` инициализируется из наличия
 * access-токена в `localStorage` (`src/lib/tokenStore.ts`) при монтировании —
 * переживает перезагрузку вкладки. Токены read/write делегированы
 * `tokenStore` (не React-модуль, его же читает мидларь HTTP-клиента,
 * `src/api/client.ts`, для авто-Authorization и авто-refresh).
 *
 * Синхронизация с мидларью: если авто-refresh по 401 не удался (refresh
 * истёк/отозван/сеть недоступна), `client.ts` вызывает `clearTokens()`
 * напрямую (без доступа к этому контексту) и шлёт событие
 * `agentify:auth:cleared`. Подписка на него (`onTokensCleared`) здесь —
 * единственный способ синхронно перевести React-состояние в
 * `isAuthenticated = false`, не полагаясь на то, что следующий рендер
 * когда-нибудь перечитает `localStorage`.
 */
export function AuthProvider({
  children,
}: {
  children: ReactNode;
}): JSX.Element {
  const [isAuthenticated, setIsAuthenticated] = useState<boolean>(() =>
    Boolean(getAccessToken()),
  );

  useEffect(() => onTokensCleared(() => setIsAuthenticated(false)), []);

  const login = useCallback((tokens: TokenPair) => {
    setTokens(tokens);
    setIsAuthenticated(true);
  }, []);

  const logout = useCallback(async () => {
    const refreshToken = getRefreshToken();
    if (refreshToken) {
      try {
        // Best-effort: `POST /auth/logout` всегда отвечает 204 независимо от
        // валидности токена (идемпотентно, см. orchestrator/internal/api/auth.go),
        // но сетевая ошибка клиента не должна мешать локальному выходу.
        await apiClient.POST("/auth/logout", {
          body: { refresh_token: refreshToken },
        });
      } catch {
        // Игнорируем — локальный logout ниже выполняется в любом случае.
      }
    }
    clearTokens();
    setIsAuthenticated(false);
  }, []);

  const value = useMemo<AuthContextValue>(
    () => ({ isAuthenticated, login, logout }),
    [isAuthenticated, login, logout],
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

/** Доступ к состоянию аутентификации; должен использоваться внутри `AuthProvider`. */
export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext);
  if (!ctx) {
    throw new Error("useAuth() должен вызываться внутри <AuthProvider>");
  }
  return ctx;
}
