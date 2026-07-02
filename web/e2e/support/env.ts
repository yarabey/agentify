// web/e2e/support/env.ts — конфигурация окружения E2E-стенда (тикет 9.8,
// критерий выхода MVP).
//
// Назначение (бизнес): сценарии 9.8 гоняются не против замоканного бэкенда,
// а против РЕАЛЬНОГО стека, поднятого `make run-local`/`make e2e`
// (docker-compose: Postgres+Redpanda+orchestrator+bot+web+caddy, см.
// deploy/docker-compose.yml) — единственная точка входа снаружи это
// фронтовый Caddy (deploy/Caddyfile), поэтому все адреса ниже строятся от
// одного base URL.
//
// Как устроено (тех): значения по умолчанию совпадают с тем, что использует
// `deploy/scripts/e2e-bootstrap.sh` (тот же скрипт заводит
// администратора/токен регистрации ПЕРЕД прогоном Playwright, см.
// `make e2e` в Makefile) — если оба места разойдутся, тесты просто не
// смогут зарегистрироваться (403 на POST /auth/register), это будет сразу
// заметно, а не тихо сломается.
/** База фронтового Caddy (см. deploy/README.md «Порты и URL»). */
export function baseUrl(): string {
  return process.env.E2E_BASE_URL ?? "http://127.0.0.1:8080";
}

/**
 * WS-адрес `/machine/ws` оркестратора для FakeMachine (docs/protocol.md §1,
 * §4) — тот же origin, что и baseUrl(), но со схемой ws(s): и префиксом
 * `/api`, который Caddy снимает на пути к оркестратору (deploy/Caddyfile,
 * `handle_path /api/*`).
 */
export function machineWsUrl(): string {
  const url = new URL("/api/machine/ws", baseUrl());
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  return url.toString();
}

/**
 * Токен регистрации, которым сценарии заводят СВЕЖИХ пользователей через
 * реальный экран `/register` (тикет 9.2). Значение по умолчанию — то же,
 * что `deploy/scripts/e2e-bootstrap.sh` кладёт в
 * `ORCH_INITIAL_REGISTRATION_TOKEN` при идемпотентном bootstrap (тикет 1.7)
 * локального/CI-стенда — НЕ прод-секрет (см. AGENTS.md §8): существует
 * только в эфемерном docker-compose стенде, поднятом на время E2E.
 */
export function registrationToken(): string {
  return process.env.E2E_REGISTRATION_TOKEN ?? "e2e-local-registration-token";
}
