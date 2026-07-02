# web/ — Web PWA оркестратора

Назначение (бизнес): веб-канал пользователя (PWA) к оркестратору —
постановка задач, диалог с агентом, история (FR группы D/E/F/H).

Статус: с тикета **9.1** здесь полноценное Vite + React 18 + TypeScript
SPA/PWA-приложение; с тикета **9.6** все экраны каркаса (`/login`,
`/register`, `/integrations`, `/tasks`, `/settings`) реализованы по-настоящему
(не заглушки); с тикета **9.8** — сквозной Playwright E2E поверх реально
поднятого стека (см. `e2e/README.md`). Стек зафиксирован в
`docs/01_tech_stack_and_architecture.md` (раздел «Web (React PWA)»).

## Структура

| Путь | Роль |
|---|---|
| `src/App.tsx`, `src/components/Layout.tsx` | Роутинг (react-router-dom) и оболочка (шапка+навигация) |
| `src/pages/*` | Экраны: `/login`+`/register` (9.2), `/integrations` (9.3), `/tasks` (9.4, домашний), `/settings` (9.6 — токен регистрации для админа + привязка Telegram + выход) |
| `src/main.tsx` | Точка входа: `QueryClientProvider` (TanStack Query) + `BrowserRouter` |
| `src/api/schema.ts` | Сгенерированные типы контракта (см. «Генерация типов» ниже) — **не редактировать руками** |
| `src/api/client.ts` | Типобезопасный REST-клиент (`openapi-fetch` + `paths` из `schema.ts`), базовый путь по умолчанию `/api` (относительный, тот же origin — CORS не нужен); опционально — `VITE_API_BASE_URL` (dev) |
| `src/hooks/useWebSocket.ts` | Переиспользуемая WS-инфраструктура (реконнект + бэкофф); ещё не подключена ни к одному экрану — эндпоинт появится в тикете 7.2, использование в UI — тикет 9.5 |
| `src/pwaManifest.ts` | Манифест installable PWA, используется и в `vite.config.ts` (`VitePWA`), и в юнит-тесте валидности |
| `src/components/ui/*` | shadcn/ui компоненты (сейчас — `Button`) |
| `tailwind.config.ts`, `postcss.config.js`, `components.json` | Tailwind CSS v3 + shadcn/ui конфигурация |
| `e2e/*.spec.ts`, `e2e/support/*` | Сквозной E2E (тикет 9.8, критерий выхода MVP) — Playwright поверх реально поднятого `make run-local`, свой `tsconfig.json`/раннер, НЕ участвует в `tsc -b`/Vitest приложения (см. `e2e/README.md`) |

## Команды

```bash
npm --prefix web ci           # установить зависимости
npm --prefix web run dev      # dev-сервер Vite
npm --prefix web run build    # tsc -b (typecheck всего приложения) + vite build -> dist/
npm --prefix web run test     # vitest run (без watch)
npm --prefix web run gen:api  # ../api/openapi.yaml -> src/api/schema.ts
npm --prefix web run e2e      # Playwright E2E — требует уже поднятого make run-local, см. e2e/README.md
# либо из корня:
make generate                 # включает gen:api вместе с Go-кодогенерацией
make e2e                      # bootstrap токена регистрации + прогон E2E (см. e2e/README.md)
```

`dist/` (Vite output, по умолчанию) — то, что копирует `web/Dockerfile` в
образ Caddy (`/srv`), см. `web/Caddyfile`.

## Конфигурация (env)

Переменные окружения читаются Vite на этапе сборки (`import.meta.env.*`,
`src/vite-env.d.ts`). Обе опциональны — без них экраны работают, просто с
менее удобным дефолтом.

| Переменная | Назначение |
|---|---|
| `VITE_API_BASE_URL` | Базовый URL REST API оркестратора для `src/api/client.ts`. По умолчанию относительный `/api` (прод, за Caddy, тот же origin). Нужен только при раздельном `vite dev` без прокси. |
| `VITE_TELEGRAM_BOT_USERNAME` | Username Telegram-бота (без `@`) для кликабельной ссылки `t.me/<bot>?start=<code>` на экране «Настройки» (тикет 9.6, `SettingsPage.tsx`). Без неё секция привязки Telegram всё равно показывает код и инструкцию `/start <code>` — просто без готовой ссылки. |

## Генерация типов API

Типы API не пишутся руками (AGENTS.md §3) — они генерятся из контракта
`api/openapi.yaml` через `openapi-typescript` (версия зафиксирована точно в
`package.json`). `src/api/schema.ts` — сгенерированный файл (заголовок
«auto-generated …, do not make direct changes»); правится контракт
(`api/openapi.yaml`), а не он. CI (`generate-check`) проверяет, что схема
остаётся синхронна с контрактом.
