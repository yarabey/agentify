# web/ — Web PWA оркестратора

Назначение (бизнес): веб-канал пользователя (PWA) к оркестратору —
постановка задач, диалог с агентом, история (FR группы D/E/F/H).

Статус: с тикета **9.1** здесь полноценное Vite + React 18 + TypeScript
SPA/PWA-приложение (каркас — маршрутизация, экраны пока заглушки; реальная
реализация экранов — тикеты 9.2-9.6). Стек зафиксирован в
`docs/01_tech_stack_and_architecture.md` (раздел «Web (React PWA)»).

## Структура

| Путь | Роль |
|---|---|
| `src/App.tsx`, `src/components/Layout.tsx` | Роутинг (react-router-dom) и оболочка (шапка+навигация) |
| `src/pages/*` | Экраны-заглушки: `/login` (9.2), `/integrations` (9.3), `/tasks` (9.4, домашний), `/settings` (9.6) |
| `src/main.tsx` | Точка входа: `QueryClientProvider` (TanStack Query) + `BrowserRouter` |
| `src/api/schema.ts` | Сгенерированные типы контракта (см. «Генерация типов» ниже) — **не редактировать руками** |
| `src/api/client.ts` | Типобезопасный REST-клиент (`openapi-fetch` + `paths` из `schema.ts`), базовый путь по умолчанию `/api` (относительный, тот же origin — CORS не нужен) |
| `src/hooks/useWebSocket.ts` | Переиспользуемая WS-инфраструктура (реконнект + бэкофф); ещё не подключена ни к одному экрану — эндпоинт появится в тикете 7.2, использование в UI — тикет 9.5 |
| `src/pwaManifest.ts` | Манифест installable PWA, используется и в `vite.config.ts` (`VitePWA`), и в юнит-тесте валидности |
| `src/components/ui/*` | shadcn/ui компоненты (сейчас — `Button`) |
| `tailwind.config.ts`, `postcss.config.js`, `components.json` | Tailwind CSS v3 + shadcn/ui конфигурация |

## Команды

```bash
npm --prefix web ci           # установить зависимости
npm --prefix web run dev      # dev-сервер Vite
npm --prefix web run build    # tsc -b (typecheck всего приложения) + vite build -> dist/
npm --prefix web run test     # vitest run (без watch)
npm --prefix web run gen:api  # ../api/openapi.yaml -> src/api/schema.ts
# либо из корня:
make generate                 # включает gen:api вместе с Go-кодогенерацией
```

`dist/` (Vite output, по умолчанию) — то, что копирует `web/Dockerfile` в
образ Caddy (`/srv`), см. `web/Caddyfile`.

## Генерация типов API

Типы API не пишутся руками (AGENTS.md §3) — они генерятся из контракта
`api/openapi.yaml` через `openapi-typescript` (версия зафиксирована точно в
`package.json`). `src/api/schema.ts` — сгенерированный файл (заголовок
«auto-generated …, do not make direct changes»); правится контракт
(`api/openapi.yaml`), а не он. CI (`generate-check`) проверяет, что схема
остаётся синхронна с контрактом.
