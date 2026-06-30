# web/ — Web PWA оркестратора

Назначение (бизнес): веб-канал пользователя (PWA) к оркестратору —
постановка задач, диалог с агентом, история (FR группы D/E/F/H).

Статус: в тикете **0.2** здесь присутствует **только генерация TS-типов** из
контракта `api/openapi.yaml`. Полноценное Vite + React-приложение — тикет **9.1**.

## Генерация типов

Типы API не пишутся руками (AGENTS.md §3) — они генерятся из контракта:

```bash
npm --prefix web ci          # установить devDependencies (openapi-typescript)
npm --prefix web run gen:api # ../api/openapi.yaml -> src/api/schema.ts
# либо из корня:
make generate                # включает этот шаг
```

`src/api/schema.ts` — сгенерированный файл (заголовок «auto-generated …, do not
make direct changes»); правится контракт, а не он. Версия `openapi-typescript`
зафиксирована точно в `package.json`.
