# syntax=docker/dockerfile:1
#
# Dockerfile web PWA (тикеты 0.3, 9.1).
#
# Назначение (бизнес): собрать статику web-канала (FR D1) и упаковать её в образ,
# который раздаёт Caddy. С тикета 9.1 стадия build — реальная Vite-сборка
# (React 18 + TS + PWA), до этого (0.3) здесь была статическая заглушка
# index.html, нужная лишь для проверки сквозной раскладки compose + Caddy
# (приёмка 0.3 «/healthz всех сервисов отвечает»).
#
# Как устроено (тех): multi-stage. Стадия build — `npm ci && npm run build`
# (tsc -b + vite build) в `node:22-alpine`; `npm run build` кладёт статику в
# `/app/dist` (дефолтный output Vite: index.html, manifest.webmanifest,
# service worker, ассеты — см. vite.config.ts и src/pwaManifest.ts).
# Схема API (`src/api/schema.ts`) уже сгенерирована и закоммичена (актуальность
# проверяет CI-джоба `web`/`generate-check`, а не эта сборка) — пересобирать
# её здесь не нужно. Рантайм — caddy:2-alpine, который раздаёт /srv как
# файловый сервер и отвечает 200 на /healthz (см. web/Caddyfile); рантайм-
# контракт не менялся.

# --- build stage: реальная Vite-сборка (тикет 9.1) ---
FROM node:22-alpine AS build
WORKDIR /app
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/ ./
RUN npm run build

# --- runtime stage: статику раздаёт Caddy, плюс собственный /healthz ---
FROM caddy:2-alpine
COPY --from=build /app/dist /srv
COPY web/Caddyfile /etc/caddy/Caddyfile
EXPOSE 8080
