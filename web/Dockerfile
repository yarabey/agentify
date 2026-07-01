# syntax=docker/dockerfile:1
#
# Dockerfile web PWA (тикет 0.3).
#
# Назначение (бизнес): собрать статику web-канала (FR D1) и упаковать её в образ,
# который раздаёт Caddy. В тикете 0.3 это намеренно ЗАГЛУШКА: статический
# index.html вместо полноценной React/Vite-сборки. Полноценный Vite-билд —
# тикет 9.1 (каркас PWA); здесь нужен лишь рабочий статический ответ и /healthz,
# чтобы проверить сквозную раскладку compose + Caddy (приёмка 0.3
# «/healthz всех сервисов отвечает»).
#
# Как устроено (тех): multi-stage. Стадия build генерирует /app/dist/index.html
# (заглушку). Рантайм — caddy:2-alpine, который раздаёт /srv как файловый сервер
# и отвечает 200 на /healthz (см. web/Caddyfile). Тикет 9.1 заменит стадию build
# на реальный `npm ci && npm run build`, не трогая рантайм-контракт.

# --- build stage (заглушка статики, тикет 0.3; реальный Vite — 9.1) ---
FROM alpine:3.20 AS build
WORKDIR /app
# Минимальная, но осмысленная заглушка: показывает, что web-канал поднят, и даёт
# человекочитаемую страницу при заходе через Caddy на корень.
RUN mkdir -p /app/dist && cat > /app/dist/index.html <<'HTML'
<!doctype html>
<html lang="ru">
  <head><meta charset="utf-8"><title>agentify — web (заглушка 0.3)</title></head>
  <body>
    <h1>agentify web</h1>
    <p>Статическая заглушка тикета 0.3. Полноценный PWA — тикет 9.1.</p>
  </body>
</html>
HTML

# --- runtime stage: статику раздаёт Caddy, плюс собственный /healthz ---
FROM caddy:2-alpine
COPY --from=build /app/dist /srv
COPY web/Caddyfile /etc/caddy/Caddyfile
EXPOSE 8080
