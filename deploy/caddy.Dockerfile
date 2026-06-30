# syntax=docker/dockerfile:1
#
# deploy/caddy.Dockerfile — фронтовый Caddy с запечённой конфигурацией (0.3).
#
# Назначение: собрать образ reverse-proxy/раздачи статики из официального
# caddy:2-alpine + наш deploy/Caddyfile. Печём конфиг в образ (а не bind-mount),
# чтобы compose работал из любой рабочей директории и образ был самодостаточен
# для деплоя (0.5). Контекст сборки — корень монорепо (см. docker-compose.yml).
FROM caddy:2-alpine
COPY deploy/Caddyfile /etc/caddy/Caddyfile
EXPOSE 8080
