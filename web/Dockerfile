# Dockerfile web PWA (ЗАГЛУШКА тикета 0.1).
#
# Назначение: multi-stage сборка статики React/Vite, которую раздаёт Caddy
# (см. 01_tech_stack §3 «Web», §5). Реальная сборка web появляется в EPIC web;
# здесь зафиксирован только каркас образа, чтобы compose-файл тикета 0.3 мог на
# него ссылаться.
#
# syntax=docker/dockerfile:1

# --- build stage (заглушка) ---
FROM node:22 AS build
WORKDIR /app
# TODO(0.x): COPY package.json и реальная сборка Vite — вне объёма тикета 0.1.
RUN mkdir -p /app/dist && echo "web placeholder (ticket 0.1)" > /app/dist/index.html

# --- runtime stage: статика раздаётся Caddy в compose ---
FROM caddy:2-alpine
COPY --from=build /app/dist /srv
