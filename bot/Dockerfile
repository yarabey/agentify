# Dockerfile Telegram-бота (заглушка тикета 0.1).
#
# Назначение: собрать статический бинарь бота и упаковать в минимальный образ
# для docker compose (тикет 0.3) и деплоя через GHCR (тикет 0.5).
#
# syntax=docker/dockerfile:1

# --- build stage ---
FROM golang:1.24 AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download || true
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/bot ./bot

# --- runtime stage ---
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/bot /usr/local/bin/bot
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/bot"]
