# syntax=docker/dockerfile:1
#
# Dockerfile Telegram-бота (тикет 0.3).
#
# Назначение (бизнес): собрать статический бинарь тонкого адаптера канала
# Telegram (FR D1, D4, G2) и упаковать в минимальный безопасный образ для
# локального docker compose (0.3) и деплоя через GHCR (0.5).
#
# Как устроено (тех): multi-stage, идентично оркестратору. Стадия build на
# golang-образе компилирует статический бинарь с BuildKit cache mounts; рантайм —
# distroless static nonroot (без шелла/менеджера пакетов, непривилегированный
# пользователь).

# --- build stage ---
FROM golang:1.24 AS build
WORKDIR /src

# Опциональный CA для сборки за MITM-прокси (см. orchestrator/Dockerfile);
# без секрета proxy_ca — no-op.
RUN --mount=type=secret,id=proxy_ca,target=/usr/local/share/ca-certificates/proxy-ca.crt \
    if [ -s /usr/local/share/ca-certificates/proxy-ca.crt ]; then update-ca-certificates; fi

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
        -o /out/bot ./bot

# --- runtime stage ---
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/bot /usr/local/bin/bot
USER nonroot:nonroot
# /healthz слушается на BOT_HEALTH_ADDR (по умолчанию :8080).
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/bot"]
