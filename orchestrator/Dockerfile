# syntax=docker/dockerfile:1
#
# Dockerfile оркестратора (тикет 0.3).
#
# Назначение (бизнес): собрать статический бинарь ядра системы (FR E*, A*, F*) и
# упаковать в минимальный безопасный образ для локального docker compose (0.3) и
# деплоя через GHCR (0.5). Оркестратор при старте сам применяет goose-миграции к
# Postgres (тикет 0.3), поэтому отдельный образ с миграциями не нужен.
#
# Как устроено (тех): multi-stage. Стадия build на golang-образе компилирует
# статический (CGO_ENABLED=0) бинарь из монорепо; кэш модулей и сборки смонтирован
# через BuildKit cache mounts, чтобы пересборки были быстрыми. Рантайм — distroless
# static nonroot: нет шелла и пакетного менеджера (минимум поверхности атаки),
# процесс идёт под непривилегированным пользователем.

# --- build stage ---
FROM golang:1.24 AS build
WORKDIR /src

# За корпоративным/egress MITM-прокси сборке нужен доверенный CA, иначе
# `go mod download` падает на проверке сертификата proxy.golang.org. CA
# передаётся как опциональный BuildKit-секрет `proxy_ca` и добавляется в
# системный trust store; без секрета (обычная сборка/CI) шаг — no-op.
RUN --mount=type=secret,id=proxy_ca,target=/usr/local/share/ca-certificates/proxy-ca.crt \
    if [ -s /usr/local/share/ca-certificates/proxy-ca.crt ]; then update-ca-certificates; fi

# Сначала только манифесты модулей — слой с зависимостями кэшируется отдельно от
# исходников и не инвалидируется при правке кода.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Остальной контекст монорепо (нужен весь: internal/platform, orchestrator/*).
COPY . .

# Статическая сборка: бинарь поедет в distroless без libc.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
        -o /out/orchestrator ./orchestrator

# --- runtime stage ---
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/orchestrator /usr/local/bin/orchestrator
USER nonroot:nonroot
# /healthz слушается на ORCH_HEALTH_ADDR (по умолчанию :8080).
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/orchestrator"]
