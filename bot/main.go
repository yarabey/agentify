// Package main — точка входа Telegram-бота.
//
// Назначение (бизнес): бот — тонкий адаптер канала Telegram (FR D1, D4, G2):
// webhook на входящие и доставка уведомлений из топика notifications.telegram.
// Вся бизнес-логика остаётся в оркестраторе (принцип «единый API»). В тикете
// 0.6 здесь реализован лишь общий операционный каркас (конфиг из env, slog,
// /healthz, graceful shutdown); webhook и consumer добавляются в EPIC 10.
//
// Как устроено (тех): main — тонкий: грузит конфиг под префиксом BOT_ через
// общий пакет platform, поднимает каркас сервиса (slog + chi /healthz) и
// блокируется до SIGTERM/SIGINT, после чего гасится gracefully.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/yarabey/agentify/internal/platform"
)

// serviceName — каноническое имя сервиса в логах и в теле /healthz.
const serviceName = "bot"

// envPrefix — префикс env-переменных бота, чтобы три сервиса не конфликтовали
// по именам в общем окружении (docker compose).
const envPrefix = "BOT_"

// config — конфиг бота: общий операционный базис плюс место для специфичных
// полей (токен бота, webhook-URL появятся в EPIC 10 под тем же префиксом BOT_).
type config struct {
	platform.Config
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "bot: фатальная ошибка:", err)
		os.Exit(1)
	}
}

// run загружает конфиг, собирает каркас сервиса и блокируется до остановки.
func run() error {
	var cfg config
	if err := platform.LoadConfig(&cfg, envPrefix); err != nil {
		return err
	}

	svc, err := platform.NewService(serviceName, cfg.Config)
	if err != nil {
		return err
	}

	return svc.Run(context.Background())
}
