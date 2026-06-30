// Package main — точка входа Telegram-бота.
//
// Назначение (бизнес): бот — тонкий адаптер канала Telegram (FR D1, D4, G2):
// webhook на входящие и доставка уведомлений из топика notifications.telegram.
// Вся бизнес-логика остаётся в оркестраторе (принцип «единый API»). В тикете
// 0.1 это заглушка каркаса; рабочий каркас сервиса — в тикете 0.6.
//
// Как устроено (тех): main печатает приветствие из внутреннего пакета hello,
// подтверждая собираемость модуля бота.
package main

import (
	"fmt"

	"github.com/yarabey/agentify/bot/internal/hello"
)

func main() {
	fmt.Println(hello.Greeting())
}
