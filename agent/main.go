// Package main — точка входа агента-демона на машине пользователя.
//
// Назначение (бизнес): агент — Go-демон, который ставится на машину
// пользователя одной командой (FR C1, C5), держит исходящий WebSocket к
// оркестратору, локальный durable outbox и запускает провайдеров Claude /
// Claude Code. В compose не входит — распространяется как бинарь через
// GoReleaser. В тикете 0.1 это заглушка каркаса.
//
// Как устроено (тех): main печатает приветствие из внутреннего пакета hello,
// подтверждая собираемость модуля агента.
package main

import (
	"fmt"

	"github.com/yarabey/agentify/agent/internal/hello"
)

func main() {
	fmt.Println(hello.Greeting())
}
