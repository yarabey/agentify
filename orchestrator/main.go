// Package main — точка входа сервиса-оркестратора.
//
// Назначение (бизнес): оркестратор — ядро системы (FR E*, A*, F*): REST+WS,
// FSM задач, auth, история, мост к Redpanda. В тикете 0.1 это лишь заглушка
// каркаса монорепо; полноценный запуск (конфиг, slog, /healthz, graceful
// shutdown) появляется в тикете 0.6.
//
// Как устроено (тех): на текущем этапе main печатает приветствие из
// внутреннего пакета hello, подтверждая, что модуль собирается и линкуется.
package main

import (
	"fmt"

	"github.com/yarabey/agentify/orchestrator/internal/hello"
)

func main() {
	fmt.Println(hello.Greeting())
}
