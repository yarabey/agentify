// Command fakeclaude — тестовый двойник CLI `claude` для committed-тестов
// пакета claudecode (тикет 4.5): говорит на том же
// control_request/control_response NDJSON-протоколе, что и реальный
// бинарь, но без сети и без реальных кредов (см. godoc пакета claudecode,
// секция про запрет реальных вызовов в CI). Живёт под testdata/ — стандартный
// каталог, который `go build ./...`/`go vet ./...` пропускают сами
// (https://pkg.go.dev/cmd/go#hdr-Package_lists_and_patterns); provider_test.go
// собирает его явным `go build ./testdata/fakeclaude` в TestMain.
//
// Сценарий вызовов инструментов, которые нужно разыграть, передаётся через
// переменную окружения FAKE_CLAUDE_SCRIPT — JSON-массив
// {request_id, tool_name, command}. Для каждого шага fakeclaude пишет в
// stdout control_request и БЛОКИРУЕТСЯ на чтении следующей строки stdin,
// ожидая control_response с тем же request_id (как и реальный CLI —
// см. ticket 4.5) — именно это позволяет тестам проверить, что Provider
// действительно ждёт решения, а не отвечает раньше времени.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// scriptStep — один элемент FAKE_CLAUDE_SCRIPT.
type scriptStep struct {
	RequestID string `json:"request_id"`
	ToolName  string `json:"tool_name"`
	Command   string `json:"command"`
	// SleepAfterApproveMs — если > 0, fakeclaude ждёт столько миллисекунд
	// ПОСЛЕ получения control_response на этот шаг, ПЕРЕД тем как перейти к
	// следующему шагу (или напечатать финальный result, если шаг последний)
	// — симулирует реально выполняющийся инструмент (тикет 8.5), давая тесту
	// окно, в котором можно вызвать Provider.Close() и убедиться, что
	// подпроцесс НЕ убит немедленно.
	SleepAfterApproveMs int `json:"sleep_after_approve_ms"`
}

// controlRequestOut — исходящая строка control_request (зеркало
// wire.go/controlRequestLine пакета claudecode).
type controlRequestOut struct {
	Type      string     `json:"type"`
	RequestID string     `json:"request_id"`
	Request   requestOut `json:"request"`
}

type requestOut struct {
	Subtype  string          `json:"subtype"`
	ToolName string          `json:"tool_name"`
	Input    json.RawMessage `json:"input"`
}

// controlResponseIn — входящая строка control_response (зеркало
// wire.go/controlResponseLine пакета claudecode).
type controlResponseIn struct {
	Type     string `json:"type"`
	Response struct {
		RequestID string `json:"request_id"`
		Response  struct {
			Behavior string `json:"behavior"`
			Message  string `json:"message"`
		} `json:"response"`
	} `json:"response"`
}

func main() {
	stdin := bufio.NewScanner(os.Stdin)
	stdin.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)
	stdout := bufio.NewWriter(os.Stdout)
	defer stdout.Flush()

	// Первая строка stdin — постановка задачи ({"type":"user",...}); fake
	// проигрывает сценарий из окружения, а не реальный текст задачи, так
	// что содержимое просто потребляется и отбрасывается.
	if !stdin.Scan() {
		return
	}

	var steps []scriptStep
	if raw := os.Getenv("FAKE_CLAUDE_SCRIPT"); raw != "" {
		if err := json.Unmarshal([]byte(raw), &steps); err != nil {
			fmt.Fprintln(os.Stderr, "fakeclaude: невалидный FAKE_CLAUDE_SCRIPT:", err)
			os.Exit(1)
		}
	}

	for _, step := range steps {
		input, err := json.Marshal(map[string]string{"command": step.Command})
		if err != nil {
			fmt.Fprintln(os.Stderr, "fakeclaude: маршалинг input:", err)
			os.Exit(1)
		}

		out := controlRequestOut{
			Type:      "control_request",
			RequestID: step.RequestID,
			Request: requestOut{
				Subtype:  "can_use_tool",
				ToolName: step.ToolName,
				Input:    input,
			},
		}
		data, err := json.Marshal(out)
		if err != nil {
			fmt.Fprintln(os.Stderr, "fakeclaude: маршалинг control_request:", err)
			os.Exit(1)
		}
		if _, err := stdout.Write(data); err != nil {
			return
		}
		if _, err := stdout.WriteString("\n"); err != nil {
			return
		}
		if err := stdout.Flush(); err != nil {
			return
		}

		if !stdin.Scan() {
			// stdin закрыт (родитель остановил процесс) — тихо завершаемся,
			// как и реальный CLI при SIGKILL/закрытии stdin.
			return
		}
		var resp controlResponseIn
		if err := json.Unmarshal(stdin.Bytes(), &resp); err != nil {
			fmt.Fprintln(os.Stderr, "fakeclaude: невалидный control_response:", err)
			os.Exit(1)
		}
		// Поведение (allow/deny) fake только логирует в stderr — тестам
		// достаточно факта, ЧТО Provider записал в stdin (см.
		// provider_test.go, которые читают это напрямую из stdin-пайпа
		// Provider'а, а не из вывода fakeclaude).
		fmt.Fprintf(os.Stderr, "fakeclaude: request_id=%s behavior=%s\n",
			resp.Response.RequestID, resp.Response.Response.Behavior)

		if step.SleepAfterApproveMs > 0 {
			time.Sleep(time.Duration(step.SleepAfterApproveMs) * time.Millisecond)
		}
	}

	fmt.Fprintln(stdout, `{"type":"result","subtype":"success"}`)
}
