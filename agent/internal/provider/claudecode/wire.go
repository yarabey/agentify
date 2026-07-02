package claudecode

import "encoding/json"

// wire.go — JSON-структуры двунаправленного NDJSON-протокола `claude`
// (`--input-format stream-json --output-format stream-json`, тикет 4.5).
// Форма строк задаётся самим CLI, а не нами; здесь — только то подмножество
// полей, которое нужно Provider, чтобы: (1) поставить задачу первой
// строкой stdin, (2) распознать запрос на разрешение инструмента
// (control_request/can_use_tool) среди прочих строк stdout, (3) ответить на
// него control_response. Прочие типы строк (system/assistant/result/
// stream_event/...) Provider намеренно не моделирует (см. godoc пакета).

// rawLine — минимальный разбор одной строки stdout: только type, достаточно
// чтобы решить, распознаём мы её или молча пропускаем.
type rawLine struct {
	Type string `json:"type"`
}

// userMessageLine — первая строка, которую Provider пишет в stdin CLI:
// постановка задачи как единственное пользовательское сообщение диалога
// (формат подтверждён живым smoke-тестом против реального бинаря, см. AGENTS
// пакета claudecode/CLAUDE_CODE_FLAGS в godoc пакета).
type userMessageLine struct {
	Type    string      `json:"type"`
	Message userMessage `json:"message"`
}

// userMessage — вложенное поле message строки type=="user".
type userMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// newUserMessageLine собирает первую строку stdin с текстом задачи.
func newUserMessageLine(taskText string) userMessageLine {
	return userMessageLine{
		Type: "user",
		Message: userMessage{
			Role:    "user",
			Content: taskText,
		},
	}
}

// controlRequestLine — входящая строка type=="control_request" от CLI.
// Provider обрабатывает только Request.Subtype=="can_use_tool" (запрос
// разрешения на инструмент); прочие subtype (если появятся) пропускаются
// так же, как и любые прочие Type.
type controlRequestLine struct {
	Type      string            `json:"type"`
	RequestID string            `json:"request_id"`
	Request   canUseToolRequest `json:"request"`
}

// canUseToolRequest — поле request строки control_request с subtype
// can_use_tool: имя инструмента и его исходный input (эхом возвращается в
// updatedInput при allow, см. Provider.Approve).
type canUseToolRequest struct {
	Subtype  string          `json:"subtype"`
	ToolName string          `json:"tool_name"`
	Input    json.RawMessage `json:"input"`
}

// controlResponseLine — исходящая строка type=="control_response", которой
// Provider отвечает на control_request: allow немедленно для allowlist-
// команд, allow/deny — по решению Approve (см. Provider.Approve).
type controlResponseLine struct {
	Type     string          `json:"type"`
	Response controlResponse `json:"response"`
}

// controlResponse — поле response строки control_response.
type controlResponse struct {
	Subtype   string           `json:"subtype"`
	RequestID string           `json:"request_id"`
	Response  permissionResult `json:"response"`
}

// permissionResult — вложенное поле response.response: собственно решение
// (allow/deny) и сопутствующие данные.
type permissionResult struct {
	// Behavior — "allow" или "deny".
	Behavior string `json:"behavior"`
	// UpdatedInput — эхо исходного input инструмента при allow (CLI ожидает
	// это поле, даже если Provider ничего в input не меняет).
	UpdatedInput json.RawMessage `json:"updatedInput,omitempty"`
	// Message — человекочитаемая причина при deny.
	Message string `json:"message,omitempty"`
}

// newAllowResponse строит control_response с behavior=="allow", эхом
// возвращая исходный input инструмента (CLI ожидает его в updatedInput).
func newAllowResponse(requestID string, input json.RawMessage) controlResponseLine {
	return controlResponseLine{
		Type: "control_response",
		Response: controlResponse{
			Subtype:   "success",
			RequestID: requestID,
			Response: permissionResult{
				Behavior:     "allow",
				UpdatedInput: input,
			},
		},
	}
}

// newDenyResponse строит control_response с behavior=="deny" и
// человекочитаемым сообщением-причиной.
func newDenyResponse(requestID, message string) controlResponseLine {
	return controlResponseLine{
		Type: "control_response",
		Response: controlResponse{
			Subtype:   "success",
			RequestID: requestID,
			Response: permissionResult{
				Behavior: "deny",
				Message:  message,
			},
		},
	}
}
