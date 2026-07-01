package claude

import "encoding/json"

// wire.go — JSON-структуры запроса/ответа Anthropic Messages API
// (POST /v1/messages, тикет 9.7), см. годок пакета (provider.go). Форма
// диктуется самим API, а не нами; здесь — минимальное подмножество полей,
// нужное Provider для агентского цикла: (1) отправить задачу первым
// пользовательским сообщением вместе с единственным инструментом bash, (2)
// разобрать content (text/tool_use блоки) и stop_reason ответа, (3) собрать
// следующий запрос с assistant-сообщением (эхо полученного content) и
// user-сообщением с tool_result блоками.

// contentBlock — один блок поля content сообщения Messages API. Поля разных
// типов блока (text/tool_use/tool_result) намеренно объединены в одну
// структуру: Provider и маршалит, и демаршалит этот формат сам, отдельный
// тип на каждый вариант блока добавил бы объём без пользы (аналог
// canUseToolRequest/permissionResult в claudecode/wire.go, где тоже
// используется одна структура на весь протокол).
type contentBlock struct {
	Type string `json:"type"`

	// Text — заполнено для Type=="text" (ответ модели без вызова
	// инструмента — сигнал завершения задачи, см. Provider.Run).
	Text string `json:"text,omitempty"`

	// ID/Name/Input — заполнены для Type=="tool_use" (запрос модели на
	// вызов инструмента bash): ID — tool_use_id, эхом возвращаемый в
	// tool_result; Name — имя инструмента ("bash" в этом Provider); Input —
	// исходный JSON аргументов инструмента (в нашем случае {"command":"..."}).
	ID    string          `json:"id,omitempty"`
	Name  string          `json:"name,omitempty"`
	Input json.RawMessage `json:"input,omitempty"`

	// ToolUseID/Content/IsError — заполнены для Type=="tool_result"
	// (наш ответ модели на её tool_use): ToolUseID — id исходного tool_use
	// блока; Content — текст результата (объединённые stdout+stderr команды
	// либо сообщение об отказе); IsError — true при ошибке
	// выполнения/отклонении пользователем.
	ToolUseID string `json:"tool_use_id,omitempty"`
	Content   string `json:"content,omitempty"`
	IsError   bool   `json:"is_error,omitempty"`
}

// apiMessage — один элемент поля messages запроса Messages API.
type apiMessage struct {
	Role    string         `json:"role"`
	Content []contentBlock `json:"content"`
}

// toolDef — описание одного инструмента в поле tools запроса Messages API.
type toolDef struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"input_schema"`
}

// messagesRequest — тело POST /v1/messages.
type messagesRequest struct {
	Model     string       `json:"model"`
	MaxTokens int          `json:"max_tokens"`
	System    string       `json:"system,omitempty"`
	Messages  []apiMessage `json:"messages"`
	Tools     []toolDef    `json:"tools,omitempty"`
}

// messagesResponse — минимальное подмножество полей ответа Messages API,
// нужное Provider (остальные поля — usage, id верхнего уровня и т.п. — не
// моделируются, они не нужны агентскому циклу тикета 9.7).
type messagesResponse struct {
	Content    []contentBlock `json:"content"`
	StopReason string         `json:"stop_reason"`
}

// apiErrorResponse — тело ответа Messages API при ненулевом HTTP-статусе
// (см. https://docs.anthropic.com/ — формат {"type":"error","error":{"type":...,"message":...}}).
type apiErrorResponse struct {
	Error struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	} `json:"error"`
}

// bashToolInputSchema — JSON Schema аргументов инструмента bash: единственное
// поле command (произвольная shell-команда, исполняется через `sh -c` в
// рабочей директории задачи, см. Provider.executeCommand).
var bashToolInputSchema = json.RawMessage(`{"type":"object","properties":{"command":{"type":"string","description":"Shell-команда для выполнения через sh -c в рабочей директории задачи."}},"required":["command"]}`)

// bashTool — описание единственного инструмента, который Provider объявляет
// модели (тикет 9.7: "второй провайдер в агенте").
func bashTool() toolDef {
	return toolDef{
		Name:        "bash",
		Description: "Выполняет shell-команду (sh -c) в рабочей директории задачи и возвращает её объединённый stdout и stderr в виде текста.",
		InputSchema: bashToolInputSchema,
	}
}

// systemPrompt — короткая системная инструкция модели (тикет 9.7): модель —
// автономный агент-исполнитель, доводящий задачу до конца через bash; ответ
// без вызова инструмента — сигнал завершения (см. Provider.Run).
const systemPrompt = "Ты — автономный агент-исполнитель задач. У тебя есть один инструмент, bash, " +
	"позволяющий выполнять произвольные shell-команды в рабочей директории задачи. " +
	"Используй bash столько раз, сколько нужно, чтобы довести задачу до полного завершения. " +
	"Когда задача полностью выполнена, ответь обычным текстом БЕЗ вызова инструмента — " +
	"это единственный сигнал того, что задача завершена."

// userTextMessage — первое сообщение диалога: постановка задачи как
// пользовательский текст.
func userTextMessage(taskText string) apiMessage {
	return apiMessage{
		Role:    "user",
		Content: []contentBlock{{Type: "text", Text: taskText}},
	}
}

// toolResultBlock строит блок content типа tool_result — ответ на конкретный
// tool_use по его id.
func toolResultBlock(toolUseID, content string, isError bool) contentBlock {
	return contentBlock{
		Type:      "tool_result",
		ToolUseID: toolUseID,
		Content:   content,
		IsError:   isError,
	}
}

// extractCommand достаёт поле "command" из input вызова инструмента bash
// (см. годок одноимённой функции в claudecode/provider.go — тот же приём:
// allowlist/approval должны показать пользователю ЧТО именно просят
// разрешить, даже если структура input окажется неожиданной).
func extractCommand(input json.RawMessage) string {
	var withCommand struct {
		Command string `json:"command"`
	}
	if err := json.Unmarshal(input, &withCommand); err == nil && withCommand.Command != "" {
		return withCommand.Command
	}
	return string(input)
}
