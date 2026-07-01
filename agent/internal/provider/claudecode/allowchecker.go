package claudecode

// AllowChecker решает, можно ли выполнить вызов инструмента CLI (tool_name +
// его command/input) без согласования пользователем — allowlist команд
// (docs/MANUAL_STEPS.md, тикет 6.3; приёмка тикета 4.5: "команда из
// allowlist выполняется"). Тикет 4.5 задаёт только точку расширения:
// настоящая конфигурируемая логика (персистентные паттерны вроде
// "Bash(git *)", хранимые и редактируемые владельцем интеграции) — предмет
// отдельного тикета 6.3 и подключается сюда через New без изменения
// внутренностей Provider.
type AllowChecker interface {
	// Allowed возвращает true, если вызов toolName(command) можно выполнить
	// сразу, без command_approval_request (FR C3, FR F3).
	Allowed(toolName, command string) bool
}

// EmptyAllowChecker — allowlist "по умолчанию пуст" (docs/MANUAL_STEPS.md,
// строка 33: "Дефолтный allowlist команд... Можно начать с пустого"): любой
// вызов инструмента уходит на согласование. Это безопасная нулевая
// реализация AllowChecker, которую использует Provider, пока тикет 6.3 не
// подключит настоящий конфигурируемый allowlist.
type EmptyAllowChecker struct{}

// Allowed всегда возвращает false (см. godoc EmptyAllowChecker).
func (EmptyAllowChecker) Allowed(string, string) bool {
	return false
}
