package claudecode

import (
	"fmt"
	"path"
	"regexp"
)

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

// patternRE — формат одного паттерна allowlist: "ToolName(glob)", где glob —
// шаблон над command в синтаксисе path.Match (*, ?, [...]).
var patternRE = regexp.MustCompile(`^([A-Za-z0-9_]+)\((.*)\)$`)

// allowPattern — один разобранный и провалидированный паттерн allowlist.
type allowPattern struct {
	tool string
	glob string
}

// PatternAllowChecker — конфигурируемый allowlist на персистентных паттернах
// вида "Bash(git *)" (тикет 6.3, FR F3, docs/User_stories_Gherkin.md §5:
// "Команда из allowlist выполняется без вопроса"). Разрешает вызов
// toolName(command), только если ХОТЯ БЫ ОДИН паттерн одновременно: (1)
// точно совпадает по toolName и (2) совпадает по command как glob-шаблон
// (path.Match). Пустой список паттернов ведёт себя идентично
// EmptyAllowChecker (декларативно санкционировано docs/MANUAL_STEPS.md,
// строка 33 — "дефолтный allowlist... можно начать с пустого").
type PatternAllowChecker struct {
	patterns []allowPattern
}

// NewPatternAllowChecker разбирает и валидирует список строк-паттернов.
// Возвращает ошибку, если ЛЮБАЯ строка не соответствует формату
// "ToolName(glob)" (patternRE) или содержит невалидный glob-шаблон
// (path.ErrBadPattern) — конфигурация allowlist фейлится целиком и сразу
// при старте агента, а не молча деградирует до частично работающего
// набора правил.
func NewPatternAllowChecker(rawPatterns []string) (*PatternAllowChecker, error) {
	patterns := make([]allowPattern, 0, len(rawPatterns))
	for _, raw := range rawPatterns {
		m := patternRE.FindStringSubmatch(raw)
		if m == nil {
			return nil, fmt.Errorf("allowlist: паттерн %q не соответствует формату ToolName(glob)", raw)
		}
		tool, glob := m[1], m[2]
		if _, err := path.Match(glob, ""); err != nil {
			return nil, fmt.Errorf("allowlist: невалидный glob-шаблон в паттерне %q: %w", raw, err)
		}
		patterns = append(patterns, allowPattern{tool: tool, glob: glob})
	}
	return &PatternAllowChecker{patterns: patterns}, nil
}

// Allowed — см. godoc AllowChecker.Allowed и PatternAllowChecker.
func (c *PatternAllowChecker) Allowed(toolName, command string) bool {
	for _, p := range c.patterns {
		if p.tool != toolName {
			continue
		}
		if ok, _ := path.Match(p.glob, command); ok {
			return true
		}
	}
	return false
}
