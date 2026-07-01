// Юнит-тесты конфигурируемого allowlist (тикет 6.3, FR F3): формат паттернов
// "ToolName(glob)" (см. годок allowchecker.go), разбор/валидация в
// NewPatternAllowChecker и матчинг в PatternAllowChecker.Allowed.
package claudecode

import (
	"strings"
	"testing"
)

// TestNewPatternAllowChecker_EmptyList_BehavesLikeEmptyAllowChecker — пустой
// список паттернов (nil и []string{}) не разрешает НИЧЕГО, как и
// EmptyAllowChecker (docs/MANUAL_STEPS.md, строка 33: "можно начать с
// пустого").
func TestNewPatternAllowChecker_EmptyList_BehavesLikeEmptyAllowChecker(t *testing.T) {
	for name, patterns := range map[string][]string{
		"nil":   nil,
		"empty": {},
	} {
		t.Run(name, func(t *testing.T) {
			checker, err := NewPatternAllowChecker(patterns)
			if err != nil {
				t.Fatalf("NewPatternAllowChecker(%v) вернул ошибку: %v", patterns, err)
			}
			if checker.Allowed("Bash", "git status") {
				t.Fatal("Allowed вернул true при пустом allowlist")
			}
			if checker.Allowed("", "") {
				t.Fatal("Allowed вернул true при пустом allowlist (пустые аргументы)")
			}
		})
	}
}

// TestPatternAllowChecker_Allowed_MatchesToolAndGlob — паттерн "Bash(git *)":
// совпадает по инструменту и glob-шаблону над command.
func TestPatternAllowChecker_Allowed_MatchesToolAndGlob(t *testing.T) {
	checker, err := NewPatternAllowChecker([]string{"Bash(git *)"})
	if err != nil {
		t.Fatalf("NewPatternAllowChecker: %v", err)
	}

	if !checker.Allowed("Bash", "git status") {
		t.Fatal(`Allowed("Bash", "git status") = false, ожидался true`)
	}
	if checker.Allowed("Bash", "git") {
		t.Fatal(`Allowed("Bash", "git") = true, ожидался false (нет пробела — не подходит под "git *")`)
	}
	if checker.Allowed("Other", "git status") {
		t.Fatal(`Allowed("Other", "git status") = true, ожидался false (другой toolName)`)
	}
}

// TestPatternAllowChecker_Allowed_ExactLiteralPattern — паттерн без wildcard
// ("Bash(ls)") ведёт себя как точное совпадение command.
func TestPatternAllowChecker_Allowed_ExactLiteralPattern(t *testing.T) {
	checker, err := NewPatternAllowChecker([]string{"Bash(ls)"})
	if err != nil {
		t.Fatalf("NewPatternAllowChecker: %v", err)
	}

	if !checker.Allowed("Bash", "ls") {
		t.Fatal(`Allowed("Bash", "ls") = false, ожидался true`)
	}
	if checker.Allowed("Bash", "ls -la") {
		t.Fatal(`Allowed("Bash", "ls -la") = true, ожидался false`)
	}
}

// TestPatternAllowChecker_Allowed_MultiplePatterns_AnyMatch — список из
// нескольких паттернов для разных инструментов: срабатывает подходящий, а не
// только первый в списке.
func TestPatternAllowChecker_Allowed_MultiplePatterns_AnyMatch(t *testing.T) {
	checker, err := NewPatternAllowChecker([]string{"Bash(git *)", "Read(*.go)"})
	if err != nil {
		t.Fatalf("NewPatternAllowChecker: %v", err)
	}

	if !checker.Allowed("Bash", "git status") {
		t.Fatal(`Allowed("Bash", "git status") = false, ожидался true`)
	}
	if !checker.Allowed("Read", "main.go") {
		t.Fatal(`Allowed("Read", "main.go") = false, ожидался true`)
	}
	if checker.Allowed("Read", "main.py") {
		t.Fatal(`Allowed("Read", "main.py") = true, ожидался false`)
	}
	if checker.Allowed("Write", "main.go") {
		t.Fatal(`Allowed("Write", "main.go") = true, ожидался false (нет паттерна для Write)`)
	}
}

// TestNewPatternAllowChecker_MalformedPattern_ReturnsError — строка без
// скобок не соответствует формату "ToolName(glob)".
func TestNewPatternAllowChecker_MalformedPattern_ReturnsError(t *testing.T) {
	_, err := NewPatternAllowChecker([]string{"BashGitStar"})
	if err == nil {
		t.Fatal("NewPatternAllowChecker вернул nil при некорректном формате паттерна")
	}
	if !strings.Contains(err.Error(), "BashGitStar") {
		t.Fatalf("ошибка %q не содержит саму невалидную строку паттерна", err.Error())
	}
}

// TestNewPatternAllowChecker_InvalidGlob_ReturnsError — непарная скобка в
// glob-шаблоне ("[") — невалидный шаблон для path.Match.
func TestNewPatternAllowChecker_InvalidGlob_ReturnsError(t *testing.T) {
	_, err := NewPatternAllowChecker([]string{"Bash([)"})
	if err == nil {
		t.Fatal("NewPatternAllowChecker вернул nil при невалидном glob-шаблоне")
	}
}
