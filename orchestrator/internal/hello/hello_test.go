package hello

import "testing"

// TestGreeting проверяет, что приветствие оркестратора содержит имя сервиса.
//
// Trace: тикет 0.1 «Приёмка» — make test реально прогоняет unit-тест и зелёный.
func TestGreeting(t *testing.T) {
	got := Greeting()
	want := "hello from orchestrator"
	if got != want {
		t.Fatalf("Greeting() = %q, want %q", got, want)
	}
}
