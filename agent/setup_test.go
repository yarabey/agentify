package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// TestRunSetup_EnvHappyPath — приёмка тикета 4.3: «неинтерактивный режим
// через env (для CI)». Все шаги задаются через AGENTIFY_SETUP_*, stdin не
// используется.
func TestRunSetup_EnvHappyPath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "agent.env")

	validUUID := uuid.New().String()
	t.Setenv("AGENTIFY_SETUP_ORCHESTRATOR_URL", "wss://orch.example.com/machine/ws")
	t.Setenv("AGENTIFY_SETUP_UUID", validUUID)
	t.Setenv("AGENTIFY_SETUP_PROVIDERS", "claude,claude-code")
	t.Setenv("AGENTIFY_SETUP_CLAUDE_API_KEY", "sk-test-1")
	t.Setenv("AGENTIFY_SETUP_CLAUDE_CODE_API_KEY", "sk-test-2")
	t.Setenv("AGENTIFY_CONFIG_PATH", path)

	var stdout bytes.Buffer
	if err := runSetup(strings.NewReader(""), &stdout); err != nil {
		t.Fatalf("runSetup() error = %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("os.Stat(%q) error = %v", path, err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("config file mode = %o, want 0600", perm)
	}

	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("os.ReadFile(%q) error = %v", path, err)
	}
	got := string(content)

	for _, want := range []string{
		"AGENT_ORCHESTRATOR_WS_URL=wss://orch.example.com/machine/ws\n",
		"AGENT_INTEGRATION_UUID=" + validUUID + "\n",
		"AGENT_PROVIDERS=claude,claude-code\n",
		"AGENT_CLAUDE_API_KEY=sk-test-1\n",
		"AGENT_CLAUDE_CODE_API_KEY=sk-test-2\n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("config file %q does not contain %q; full content:\n%s", path, want, got)
		}
	}
}

// TestRunSetup_DefaultConfigPath — без AGENTIFY_CONFIG_PATH файл должен
// появиться в $HOME/.agentify/agent.env.
func TestRunSetup_DefaultConfigPath(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	t.Setenv("AGENTIFY_SETUP_ORCHESTRATOR_URL", "wss://orch.example.com/machine/ws")
	t.Setenv("AGENTIFY_SETUP_UUID", uuid.New().String())
	t.Setenv("AGENTIFY_SETUP_PROVIDERS", "claude")
	t.Setenv("AGENTIFY_SETUP_CLAUDE_API_KEY", "sk-test")

	var stdout bytes.Buffer
	if err := runSetup(strings.NewReader(""), &stdout); err != nil {
		t.Fatalf("runSetup() error = %v", err)
	}

	wantPath := filepath.Join(home, ".agentify", "agent.env")
	if _, err := os.Stat(wantPath); err != nil {
		t.Fatalf("expected config file at %q: %v", wantPath, err)
	}
}

// TestRunSetup_SingleProvider — если выбран только "claude", в файле не
// должно быть строки AGENT_CLAUDE_CODE_API_KEY вообще (не пустое значение —
// полное отсутствие строки).
func TestRunSetup_SingleProvider(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "agent.env")

	t.Setenv("AGENTIFY_SETUP_ORCHESTRATOR_URL", "wss://orch.example.com/machine/ws")
	t.Setenv("AGENTIFY_SETUP_UUID", uuid.New().String())
	t.Setenv("AGENTIFY_SETUP_PROVIDERS", "claude")
	t.Setenv("AGENTIFY_SETUP_CLAUDE_API_KEY", "sk-test")
	t.Setenv("AGENTIFY_CONFIG_PATH", path)

	var stdout bytes.Buffer
	if err := runSetup(strings.NewReader(""), &stdout); err != nil {
		t.Fatalf("runSetup() error = %v", err)
	}

	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("os.ReadFile(%q) error = %v", path, err)
	}
	got := string(content)

	if !strings.Contains(got, "AGENT_CLAUDE_API_KEY=") {
		t.Errorf("expected AGENT_CLAUDE_API_KEY in config, got:\n%s", got)
	}
	if strings.Contains(got, "AGENT_CLAUDE_CODE_API_KEY") {
		t.Errorf("did not expect AGENT_CLAUDE_CODE_API_KEY in config, got:\n%s", got)
	}
}

// TestRunSetup_InvalidUUID — невалидный AGENTIFY_SETUP_UUID должен вернуть
// ошибку.
func TestRunSetup_InvalidUUID(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("AGENTIFY_SETUP_ORCHESTRATOR_URL", "wss://orch.example.com/machine/ws")
	t.Setenv("AGENTIFY_SETUP_UUID", "not-a-uuid")
	t.Setenv("AGENTIFY_SETUP_PROVIDERS", "claude")
	t.Setenv("AGENTIFY_SETUP_CLAUDE_API_KEY", "sk-test")
	t.Setenv("AGENTIFY_CONFIG_PATH", filepath.Join(dir, "agent.env"))

	var stdout bytes.Buffer
	if err := runSetup(strings.NewReader(""), &stdout); err == nil {
		t.Fatal("runSetup() expected error for invalid UUID, got nil")
	}
}

// TestRunSetup_InvalidProvider — неизвестный провайдер должен вернуть
// ошибку.
func TestRunSetup_InvalidProvider(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("AGENTIFY_SETUP_ORCHESTRATOR_URL", "wss://orch.example.com/machine/ws")
	t.Setenv("AGENTIFY_SETUP_UUID", uuid.New().String())
	t.Setenv("AGENTIFY_SETUP_PROVIDERS", "gpt4")
	t.Setenv("AGENTIFY_CONFIG_PATH", filepath.Join(dir, "agent.env"))

	var stdout bytes.Buffer
	if err := runSetup(strings.NewReader(""), &stdout); err == nil {
		t.Fatal("runSetup() expected error for unknown provider, got nil")
	}
}

// TestRunSetup_EmptyProviders — пустой список провайдеров — ошибка.
func TestRunSetup_EmptyProviders(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("AGENTIFY_SETUP_ORCHESTRATOR_URL", "wss://orch.example.com/machine/ws")
	t.Setenv("AGENTIFY_SETUP_UUID", uuid.New().String())
	t.Setenv("AGENTIFY_SETUP_PROVIDERS", "")
	t.Setenv("AGENTIFY_CONFIG_PATH", filepath.Join(dir, "agent.env"))

	var stdout bytes.Buffer
	if err := runSetup(strings.NewReader(""), &stdout); err == nil {
		t.Fatal("runSetup() expected error for empty providers, got nil")
	}
}

// TestRunSetup_InteractiveNonTTY — без AGENTIFY_SETUP_* вообще, все ответы
// читаются построчно из stdin (strings.Reader — не *os.File, поэтому это
// заодно проверяет non-TTY fallback для credential-шага вместо
// term.ReadPassword). Проверяем в т.ч. derivation адреса оркестратора:
// добавление схемы wss:// и суффикса /machine/ws.
func TestRunSetup_InteractiveNonTTY(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "agent.env")
	t.Setenv("AGENTIFY_CONFIG_PATH", path)

	validUUID := uuid.New().String()
	stdin := strings.NewReader("orchestrator.example.com\n" + validUUID + "\nclaude\nsk-test\n")

	var stdout bytes.Buffer
	if err := runSetup(stdin, &stdout); err != nil {
		t.Fatalf("runSetup() error = %v", err)
	}

	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("os.ReadFile(%q) error = %v", path, err)
	}
	got := string(content)

	for _, want := range []string{
		"AGENT_ORCHESTRATOR_WS_URL=wss://orchestrator.example.com/machine/ws\n",
		"AGENT_INTEGRATION_UUID=" + validUUID + "\n",
		"AGENT_PROVIDERS=claude\n",
		"AGENT_CLAUDE_API_KEY=sk-test\n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("config file %q does not contain %q; full content:\n%s", path, want, got)
		}
	}
}
