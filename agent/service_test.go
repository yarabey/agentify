package main

import (
	"bytes"
	"encoding/xml"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"testing"
)

// TestSystemdUnitContent — точное содержимое systemd unit-файла (Description,
// User=, WorkingDirectory=, EnvironmentFile=, ExecStart= БЕЗ доп. аргументов,
// Restart=on-failure, RestartSec=2, WantedBy=multi-user.target).
func TestSystemdUnitContent(t *testing.T) {
	got := systemdUnitContent("/usr/local/bin/agentify-agent", "/home/alice/.agentify/agent.env", "/home/alice/.agentify", "alice")

	want := `[Unit]
Description=agentify-agent
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=alice
WorkingDirectory=/home/alice/.agentify
EnvironmentFile=/home/alice/.agentify/agent.env
ExecStart=/usr/local/bin/agentify-agent
Restart=on-failure
RestartSec=2

[Install]
WantedBy=multi-user.target
`
	if got != want {
		t.Fatalf("systemdUnitContent() =\n%s\nwant:\n%s", got, want)
	}
}

// TestParseEnvFile — парсинг KEY=VALUE\n, включая пустые строки и значения с
// "=" внутри (split только по первому "=").
func TestParseEnvFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "agent.env")
	content := "AGENT_ORCHESTRATOR_WS_URL=wss://orch.example.com/machine/ws\n" +
		"\n" +
		"AGENT_CLAUDE_API_KEY=sk-a=b\n" +
		"AGENT_PROVIDERS=claude\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}

	env, err := parseEnvFile(path)
	if err != nil {
		t.Fatalf("parseEnvFile() error = %v", err)
	}

	want := map[string]string{
		"AGENT_ORCHESTRATOR_WS_URL": "wss://orch.example.com/machine/ws",
		"AGENT_CLAUDE_API_KEY":      "sk-a=b",
		"AGENT_PROVIDERS":           "claude",
	}
	if len(env) != len(want) {
		t.Fatalf("parseEnvFile() = %#v, want %#v", env, want)
	}
	for k, v := range want {
		if env[k] != v {
			t.Errorf("parseEnvFile()[%q] = %q, want %q", k, env[k], v)
		}
	}
}

// TestLaunchdPlistContent_EscapesSpecialChars — значения из конфига (в т.ч.
// потенциальные API-ключи) с XML-спецсимволами должны быть корректно
// экранированы, а результат — валидным XML.
func TestLaunchdPlistContent_EscapesSpecialChars(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "agent.env")
	content := "AGENT_CLAUDE_API_KEY=sk-<test>&value\n" +
		"AGENT_PROVIDERS=claude\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}

	got, err := launchdPlistContent("/usr/local/bin/agentify-agent", path, dir, "alice")
	if err != nil {
		t.Fatalf("launchdPlistContent() error = %v", err)
	}

	if strings.Contains(got, "sk-<test>&value") {
		t.Fatalf("launchdPlistContent() contains unescaped raw value:\n%s", got)
	}
	for _, want := range []string{"&amp;", "&lt;", "&gt;"} {
		if !strings.Contains(got, want) {
			t.Errorf("launchdPlistContent() does not contain escaped %q; full content:\n%s", want, got)
		}
	}

	for _, want := range []string{
		"<key>ProgramArguments</key>",
		"<key>UserName</key>",
		"<key>WorkingDirectory</key>",
		"<key>RunAtLoad</key>",
		"<key>KeepAlive</key>",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("launchdPlistContent() does not contain %q; full content:\n%s", want, got)
		}
	}

	// Well-formedness: декодируем весь документ до EOF, ошибок парсинга
	// быть не должно (доказывает, что экранирование не сломало структуру).
	dec := xml.NewDecoder(strings.NewReader(got))
	for {
		_, err := dec.Token()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			t.Fatalf("сгенерированный plist не является well-formed XML: %v\nfull content:\n%s", err, got)
		}
	}
}

// TestResolveConfigPath — с AGENTIFY_CONFIG_PATH и без (дефолт
// <home>/.agentify/agent.env).
func TestResolveConfigPath(t *testing.T) {
	t.Run("env override", func(t *testing.T) {
		t.Setenv(configPathEnv, "/custom/path/agent.env")
		if got := resolveConfigPath("/home/alice"); got != "/custom/path/agent.env" {
			t.Errorf("resolveConfigPath() = %q, want %q", got, "/custom/path/agent.env")
		}
	})

	t.Run("default", func(t *testing.T) {
		t.Setenv(configPathEnv, "")
		want := filepath.Join("/home/alice", ".agentify", "agent.env")
		if got := resolveConfigPath("/home/alice"); got != want {
			t.Errorf("resolveConfigPath() = %q, want %q", got, want)
		}
	})
}

// TestTargetUser — с SUDO_USER=<текущий пользователь> и без.
func TestTargetUser(t *testing.T) {
	current, err := user.Current()
	if err != nil {
		t.Skipf("user.Current() недоступен в этом окружении: %v", err)
	}

	t.Run("with SUDO_USER", func(t *testing.T) {
		t.Setenv("SUDO_USER", current.Username)
		got, err := targetUser()
		if err != nil {
			t.Fatalf("targetUser() error = %v", err)
		}
		if got.Username != current.Username {
			t.Errorf("targetUser().Username = %q, want %q", got.Username, current.Username)
		}
	})

	t.Run("without SUDO_USER", func(t *testing.T) {
		t.Setenv("SUDO_USER", "")
		got, err := targetUser()
		if err != nil {
			t.Fatalf("targetUser() error = %v", err)
		}
		if got.Username != current.Username {
			t.Errorf("targetUser().Username = %q, want %q", got.Username, current.Username)
		}
	})
}

// TestRunServiceInstall_RequiresRoot — go test всегда выполняется НЕ под
// root, поэтому runServiceInstall должен вернуть ошибку про root/sudo ДО
// каких-либо файловых операций — это делает тест безопасным и
// детерминированным без настоящего systemd/launchd.
func TestRunServiceInstall_RequiresRoot(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("тест не имеет смысла при запуске от root (например, некоторые CI-контейнеры) — проверка root является ветвью-заглушкой в этом случае")
	}

	var stdout bytes.Buffer
	err := runServiceInstall(&stdout)
	if err == nil {
		t.Fatal("runServiceInstall() expected error when not running as root, got nil")
	}
	if !strings.Contains(err.Error(), "root") && !strings.Contains(err.Error(), "sudo") {
		t.Errorf("runServiceInstall() error = %q, want mention of root/sudo", err.Error())
	}
}
