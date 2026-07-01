// agent/service.go — тикет 4.4 (deps: 4.3).
//
// Назначение (бизнес): демонизация агента (FR C5, «Политика автозапуска и
// перезапуска при сбое» — вторая часть FR C5, обновление/совместимость
// версий, не входит, это тикет 4.7). После `agentify-agent setup` (тикет
// 4.3, agent/setup.go) пользователь один раз запускает
// `agentify-agent service-install`, который регистрирует бинарь как
// системный сервис с автозапуском при загрузке ОС и автоматическим
// рестартом при падении процесса: systemd unit на Linux, launchd
// LaunchDaemon на macOS.
//
// Границы (осознанно НЕ входит в этот тикет, см. docs/MVP_TICKETS.md EPIC 4):
//   - обновление демона / контроль совместимости версий — тикет 4.7, deps:
//     4.4;
//   - интерактивный визард настройки конфига — тикет 4.3 (agent/setup.go),
//     этот файл конфиг НЕ пишет и НЕ изменяет, только читает уже
//     существующий;
//   - авто-эскалация до root (self-exec через sudo) — сознательно не
//     делается, см. годок runServiceInstall.
//
// Как устроено (тех): единственная точка входа — runServiceInstall,
// вызываемая из main() по подкоманде `service-install`. Ветвление по ОС —
// runtime.GOOS, без build-тегов (вся логика — чистые строковые генераторы
// unit/plist-файлов плюс os/exec-вызовы внешних системных утилит
// systemctl/launchctl, ничего платформо-специфичного на уровне синтаксиса
// Go нет, поэтому файл кросс-компилируется как есть).
package main

import (
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
)

const (
	// systemdUnitPath — путь системного unit-файла (system-level, не
	// per-user) — сервис должен работать независимо от того, залогинен ли
	// пользователь в сессию (FR C5: автозапуск при загрузке ОС).
	systemdUnitPath = "/etc/systemd/system/agentify-agent.service"

	// launchdPlistPath — путь системного LaunchDaemon (аналог system-level
	// systemd unit на macOS: запускается при загрузке ОС, а не при входе
	// пользователя, в отличие от LaunchAgent).
	launchdPlistPath = "/Library/LaunchDaemons/com.agentify.agent.plist"

	// launchdLabel — Label сервиса в launchd (используется launchctl для
	// адресации job'ы).
	launchdLabel = "com.agentify.agent"
)

// runServiceInstall — реализация команды `agentify-agent service-install`,
// см. годок файла. Устанавливает и включает системный сервис для ТЕКУЩЕЙ ОС
// (runtime.GOOS): автозапуск при загрузке + рестарт при падении процесса
// (FR C5, без обновления/совместимости версий — тикет 4.7). stdout
// параметризован для тестируемости и по аналогии с runSetup(stdin, stdout).
//
// Требует root (напрямую или через sudo) — юнит/plist пишутся в
// root-only-директории (/etc/systemd/system, /Library/LaunchDaemons).
// Никакой авто-эскалации до root (self-exec через sudo) не делается: если
// процесс запущен не от root, функция сразу возвращает понятную ошибку —
// проверка выполняется ДО любых файловых операций, что делает поведение
// детерминированным и без побочных эффектов при запуске без привилегий (в
// т.ч. в юнит-тестах на CI, которые всегда выполняются не под root).
func runServiceInstall(stdout io.Writer) error {
	if os.Geteuid() != 0 {
		return fmt.Errorf("service-install должен быть запущен от root, например: sudo agentify-agent service-install")
	}

	u, err := targetUser()
	if err != nil {
		return fmt.Errorf("не удалось определить целевого пользователя для сервиса: %w", err)
	}
	_, _ = fmt.Fprintf(stdout, "Целевой пользователь сервиса: %s (домашняя директория %s).\n", u.Username, u.HomeDir)

	configPath := resolveConfigPath(u.HomeDir)
	if _, err := os.Stat(configPath); err != nil {
		return fmt.Errorf("конфиг агента не найден (%s) — сначала выполните `agentify-agent setup`: %w", configPath, err)
	}
	_, _ = fmt.Fprintf(stdout, "Конфиг агента: %s.\n", configPath)

	binaryPath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("не удалось определить путь к бинарю агента: %w", err)
	}
	if resolved, err := filepath.EvalSymlinks(binaryPath); err == nil {
		binaryPath = resolved
	}
	_, _ = fmt.Fprintf(stdout, "Бинарь агента: %s.\n", binaryPath)

	// workingDir — директория конфиг-файла (обычно $HOME/.agentify).
	// Обоснование: config.OutboxPath (agent/main.go) по умолчанию
	// относительный ("agent-outbox.db") и сохраняется в CWD процесса; без
	// явного WorkingDirectory= (systemd) / WorkingDirectory (launchd)
	// systemd-юнит стартовал бы с CWD "/", т.е. outbox оказался бы
	// НЕ в персистентной директории данных агента. Указывая WorkingDirectory
	// на директорию конфига, делаем относительный дефолт OutboxPath
	// персистентным без изменения формата конфига/setup.go.
	workingDir := filepath.Dir(configPath)

	switch runtime.GOOS {
	case "linux":
		return installSystemdService(stdout, binaryPath, configPath, workingDir, u.Username)
	case "darwin":
		return installLaunchdService(stdout, binaryPath, configPath, workingDir, u.Username)
	default:
		return fmt.Errorf("service-install: неподдерживаемая ОС %q", runtime.GOOS)
	}
}

// targetUser — определяет пользователя, от имени которого должен работать
// сервис (не root: конфиг агента лежит в $HOME/.agentify/agent.env
// НЕпривилегированного пользователя, см. agent/setup.go). При запуске через
// `sudo agentify-agent service-install` sudo проставляет SUDO_USER —
// исходный (непривилегированный) пользователь, от чьего имени вызван sudo;
// именно его и нужно использовать, а не root (os/user.Current() под sudo
// вернул бы root).
//
// Если service-install запущен НЕ через sudo, а напрямую под root (login
// root, SUDO_USER пуст) — целевым пользователем окажется сам root. Это
// осознанное упрощение MVP: полноценный выбор произвольного целевого
// пользователя (флаг --user и т.п.) можно добавить отдельным тикетом, если
// понадобится.
func targetUser() (*user.User, error) {
	if sudoUser := os.Getenv("SUDO_USER"); sudoUser != "" {
		return user.Lookup(sudoUser)
	}
	u, err := user.Current()
	if err != nil {
		return nil, fmt.Errorf("SUDO_USER не задан и не удалось определить текущего пользователя: %w", err)
	}
	return u, nil
}

// resolveConfigPath — путь к конфигу демона для сервиса: AGENTIFY_CONFIG_PATH
// (та же переменная, что и в agent/setup.go, configPathEnv), если задана,
// иначе <home>/.agentify/agent.env. НЕ переиспользует setup.go:configFilePath
// напрямую, т.к. та берёт os.UserHomeDir() текущего процесса — под sudo это
// HOME рута, а не целевого (непривилегированного) пользователя сервиса; home
// сюда передаётся явно, уже определённый через targetUser().
func resolveConfigPath(home string) string {
	if p := os.Getenv(configPathEnv); p != "" {
		return p
	}
	return filepath.Join(home, ".agentify", "agent.env")
}

// --- Linux (systemd) --------------------------------------------------------

// systemdUnitContent — чистая строковая генерация unit-файла systemd, без
// побочных эффектов (важно для юнит-тестов, agent/service_test.go).
// EnvironmentFile= указывает systemd читать AGENT_*-переменные
// непосредственно из конфиг-файла при каждом (пере)старте сервиса — в
// отличие от launchd (см. installLaunchdService), значения не нужно
// встраивать статично на момент установки.
func systemdUnitContent(binaryPath, configPath, workingDir, username string) string {
	return fmt.Sprintf(`[Unit]
Description=agentify-agent
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=%s
WorkingDirectory=%s
EnvironmentFile=%s
ExecStart=%s
Restart=on-failure
RestartSec=2

[Install]
WantedBy=multi-user.target
`, username, workingDir, configPath, binaryPath)
}

// installSystemdService пишет unit-файл и включает автозапуск+рестарт через
// systemctl. Явный `restart` ПОСЛЕ `enable --now` гарантирует, что повторный
// запуск service-install (например, после правки конфига через новый
// `setup`) подхватывает свежий EnvironmentFile=, а не оставляет старый
// процесс работать со старыми переменными; `enable --now` идемпотентен для
// первой установки, `restart` следом безопасен и на первом запуске (сервис
// уже запущен).
func installSystemdService(stdout io.Writer, binaryPath, configPath, workingDir, username string) error {
	content := systemdUnitContent(binaryPath, configPath, workingDir, username)

	// Юнит-файл не секретен (креды остаются только в конфиге, права 0600,
	// который EnvironmentFile= читает отдельно) — 0644, как принято для
	// systemd unit-файлов.
	if err := os.WriteFile(systemdUnitPath, []byte(content), 0o644); err != nil {
		return fmt.Errorf("не удалось записать unit-файл %s: %w", systemdUnitPath, err)
	}
	_, _ = fmt.Fprintf(stdout, "systemd unit записан: %s.\n", systemdUnitPath)

	for _, args := range [][]string{
		{"daemon-reload"},
		{"enable", "--now", "agentify-agent.service"},
		{"restart", "agentify-agent.service"},
	} {
		if err := runSystemctl(stdout, args...); err != nil {
			return err
		}
	}

	_, _ = fmt.Fprintln(stdout, "Сервис agentify-agent установлен и запущен (systemd, автозапуск + рестарт при сбое).")
	return nil
}

// runSystemctl выполняет один вызов systemctl, пробрасывая stdout/stderr
// дочернего процесса напрямую в os.Stdout/os.Stderr (не в параметр stdout
// функции — тот используется только для собственных информационных
// сообщений service.go, по аналогии с runSetup).
func runSystemctl(stdout io.Writer, args ...string) error {
	_, _ = fmt.Fprintf(stdout, "systemctl %s\n", strings.Join(args, " "))
	cmd := exec.Command("systemctl", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("systemctl %s: %w", strings.Join(args, " "), err)
	}
	return nil
}

// --- macOS (launchd) ---------------------------------------------------------

// parseEnvFile читает простой KEY=VALUE-файл (тот же формат, что пишет
// agent/setup.go: без export/кавычек, по одной переменной на строку),
// пропуская пустые строки. Split — только по ПЕРВОМУ "=", чтобы значения,
// сами содержащие "=" (например API-ключи), не обрезались.
func parseEnvFile(path string) (map[string]string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("не удалось прочитать конфиг агента %s: %w", path, err)
	}

	env := make(map[string]string)
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		env[key] = value
	}
	return env, nil
}

// launchdPlistContent генерирует XML LaunchDaemon plist. launchd, в отличие
// от systemd EnvironmentFile=, не умеет читать переменные окружения из
// внешнего файла на момент (пере)старта — значения из конфига встраиваются
// в сам plist как literal-строки на момент установки. Следствие
// (осознанное ограничение самого launchd, без workaround в рамках этого
// тикета): если пользователь позже поменяет конфиг через `agentify-agent
// setup`, новые значения подхватятся только после повторного
// `service-install` (перегенерации и перезагрузки plist).
//
// Значения (username, workingDir, binaryPath, каждый ключ/значение из
// конфига) — потенциально произвольные строки, включая API-ключи
// пользователя, которые могут содержать XML-спецсимволы. Каждая
// подставляемая строка экранируется через xml.EscapeText — без этого
// значение вроде `sk-<key>&x` сломало бы структуру plist (или в худшем
// случае позволило бы инъекцию дополнительных XML-узлов).
func launchdPlistContent(binaryPath, configPath, workingDir, username string) (string, error) {
	env, err := parseEnvFile(configPath)
	if err != nil {
		return "", err
	}

	keys := make([]string, 0, len(env))
	for k := range env {
		keys = append(keys, k)
	}
	sort.Strings(keys) // детерминированный порядок — иначе тесты/диффы плист-файла флакуют.

	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n")
	b.WriteString(`<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">` + "\n")
	b.WriteString(`<plist version="1.0">` + "\n")
	b.WriteString("<dict>\n")
	b.WriteString("\t<key>Label</key>\n")
	writePlistString(&b, launchdLabel)
	b.WriteString("\t<key>UserName</key>\n")
	writePlistString(&b, username)
	b.WriteString("\t<key>WorkingDirectory</key>\n")
	writePlistString(&b, workingDir)
	b.WriteString("\t<key>ProgramArguments</key>\n")
	b.WriteString("\t<array>\n")
	b.WriteString("\t\t")
	writePlistString(&b, binaryPath)
	b.WriteString("\t</array>\n")
	b.WriteString("\t<key>EnvironmentVariables</key>\n")
	b.WriteString("\t<dict>\n")
	for _, k := range keys {
		b.WriteString("\t\t<key>")
		_ = xml.EscapeText(&b, []byte(k))
		b.WriteString("</key>\n\t\t")
		writePlistString(&b, env[k])
	}
	b.WriteString("\t</dict>\n")
	b.WriteString("\t<key>RunAtLoad</key>\n\t<true/>\n")
	b.WriteString("\t<key>KeepAlive</key>\n\t<true/>\n")
	b.WriteString("</dict>\n</plist>\n")

	return b.String(), nil
}

// writePlistString пишет один <string>ЗНАЧЕНИЕ</string>-узел с экранированным
// (xml.EscapeText) содержимым и переводом строки после закрывающего тега.
func writePlistString(b *strings.Builder, value string) {
	b.WriteString("<string>")
	_ = xml.EscapeText(b, []byte(value))
	b.WriteString("</string>\n")
}

// writeLaunchdPlist пишет уже сгенерированное содержимое plist (см. godoc
// launchdPlistContent) по указанному path с правами 0600. Вынесена в
// отдельную функцию, параметризованную path (тот же приём, что и остальные
// функции файла уже параметризуют configPath/workingDir/binaryPath вместо
// использования глобальных констант напрямую), чтобы саму запись можно было
// покрыть юнит-тестом (agent/service_test.go) во временной директории, не
// требуя root и системного пути launchdPlistPath.
//
// Права 0600, а не 0644 (как у systemdUnitContent/installSystemdService,
// где это осознанно верно — см. её комментарий): plist встраивает креды
// провайдера в открытом виде (см. godoc launchdPlistContent) — файл должен
// быть root-only, как и agent.env, а не мирочитаемым, иначе любой локальный
// непривилегированный пользователь машины смог бы прочитать API-ключ
// провайдера (тикет 4.6, FR C4).
func writeLaunchdPlist(path, content string) error {
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		return fmt.Errorf("не удалось записать launchd plist %s: %w", path, err)
	}
	return nil
}

// installLaunchdService пишет plist и загружает его через launchctl.
func installLaunchdService(stdout io.Writer, binaryPath, configPath, workingDir, username string) error {
	content, err := launchdPlistContent(binaryPath, configPath, workingDir, username)
	if err != nil {
		return err
	}

	if err := writeLaunchdPlist(launchdPlistPath, content); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(stdout, "launchd plist записан: %s.\n", launchdPlistPath)

	_, _ = fmt.Fprintf(stdout, "launchctl load -w %s\n", launchdPlistPath)
	cmd := exec.Command("launchctl", "load", "-w", launchdPlistPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("launchctl load -w %s: %w", launchdPlistPath, err)
	}

	_, _ = fmt.Fprintln(stdout, "Сервис agentify-agent установлен и запущен (launchd, автозапуск + рестарт при сбое).")
	return nil
}
