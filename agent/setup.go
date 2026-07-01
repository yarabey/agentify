// agent/setup.go — тикет 4.3 (deps: 4.2).
//
// Назначение (бизнес): интерактивная фаза настройки агента (FR C2;
// docs/User_stories_Gherkin.md «Установка агента одной командой», сценарии
// «Установка на чистой системе» и «Пользователь выбирает провайдера сам»).
// После установки бинаря (тикет 4.2, install/install.sh) пользователь
// запускает `agentify-agent setup` — команда спрашивает адрес оркестратора,
// UUID машины (выданный при создании интеграции в веб-интерфейсе,
// POST /integrations, тикет 2.2) и провайдер(ы) ИИ с их учётными данными, и
// записывает всё в локальный конфиг-файл с правами 0600. Система НЕ
// управляет оплатой провайдера и не передаёт креды оркестратору (FR C4) —
// они остаются только в этом файле.
//
// Границы (осознанно НЕ входит в этот тикет, см. docs/MVP_TICKETS.md EPIC 4):
//   - демонизация (systemd/launchd, автозапуск/рестарт) — тикет 4.4, deps:
//     4.3. runSetup ничего не запускает в фоне, только печатает финальную
//     подсказку про следующий шаг;
//   - безопасное хранение кредов сверх файла 0600 (keychain и т.п.) —
//     тикет 4.6, deps: 4.3;
//   - реальный запуск/использование `claude` CLI подпроцесса — тикет 4.5;
//   - сетевая валидация UUID против оркестратора — не входит: здесь только
//     синтаксическая проверка формата (uuid.Parse). Реальная аутентификация
//     по UUID происходит позже, автоматически, при первом коннекте демона к
//     /machine/ws (тикеты 2.3/3.3, agent/internal/wsclient) — этот путь
//     setup.go не трогает.
//
// Как устроено (тех): runSetup — один линейный проход БЕЗ циклов повтора:
// на невалидный ввод сразу возвращается ошибка, для повтора пользователь
// перезапускает `agentify-agent setup` целиком. Это осознанное упрощение
// MVP — полноценный интерактивный ре-запрос при ошибке ввода можно добавить
// отдельным тикетом, если понадобится. Каждый шаг сперва проверяет
// соответствующую env-переменную под префиксом AGENTIFY_SETUP_ (отдельным от
// рантайм-префикса AGENT_ — это входные данные визарда, а не конфиг
// демона; аналогично тому, как install.sh уже разделяет
// инсталлятор-уровневый префикс AGENTIFY_ и рантайм-префикс AGENT_) — это и
// есть «неинтерактивный режим через env для CI» из приёмки тикета; если
// переменной нет, читаем строку из stdin через один общий bufio.Scanner,
// созданный один раз в начале функции (важно для буферизации между
// последовательными чтениями). Результат — обычный текстовый файл
// KEY=VALUE (без export/кавычек — формат, который напрямую понимает
// systemd EnvironmentFile=, см. будущий демон-юнит тикета 4.4), права 0600
// выставляются явно через os.Chmod после записи (защита от умаска или уже
// существующего файла с другими правами).
package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"golang.org/x/term"
)

// setupEnvPrefix — префикс env-переменных интерактивной настройки
// (agentify-agent setup), см. годок пакета. Не путать с envPrefix
// ("AGENT_") — тем рантайм-конфигом, что грузит platform.LoadConfig в run().
const setupEnvPrefix = "AGENTIFY_SETUP_"

// configPathEnv — переменная, которой можно переопределить путь к итоговому
// конфиг-файлу (используется и в CI/тестах, и для нестандартных раскладок
// на машине агента). Без префикса AGENTIFY_SETUP_, т.к. это не входные
// данные визарда, а параметр самой операции записи.
const configPathEnv = "AGENTIFY_CONFIG_PATH"

// knownProviders — допустимые значения провайдера ИИ (FR C2: «я выбираю
// Claude или Claude Code»). Ключ — provider ID (как пишется в
// AGENT_PROVIDERS и передаётся в hello, docs/protocol.md §4), значение —
// суффикс для env-переменной креда (AGENTIFY_SETUP_<suffix>_API_KEY) и для
// итоговой переменной в конфиге (AGENT_<suffix>_API_KEY).
var knownProviders = map[string]string{
	"claude":      "CLAUDE",
	"claude-code": "CLAUDE_CODE",
}

// runSetup — реализация команды `agentify-agent setup`, см. годок пакета.
// stdin/stdout параметризованы для тестируемости (agent/setup_test.go) —
// без них пришлось бы гонять реальный os.Stdin в юнит-тестах.
func runSetup(stdin io.Reader, stdout io.Writer) error {
	scanner := bufio.NewScanner(stdin)

	orchestratorWSURL, err := setupOrchestratorURL(scanner, stdout)
	if err != nil {
		return err
	}

	integrationUUID, err := setupIntegrationUUID(scanner, stdout)
	if err != nil {
		return err
	}

	providers, err := setupProviders(scanner, stdout)
	if err != nil {
		return err
	}

	credentials, err := setupCredentials(scanner, stdin, stdout, providers)
	if err != nil {
		return err
	}

	path, err := configFilePath()
	if err != nil {
		return err
	}

	if err := writeConfigFile(path, orchestratorWSURL, integrationUUID, providers, credentials); err != nil {
		return err
	}

	// Ошибки записи в stdout здесь намеренно игнорируются (_, _ =) — это
	// финальный информационный вывод уже ПОСЛЕ успешной записи конфига,
	// сбой терминала/пайпа на этом этапе не должен превращаться в fatal-код
	// возврата runSetup (тот же принцип, что fmt.Println(version) в main()).
	_, _ = fmt.Fprintf(stdout, "Конфиг агента записан: %s (права 0600).\n", path)
	_, _ = fmt.Fprintf(stdout, "Провайдер(ы): %s.\n", strings.Join(providers, ", "))
	_, _ = fmt.Fprintln(stdout, "Учётные данные хранятся только локально в этом файле и не передаются оркестратору (FR C4).")
	_, _ = fmt.Fprintln(stdout, "Дальше: настройка автозапуска демона (systemd/launchd) — отдельный шаг (тикет 4.4).")

	return nil
}

// setupOrchestratorURL — шаг A: адрес оркестратора → полный WS-URL
// /machine/ws. Env AGENTIFY_SETUP_ORCHESTRATOR_URL используется КАК ЕСТЬ,
// без derivation (ожидается, что в CI он уже полный и корректный); при
// интерактивном вводе пользователь даёт голый host[:port], а схему и путь
// /machine/ws достраиваем сами — так короче и меньше шансов на опечатку в
// схеме/пути.
func setupOrchestratorURL(scanner *bufio.Scanner, stdout io.Writer) (string, error) {
	if raw := os.Getenv(setupEnvPrefix + "ORCHESTRATOR_URL"); raw != "" {
		if !strings.HasPrefix(raw, "ws://") && !strings.HasPrefix(raw, "wss://") {
			return "", fmt.Errorf("AGENTIFY_SETUP_ORCHESTRATOR_URL должен начинаться с ws:// или wss://")
		}
		return raw, nil
	}

	_, _ = fmt.Fprint(stdout, "Адрес оркестратора (host[:port], без схемы — например orchestrator.example.com): ")
	raw, err := readLine(scanner, "не удалось прочитать адрес оркестратора")
	if err != nil {
		return "", err
	}
	if raw == "" {
		return "", fmt.Errorf("адрес оркестратора не может быть пустым")
	}

	base := strings.TrimSuffix(raw, "/")
	if !strings.HasPrefix(base, "ws://") && !strings.HasPrefix(base, "wss://") {
		base = "wss://" + base
	}
	if !strings.HasSuffix(base, "/machine/ws") {
		base += "/machine/ws"
	}
	return base, nil
}

// setupIntegrationUUID — шаг B: UUID машины, выданный при создании
// интеграции в веб-интерфейсе (POST /integrations, тикет 2.2). Проверяется
// только синтаксически (uuid.Parse) — см. годок пакета про границы.
func setupIntegrationUUID(scanner *bufio.Scanner, stdout io.Writer) (string, error) {
	raw := os.Getenv(setupEnvPrefix + "UUID")
	if raw == "" {
		_, _ = fmt.Fprint(stdout, "UUID машины (получен в веб-интерфейсе при создании интеграции): ")
		var err error
		raw, err = readLine(scanner, "не удалось прочитать UUID машины")
		if err != nil {
			return "", err
		}
	}

	if _, err := uuid.Parse(raw); err != nil {
		return "", fmt.Errorf("UUID машины невалиден: %w", err)
	}
	return raw, nil
}

// setupProviders — шаг C: список провайдеров ИИ (FR C2: «я выбираю Claude
// или Claude Code»). Дедуплицирует, сохраняя порядок первого появления.
func setupProviders(scanner *bufio.Scanner, stdout io.Writer) ([]string, error) {
	raw := os.Getenv(setupEnvPrefix + "PROVIDERS")
	if raw == "" {
		_, _ = fmt.Fprint(stdout, "Провайдер(ы) ИИ, через запятую (доступно: claude, claude-code): ")
		var err error
		raw, err = readLine(scanner, "не удалось прочитать список провайдеров")
		if err != nil {
			return nil, err
		}
	}

	seen := make(map[string]bool)
	var providers []string
	for _, tok := range strings.Split(raw, ",") {
		tok = strings.TrimSpace(tok)
		if tok == "" {
			continue
		}
		if _, ok := knownProviders[tok]; !ok {
			return nil, fmt.Errorf("неизвестный провайдер %q — допустимые значения: claude, claude-code", tok)
		}
		if seen[tok] {
			continue
		}
		seen[tok] = true
		providers = append(providers, tok)
	}

	if len(providers) == 0 {
		return nil, fmt.Errorf("нужно выбрать хотя бы одного провайдера")
	}
	return providers, nil
}

// setupCredentials — шаг D: креды для каждого выбранного провайдера.
// Порядок опроса — порядок providers (порядок первого появления из шага C).
func setupCredentials(scanner *bufio.Scanner, stdin io.Reader, stdout io.Writer, providers []string) (map[string]string, error) {
	credentials := make(map[string]string, len(providers))

	for _, provider := range providers {
		suffix := knownProviders[provider]

		cred := os.Getenv(setupEnvPrefix + suffix + "_API_KEY")
		if cred == "" {
			_, _ = fmt.Fprintf(stdout, "API-ключ/токен для %s: ", provider)

			var err error
			cred, err = readCredential(scanner, stdin, stdout)
			if err != nil {
				return nil, fmt.Errorf("не удалось прочитать креды для провайдера %q: %w", provider, err)
			}
		}

		cred = strings.TrimSpace(cred)
		if cred == "" {
			return nil, fmt.Errorf("креды для провайдера %q не заданы", provider)
		}
		credentials[provider] = cred
	}

	return credentials, nil
}

// readCredential читает один креденшел со stdin: маскированно через
// term.ReadPassword, если stdin — реальный терминал, иначе обычной строкой
// через тот же bufio.Scanner, что и остальные шаги (нужно для CI/тестов, где
// stdin — не *os.File или не TTY).
func readCredential(scanner *bufio.Scanner, stdin io.Reader, stdout io.Writer) (string, error) {
	if f, ok := stdin.(*os.File); ok && term.IsTerminal(int(f.Fd())) {
		b, err := term.ReadPassword(int(f.Fd()))
		_, _ = fmt.Fprintln(stdout) // ReadPassword не печатает перевод строки сам
		if err != nil {
			return "", err
		}
		return string(b), nil
	}

	return readLine(scanner, "не удалось прочитать значение")
}

// readLine читает и тримит одну строку из общего scanner. errMsg — префикс
// сообщения об ошибке при неуспешном/пустом чтении (EOF или ошибка scanner).
func readLine(scanner *bufio.Scanner, errMsg string) (string, error) {
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return "", fmt.Errorf("%s: %w", errMsg, err)
		}
		return "", fmt.Errorf("%s: EOF", errMsg)
	}
	return strings.TrimSpace(scanner.Text()), nil
}

// configFilePath — путь к итоговому конфиг-файлу: AGENTIFY_CONFIG_PATH, если
// задана, иначе $HOME/.agentify/agent.env (дефолт для реальной установки).
func configFilePath() (string, error) {
	if p := os.Getenv(configPathEnv); p != "" {
		return p, nil
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("не удалось определить домашнюю директорию для конфига агента: %w", err)
	}
	return filepath.Join(home, ".agentify", "agent.env"), nil
}

// writeConfigFile пишет KEY=VALUE-конфиг демона (см. годок пакета) с
// правами 0600. Повторный запуск `setup` перезаписывает файл целиком — это
// ожидаемое поведение для реконфигурации машины, отдельного подтверждения
// не требуется (MVP: единственный способ переконфигурировать агента —
// перезапустить setup заново).
func writeConfigFile(path, orchestratorWSURL, integrationUUID string, providers []string, credentials map[string]string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("не удалось создать директорию для конфига агента %q: %w", filepath.Dir(path), err)
	}

	var b strings.Builder
	fmt.Fprintf(&b, "AGENT_ORCHESTRATOR_WS_URL=%s\n", orchestratorWSURL)
	fmt.Fprintf(&b, "AGENT_INTEGRATION_UUID=%s\n", integrationUUID)
	fmt.Fprintf(&b, "AGENT_PROVIDERS=%s\n", strings.Join(providers, ","))
	for _, provider := range providers {
		suffix := knownProviders[provider]
		fmt.Fprintf(&b, "AGENT_%s_API_KEY=%s\n", suffix, credentials[provider])
	}

	if err := os.WriteFile(path, []byte(b.String()), 0o600); err != nil {
		return fmt.Errorf("не удалось записать конфиг агента %q: %w", path, err)
	}
	// Явный Chmod ПОСЛЕ записи — защита от умаска процесса или уже
	// существующего файла с другими правами (WriteFile применяет
	// запрошенный режим только при создании нового файла, но не меняет режим
	// уже существующего).
	if err := os.Chmod(path, 0o600); err != nil {
		return fmt.Errorf("не удалось выставить права 0600 на конфиг агента %q: %w", path, err)
	}

	return nil
}
