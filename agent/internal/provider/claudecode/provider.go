// Package claudecode — провайдер задач поверх CLI `claude` (тикет 4.5,
// deps 4.4/6.1; docs/MVP_TICKETS.md строки 223-226).
//
// Назначение (бизнес): агент выполняет задачу не сам, а поручая её
// установленному на машине CLI `claude` (Claude Code) — Provider запускает
// его подпроцессом и управляет его stdio по двунаправленному
// NDJSON-протоколу (`--input-format stream-json --output-format
// stream-json`, эмпирически подтверждено live smoke-тестом против реального
// бинаря v2.1.196). Ключевая бизнес-логика этого тикета (FR C3, FR F3,
// docs/User_stories_Gherkin.md §5): когда CLI просит разрешение на
// выполнение конкретного вызова инструмента (control_request с
// request.subtype=="can_use_tool", например Bash-команда), Provider решает
// сам — ТОЛЬКО если вызов входит в allowlist (AllowChecker, тикет 6.3),
// отвечая CLI allow немедленно, без какого-либо ожидания. Если вызов вне
// allowlist — Provider НЕ отвечает CLI сразу (подпроцесс блокируется на
// этом конкретном control_request до ответа), а публикует событие
// command_approval_request через Publisher (совместим с
// agent/internal/outbox.Store.Enqueue) и ждёт решения пользователя,
// доставленного вызовом Approve (тикет 6.4 — обработчик command_decision на
// стороне оркестратора/agent — вне объёма этого тикета; здесь только точка
// расширения).
//
// Как устроено (тех): Provider.Run — единственный метод жизненного цикла
// одного запуска задачи: exec.CommandContext (см. godoc Run про отмену
// ctx — намеренный задел под тикет 8.4 "отмена долетает до машины"),
// StdinPipe/StdoutPipe, запись первой строки-задачи, затем построчное
// чтение stdout через bufio.Scanner с увеличенным буфером (JSON-строки с
// input инструмента могут быть длиннее дефолтных 64KiB). Каждая строка
// сперва разбирается только по полю type (rawLine, см. wire.go); строки,
// отличные от control_request, молча пропускаются (assistant/result/
// stream_event/... — построение полной модели agent_progress/
// agent_completed/error намеренно вне объёма тикета 4.5, см.
// docs/MVP_TICKETS.md 5.x). Для control_request с
// request.subtype=="can_use_tool" — либо немедленный allow (allowlist),
// либо регистрация в pending (map, под мьютексом) и публикация
// command_approval_request; ответ CLI в этом случае формирует Approve,
// вызываемый ПОЗЖЕ, из другой горутины (когда придёт решение
// пользователя) — запись в stdin подпроцесса защищена отдельным мьютексом
// (Provider.mu), общим для обоих путей (немедленный allow из Run и
// allow/deny из Approve).
package claudecode

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/yarabey/agentify/internal/bus"
)

// maxScannerLine — верхняя граница длины ОДНОЙ строки NDJSON stdout CLI,
// которую умеет прочитать bufio.Scanner (см. godoc пакета: дефолтные 64KiB
// bufio.Scanner маловаты для строк с input больших инструментов/diff'ов).
// 10MiB — с большим запасом относительно любого реалистичного
// tool_use/control_request, но не безграничный (защита от аномального
// подпроцесса, который никогда не шлёт '\n').
const maxScannerLine = 10 * 1024 * 1024

// ErrUnknownRequest возвращает Approve, если requestID не найден среди
// ожидающих решения control_request (никогда не существовал, уже разрешён
// предыдущим вызовом Approve, либо относится к уже завершившемуся
// Run) — штатная ситуация at-least-once (повторная/поздняя доставка
// command_decision, см. godoc AckPayload в internal/bus/messages.go, тот же
// принцип), а не повод паниковать или ронять агент.
var ErrUnknownRequest = errors.New("claudecode: неизвестный или уже разрешённый request_id")

// ErrInvalidDecision возвращает Approve при decision, отличном от "approve"
// и "reject" (protocol.md §4, CommandDecisionPayload.Decision).
var ErrInvalidDecision = errors.New(`claudecode: decision должен быть "approve" или "reject"`)

// Publisher — узкий интерфейс публикации событий агент→оркестратор,
// которым Provider оповещает о вызове инструмента вне allowlist
// (command_approval_request). Совпадает по имени и сигнатуре метода с
// agent/internal/outbox.Store.Enqueue (см. godoc Store.Enqueue) — это
// НАМЕРЕННО: *outbox.Store удовлетворяет Publisher структурно, без
// адаптера, а тесты пакета claudecode подставляют fake-Publisher в памяти
// (см. provider_test.go), не поднимая bbolt.
type Publisher interface {
	// Enqueue durable-публикует конверт события (см. outbox.Store.Enqueue).
	Enqueue(env bus.Envelope) error
}

// Config — параметры одного Provider (одна задача, один подпроцесс
// `claude`, см. godoc пакета).
type Config struct {
	// BinPath — путь к бинарю `claude`. Пусто → резолвится через PATH как
	// "claude" (см. New).
	BinPath string
	// WorkDir — рабочая директория подпроцесса (CLI выполняет
	// файловые/shell-инструменты относительно неё).
	WorkDir string
	// AllowChecker — allowlist вызовов инструментов (см. godoc AllowChecker).
	// Пусто (nil) → New подставляет EmptyAllowChecker (безопасный дефолт).
	AllowChecker AllowChecker
	// Publisher — приёмник событий command_approval_request (см. godoc
	// Publisher). Обязателен: без него некому передать запрос на
	// согласование вне allowlist.
	Publisher Publisher
	// TaskID — идентификатор задачи (protocol.md §2); входит в каждый
	// публикуемый конверт (Envelope.TaskID). Обязателен — событие
	// command_approval_request всегда task-scoped.
	TaskID string
	// IntegrationID — идентификатор интеграции/машины (protocol.md §2);
	// входит в каждый публикуемый конверт (Envelope.IntegrationID).
	// Обязателен.
	IntegrationID string
	// Logger — логгер Provider. Пусто (nil) → New подставляет slog.Default().
	Logger *slog.Logger
	// Env — дополнительные переменные окружения подпроцесса ("KEY=VALUE"),
	// добавляемые к унаследованным от родителя (os.Environ()) — например,
	// CLAUDE_CODE_API_KEY (agent/main.go, тикет 4.4) в проде или
	// FAKE_CLAUDE_SCRIPT для тестового двойника (provider_test.go). Пусто →
	// подпроцесс получает окружение процесса агента как есть.
	Env []string
	// Stderr — куда копировать stderr подпроцесса (диагностика). Пусто
	// (nil) → stderr только буферизуется внутри и попадает в текст ошибки
	// Run при ненулевом коде выхода (см. Run).
	Stderr io.Writer
}

// Ошибки валидации Config, возвращаемые New (конструктор не паникует — та
// же дисциплина, что agent/internal/wsclient.New).
var (
	// ErrNilPublisher — New вызван с Config.Publisher == nil.
	ErrNilPublisher = errors.New("claudecode: nil Publisher")
	// ErrEmptyTaskID — New вызван с пустым Config.TaskID.
	ErrEmptyTaskID = errors.New("claudecode: пустой TaskID")
	// ErrEmptyIntegrationID — New вызван с пустым Config.IntegrationID.
	ErrEmptyIntegrationID = errors.New("claudecode: пустой IntegrationID")
)

// pendingRequest — состояние одного ещё не разрешённого control_request:
// то, что нужно Approve, чтобы сформировать ответ CLI (см. godoc Approve).
type pendingRequest struct {
	toolName string
	input    json.RawMessage
}

// Provider — обёртка над одним подпроцессом `claude` (см. godoc пакета).
// Собирается New; готов к однократному Run. Повторное использование одного
// Provider для нескольких задач не предусмотрено (одна задача — один
// Provider — один подпроцесс).
type Provider struct {
	cfg          Config
	allowChecker AllowChecker
	logger       *slog.Logger

	// mu защищает cmd и stdin — оба поля пишет Run при старте подпроцесса и
	// читает Close (может вызываться из другой горутины конкурентно со
	// стартом/остановкой, см. godoc пакета); writeLine тоже берёт mu на всё
	// время записи, чтобы немедленный allow из readLoop (Run) и allow/deny
	// из Approve никогда не писали в stdin одновременно.
	mu    sync.Mutex
	stdin io.WriteCloser
	cmd   *exec.Cmd

	pendingMu sync.Mutex
	pending   map[string]pendingRequest
}

// New собирает Provider, валидируя Config (см. поля Config и sentinel
// ошибки выше). Не паникует на невалидном конфиге.
func New(cfg Config) (*Provider, error) {
	if cfg.Publisher == nil {
		return nil, ErrNilPublisher
	}
	if cfg.TaskID == "" {
		return nil, ErrEmptyTaskID
	}
	if cfg.IntegrationID == "" {
		return nil, ErrEmptyIntegrationID
	}
	if cfg.BinPath == "" {
		cfg.BinPath = "claude"
	}
	allowChecker := cfg.AllowChecker
	if allowChecker == nil {
		allowChecker = EmptyAllowChecker{}
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Provider{
		cfg:          cfg,
		allowChecker: allowChecker,
		logger:       logger,
		pending:      make(map[string]pendingRequest),
	}, nil
}

// claudeArgs — аргументы запуска CLI `claude` (эмпирически подтверждены
// живым smoke-тестом против реального бинаря /opt/node22/bin/claude
// v2.1.196, см. финальный отчёт тикета 4.5):
//   - --print: неинтерактивный режим, единственный запрошенный ответ и
//     выход (без него CLI стартует интерактивный REPL).
//   - --input-format stream-json --output-format stream-json: двунаправленный
//     NDJSON-протокол (тикет 4.5) — без него CLI ожидает/пишет обычный
//     текст, а не JSON-строки, и control_request/control_response
//     недоступны как формат ввода-вывода в принципе.
//   - --verbose: обязателен вместе с --output-format=stream-json и --print
//     (CLI требует его для этой комбинации, см. `claude --help`: "Override
//     verbose mode setting from config").
//   - --permission-mode default: базовый режим разрешений (ни "во всём
//     доверять", ни "интерактивный TTY-prompt") — control_request/
//     can_use_tool относится именно к механике проверки разрешений на
//     уровне ЭТОГО режима.
//
// ВАЖНО (задокументированная неопределённость, см. финальный отчёт): живые
// прогоны с ЭТИМ набором флагов подтвердили формат NDJSON-стрима как
// таковой (system/assistant/tool_use/tool_result/result), но ни разу не
// вызвали появление строки control_request над обычным локальным stdio —
// оба опробованных случая (безобидная и разрушительная команда)
// авто-разрешились/авто-заблокировались внутри CLI без внешнего
// вмешательства. Статический анализ бандла cli.js показал, что реальный
// control_request-путь включается либо --sdk-url (перенаправляет весь
// транспорт на сетевую сессию Anthropic — не подходит для локального
// подпроцесса), либо --permission-prompt-tool + MCP-сервер (другой
// wire-протокол, JSON-RPC, не control_request/control_response) — при этом
// --permission-prompt-tool отсутствует в `claude --help` установленного
// бинаря v2.1.196. Однако сам формат control_request/control_response,
// который реализует этот Provider, — реальный, задокументированный тикетом
// протокол CLI (подтверждён исходным кодом cli.js), поэтому Provider
// реализован строго по нему; какой именно флаг активирует его в
// установленной версии бинаря — открытый вопрос, требующий отдельной
// проверки ПЕРЕД тем, как Provider будет подключён к боевому циклу
// (тикеты 5.4/6.3), см. финальный отчёт.
func claudeArgs() []string {
	return []string{
		"--print",
		"--input-format", "stream-json",
		"--output-format", "stream-json",
		"--verbose",
		"--permission-mode", "default",
	}
}

// Run запускает подпроцесс `claude`, ставит задачу первой строкой stdin и
// обрабатывает его stdout до завершения подпроцесса или отмены ctx (см.
// godoc пакета). Отмена ctx останавливает подпроцесс через
// exec.CommandContext (SIGKILL, задел под тикет 8.4) — Run в этом случае
// возвращает ctx.Err(), а не ошибку подпроцесса.
//
// Run владеет подпроцессом единолично: конкурентно с ним допустимо звать
// ТОЛЬКО Approve (для разрешения ранее опубликованных
// command_approval_request) и Close (для принудительной остановки).
// Повторный вызов Run на одном Provider не поддерживается.
func (p *Provider) Run(ctx context.Context, taskText string) error {
	cmd := exec.CommandContext(ctx, p.cfg.BinPath, claudeArgs()...)
	cmd.Dir = p.cfg.WorkDir
	if len(p.cfg.Env) > 0 {
		cmd.Env = append(os.Environ(), p.cfg.Env...)
	}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("claudecode: stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("claudecode: stdout pipe: %w", err)
	}
	var stderr bytes.Buffer
	if p.cfg.Stderr != nil {
		cmd.Stderr = io.MultiWriter(&stderr, p.cfg.Stderr)
	} else {
		cmd.Stderr = &stderr
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("claudecode: запуск %s: %w", p.cfg.BinPath, err)
	}

	p.mu.Lock()
	p.cmd = cmd
	p.stdin = stdin
	p.mu.Unlock()

	if err := p.writeLine(newUserMessageLine(taskText)); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return fmt.Errorf("claudecode: запись строки задачи: %w", err)
	}

	scanErr := p.readLoop(stdout)

	waitErr := cmd.Wait()

	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if scanErr != nil {
		return fmt.Errorf("claudecode: чтение stdout: %w", scanErr)
	}
	if waitErr != nil {
		return fmt.Errorf("claudecode: подпроцесс %s: %w (stderr: %s)", p.cfg.BinPath, waitErr, stderr.String())
	}
	return nil
}

// readLoop построчно читает stdout подпроцесса (bufio.Scanner с увеличенным
// буфером, см. maxScannerLine) до EOF/ошибки, разбирая каждую строку
// сначала как rawLine (см. wire.go), а затем — только для
// type=="control_request" — как controlRequestLine (см.
// handleControlRequest). Любая иная строка (пустая, не-JSON, иного type)
// молча пропускается (см. godoc пакета).
func (p *Provider) readLoop(stdout io.Reader) error {
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 64*1024), maxScannerLine)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}

		var raw rawLine
		if err := json.Unmarshal(line, &raw); err != nil {
			p.logger.Warn("claudecode: не-JSON строка stdout, пропускаю", "error", err)
			continue
		}
		if raw.Type != "control_request" {
			continue
		}

		var req controlRequestLine
		if err := json.Unmarshal(line, &req); err != nil {
			p.logger.Warn("claudecode: невалидная строка control_request, пропускаю", "error", err)
			continue
		}
		if req.Request.Subtype != "can_use_tool" {
			continue
		}
		p.handleControlRequest(req)
	}
	return scanner.Err()
}

// handleControlRequest — бизнес-ядро тикета 4.5 (FR C3, FR F3,
// docs/User_stories_Gherkin.md §5): решает, отвечать CLI немедленно
// (allowlist) или уйти на согласование пользователя.
func (p *Provider) handleControlRequest(req controlRequestLine) {
	toolName := req.Request.ToolName
	command := extractCommand(req.Request.Input)

	if p.allowChecker.Allowed(toolName, command) {
		if err := p.respondAllow(req.RequestID, req.Request.Input); err != nil {
			p.logger.Warn("claudecode: не удалось ответить allow на allowlist-команду",
				"request_id", req.RequestID, "error", err)
		}
		return
	}

	p.pendingMu.Lock()
	p.pending[req.RequestID] = pendingRequest{toolName: toolName, input: req.Request.Input}
	p.pendingMu.Unlock()

	if err := p.publishApprovalRequest(req.RequestID, toolName, command); err != nil {
		p.logger.Warn("claudecode: не удалось опубликовать command_approval_request",
			"request_id", req.RequestID, "error", err)
	}
}

// extractCommand достаёт поле "command" из input инструмента, если оно там
// есть (Bash-инструмент, см. docs/MVP_TICKETS.md 4.5); для инструментов без
// такого поля возвращает исходный input как строку JSON — allowlist/причина
// согласования в любом случае должны показать пользователю ЧТО именно
// просят разрешить, даже если это не Bash.
func extractCommand(input json.RawMessage) string {
	var withCommand struct {
		Command string `json:"command"`
	}
	if err := json.Unmarshal(input, &withCommand); err == nil && withCommand.Command != "" {
		return withCommand.Command
	}
	return string(input)
}

// publishApprovalRequest публикует событие command_approval_request (FR F3,
// protocol.md §4) через Publisher — конверт строится по тому же образцу,
// что sendHeartbeat в agent/main.go (bus.NewMessageID, RFC3339 UTC,
// bus.ProtocolVersion), но с непустым TaskID (событие task-scoped).
func (p *Provider) publishApprovalRequest(requestID, toolName, command string) error {
	payload, err := json.Marshal(bus.CommandApprovalRequestPayload{
		RequestID: requestID,
		Command:   command,
		Reason:    fmt.Sprintf("инструмент %q вне allowlist требует согласования", toolName),
	})
	if err != nil {
		return fmt.Errorf("маршалинг CommandApprovalRequestPayload: %w", err)
	}

	taskID := p.cfg.TaskID
	env := bus.Envelope{
		MessageID:       bus.NewMessageID(),
		TaskID:          &taskID,
		IntegrationID:   p.cfg.IntegrationID,
		Type:            bus.MessageTypeCommandApprovalRequest,
		Ts:              time.Now().UTC().Format(time.RFC3339),
		ProtocolVersion: bus.ProtocolVersion,
		Payload:         payload,
	}
	return p.cfg.Publisher.Enqueue(env)
}

// Approve разрешает ранее опубликованный control_request по его requestID
// (FR F3): decision — "approve" или "reject" (тот же словарь, что
// bus.CommandDecisionPayload.Decision, protocol.md §4). Пишет
// соответствующий control_response в stdin подпроцесса, разблокируя его
// выполнение ИМЕННО этого вызова инструмента.
//
// Неизвестный/уже разрешённый requestID — ErrUnknownRequest (см. её
// godoc — штатная ситуация at-least-once, не паника). Decision, отличный от
// "approve"/"reject" — ErrInvalidDecision.
func (p *Provider) Approve(requestID, decision string) error {
	if decision != "approve" && decision != "reject" {
		return ErrInvalidDecision
	}

	p.pendingMu.Lock()
	pending, ok := p.pending[requestID]
	if ok {
		delete(p.pending, requestID)
	}
	p.pendingMu.Unlock()

	if !ok {
		return ErrUnknownRequest
	}

	if decision == "approve" {
		return p.respondAllow(requestID, pending.input)
	}
	return p.respondDeny(requestID, "команда отклонена пользователем")
}

// respondAllow пишет control_response{behavior:"allow"} в stdin подпроцесса,
// эхом возвращая исходный input инструмента (CLI ожидает updatedInput даже
// без изменений, см. wire.go).
func (p *Provider) respondAllow(requestID string, input json.RawMessage) error {
	return p.writeLine(newAllowResponse(requestID, input))
}

// respondDeny пишет control_response{behavior:"deny"} в stdin подпроцесса с
// человекочитаемым сообщением-причиной.
func (p *Provider) respondDeny(requestID, message string) error {
	return p.writeLine(newDenyResponse(requestID, message))
}

// writeLine сериализует v в JSON и пишет одной NDJSON-строкой в stdin
// подпроцесса под mu — тот же мьютекс защищает и немедленный allow из
// readLoop (Run), и allow/deny из Approve (вызывается из другой горутины,
// см. godoc пакета), не допуская гонки/перемежающихся строк.
func (p *Provider) writeLine(v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("маршалинг строки stdin: %w", err)
	}
	data = append(data, '\n')

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stdin == nil {
		return errors.New("claudecode: Run ещё не запущен (stdin отсутствует)")
	}
	if _, err := p.stdin.Write(data); err != nil {
		return fmt.Errorf("запись в stdin подпроцесса: %w", err)
	}
	return nil
}

// Close принудительно останавливает подпроцесс (если он запущен),
// закрывая stdin и посылая ему SIGKILL (см. godoc пакета про задел под
// тикет 8.4 "отмена долетает до машины"). Безопасен к повторному вызову и к
// вызову до Run (no-op). Не ждёт завершения подпроцесса — это делает сам
// Run (единственный владелец cmd.Wait, см. её godoc), Close лишь
// инициирует остановку, чтобы Run быстрее вернул управление.
func (p *Provider) Close() error {
	p.mu.Lock()
	if p.stdin != nil {
		_ = p.stdin.Close()
	}
	cmd := p.cmd
	p.mu.Unlock()

	if cmd == nil || cmd.Process == nil {
		return nil
	}
	if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return fmt.Errorf("claudecode: остановка подпроцесса: %w", err)
	}
	return nil
}
