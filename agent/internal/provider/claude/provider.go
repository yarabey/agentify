// Package claude — второй провайдер задач агента (тикет 9.7, deps: 4.5) —
// прямые HTTPS-вызовы Anthropic Messages API (POST /v1/messages), в отличие
// от claudecode (тикет 4.5), который поручает задачу подпроцессу CLI
// `claude`. Приёмка тикета: FR C3 (согласование команд вне allowlist), FR J1
// (второй провайдер доступен агенту как полноценная альтернатива).
//
// Как устроено (тех): Provider реализует агентский цикл вручную, поверх
// стандартной библиотеки (net/http + encoding/json, без стороннего SDK —
// намеренное ограничение тикета: не добавлять зависимость в go.mod). Задача
// ставится единственным пользовательским сообщением; модели предоставляется
// один инструмент — bash (см. wire.go: bashTool, systemPrompt). Каждый шаг
// цикла: (1) POST /v1/messages с текущей историей messages и объявленным
// инструментом bash; (2) разбор content (блоки text/tool_use) и stop_reason
// ответа; (3) для КАЖДОГО tool_use блока (обычно один за шаг, но обрабатываем
// список) — allowlist-проверка через AllowChecker.Allowed("bash", command):
// разрешено — выполняется немедленно (exec.CommandContext); не разрешено —
// публикуется command_approval_request (тот же Publisher/протокол, что и
// claudecode.Provider.publishApprovalRequest) и Run блокируется до внешнего
// вызова Approve с этим конкретным tool_use id; (4) после обработки всех
// tool_use блоков шага — в историю messages добавляется assistant-сообщение
// (эхо полученного content) и user-сообщение с массивом tool_result блоков,
// цикл продолжается, пока stop_reason == "tool_use"; как только модель
// отвечает без tool_use (обычно stop_reason == "end_turn") — Run завершается
// nil, задача считается провайдером выполненной.
//
// Границы (осознанно НЕ входят в этот тикет, тот же принцип, что и в
// claudecode, см. её годок пакета): построение agent_progress/agent_completed/
// error из ответов модели — отдельная, более широкая граница, разделяемая
// ОБОИМИ провайдерами, не закрывается здесь.
//
// AllowChecker и Publisher намеренно переиспользуются из пакета claudecode
// (claudecode.AllowChecker, claudecode.Publisher) — это те же самые узкие
// интерфейсы, что и у первого провайдера (Allowed(toolName, command string)
// bool; Enqueue(env bus.Envelope) error): agent/task_runner.go уже строит
// ОДИН allowChecker и ОДИН publisher на taskAcceptor и передаёт их в оба
// провайдера без адаптера — дублировать эти интерфейсы в новом пакете было
// бы избыточно.
//
// Безопасность при отмене (Close, тикет 8.5, FR E6) реализована для ЭТОГО
// провайдера так же строго, как и для claudecode.Provider.Close: если в
// момент вызова Close выполняется РАЗРЕШЁННЫЙ (allowlist или approve), ещё не
// завершившийся exec.Command — немедленной отмены не происходит, отмена
// откладывается до естественного завершения этой команды (см. поля
// criticalInFlight/cancelRequested и executeCommand). Во всех остальных
// случаях (ожидание ответа HTTP, ожидание Approve пользователя) — Close
// отменяет внутренний context немедленно.
package claude

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/yarabey/agentify/agent/internal/provider/claudecode"
	"github.com/yarabey/agentify/internal/bus"
)

// Значения по умолчанию Config (см. New).
const (
	// defaultBaseURL — базовый URL Anthropic API, если Config.BaseURL пуст.
	defaultBaseURL = "https://api.anthropic.com"
	// defaultModel — модель по умолчанию, если Config.Model пуст (актуальный
	// на момент реализации тикета ID модели семейства Claude Sonnet).
	defaultModel = "claude-sonnet-4-6"
	// anthropicVersion — обязательный заголовок anthropic-version (формат
	// запроса/ответа Messages API зафиксирован этой версией).
	anthropicVersion = "2023-06-01"
	// defaultMaxTokens — верхняя граница длины ОДНОГО ответа модели за шаг
	// цикла (max_tokens запроса) — с запасом для текстового ответа и/или
	// одного-нескольких tool_use блоков.
	defaultMaxTokens = 4096
	// maxOutputBytes — верхняя граница длины текста tool_result (объединённые
	// stdout+stderr выполненной команды) — защита от того, чтобы
	// многогигабайтный вывод команды не улетел в API целиком.
	maxOutputBytes = 8000
	// defaultHTTPTimeout — таймаут http.Client по умолчанию, если
	// Config.HTTPClient пуст.
	defaultHTTPTimeout = 5 * time.Minute
)

// Sentinel-ошибки Config, возвращаемые New — та же дисциплина, что
// claudecode.New (конструктор не паникует на невалидном конфиге).
var (
	// ErrEmptyAPIKey — New вызван с пустым Config.APIKey.
	ErrEmptyAPIKey = errors.New("claude: пустой APIKey")
	// ErrNilPublisher — New вызван с Config.Publisher == nil.
	ErrNilPublisher = errors.New("claude: nil Publisher")
	// ErrEmptyTaskID — New вызван с пустым Config.TaskID.
	ErrEmptyTaskID = errors.New("claude: пустой TaskID")
	// ErrEmptyIntegrationID — New вызван с пустым Config.IntegrationID.
	ErrEmptyIntegrationID = errors.New("claude: пустой IntegrationID")
)

// Sentinel-ошибки Approve — см. годок claudecode.ErrUnknownRequest/
// ErrInvalidDecision: тот же смысл, отдельные переменные в этом пакете.
var (
	// ErrUnknownRequest возвращает Approve, если requestID не найден среди
	// ожидающих решения tool_use (никогда не существовал, уже разрешён
	// предыдущим вызовом Approve либо относится к уже завершившемуся Run).
	ErrUnknownRequest = errors.New("claude: неизвестный или уже разрешённый request_id")
	// ErrInvalidDecision возвращает Approve при decision, отличном от
	// "approve" и "reject".
	ErrInvalidDecision = errors.New(`claude: decision должен быть "approve" или "reject"`)
)

// Config — параметры одного Provider (одна задача, один цикл запросов к
// Anthropic API, см. годок пакета).
type Config struct {
	// APIKey — ключ Anthropic API (заголовок x-api-key). Обязателен.
	APIKey string
	// BaseURL — базовый URL Messages API. Пусто → defaultBaseURL. Отдельное
	// поле нужно в первую очередь тестам (httptest.Server), в проде не
	// заполняется.
	BaseURL string
	// Model — ID модели. Пусто → defaultModel.
	Model string
	// WorkDir — рабочая директория, в которой выполняются команды bash
	// (exec.CommandContext, см. executeCommand). Пусто → наследуется текущая
	// рабочая директория процесса агента (os/exec по умолчанию).
	WorkDir string
	// AllowChecker — allowlist вызовов bash (см. claudecode.AllowChecker).
	// Пусто (nil) → New подставляет claudecode.EmptyAllowChecker{} (безопасный
	// дефолт — согласования требует каждая команда).
	AllowChecker claudecode.AllowChecker
	// Publisher — приёмник событий command_approval_request (см.
	// claudecode.Publisher). Обязателен.
	Publisher claudecode.Publisher
	// TaskID — идентификатор задачи; входит в каждый публикуемый конверт.
	// Обязателен.
	TaskID string
	// IntegrationID — идентификатор интеграции/машины; входит в каждый
	// публикуемый конверт. Обязателен.
	IntegrationID string
	// Logger — логгер Provider. Пусто (nil) → New подставляет slog.Default().
	Logger *slog.Logger
	// HTTPClient — HTTP-клиент для вызовов Anthropic API. Пусто (nil) → New
	// подставляет &http.Client{Timeout: defaultHTTPTimeout}. Поле оставлено
	// для гибкости тестов, хотя обычно достаточно переопределить BaseURL на
	// httptest.Server.
	HTTPClient *http.Client
}

// New собирает Provider, валидируя Config (см. поля Config и sentinel-ошибки
// выше). Не паникует на невалидном конфиге — та же дисциплина, что
// claudecode.New.
func New(cfg Config) (*Provider, error) {
	if cfg.APIKey == "" {
		return nil, ErrEmptyAPIKey
	}
	if cfg.Publisher == nil {
		return nil, ErrNilPublisher
	}
	if cfg.TaskID == "" {
		return nil, ErrEmptyTaskID
	}
	if cfg.IntegrationID == "" {
		return nil, ErrEmptyIntegrationID
	}
	if cfg.BaseURL == "" {
		cfg.BaseURL = defaultBaseURL
	}
	if cfg.Model == "" {
		cfg.Model = defaultModel
	}

	allowChecker := cfg.AllowChecker
	if allowChecker == nil {
		allowChecker = claudecode.EmptyAllowChecker{}
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultHTTPTimeout}
	}

	return &Provider{
		cfg:          cfg,
		allowChecker: allowChecker,
		logger:       logger,
		httpClient:   httpClient,
		pending:      make(map[string]chan string),
	}, nil
}

// Provider — один запуск одной задачи через Anthropic Messages API (см.
// годок пакета). Собирается New; готов к однократному Run. Повторное
// использование одного Provider для нескольких задач не предусмотрено (тот
// же контракт, что claudecode.Provider).
type Provider struct {
	cfg          Config
	allowChecker claudecode.AllowChecker
	logger       *slog.Logger
	httpClient   *http.Client

	// pendingMu защищает pending — map requestID (tool_use id) → канал
	// решения, которым Approve разблокирует ожидающую горутину Run (см.
	// handleToolUse/Approve).
	pendingMu sync.Mutex
	pending   map[string]chan string

	// mu защищает поля ниже — критическую секцию "запрос на отмену
	// (Close) против выполняющейся разрешённой команды" (тикет 8.5, FR E6,
	// тот же принцип, что claudecode.Provider.criticalInFlight/
	// cancelRequested — см. годок Close).
	mu sync.Mutex
	// cancel — CancelFunc внутреннего context, производного от ctx,
	// переданного в Run (см. Run). Close вызывает его для немедленной отмены,
	// когда ничего критического не выполняется.
	cancel context.CancelFunc
	// criticalInFlight — true, пока выполняется exec.Command разрешённой
	// команды bash (allowlist ИЛИ approve пользователем), ещё не
	// завершившийся. "Критическая операция" — та же самая MVP-трактовка, что
	// и в claudecode (см. её годок): любой разрешённый, ещё не завершившийся
	// вызов инструмента, без классификации на деструктивные/безопасные.
	criticalInFlight bool
	// cancelRequested — Close вызван, пока criticalInFlight==true: реальная
	// отмена (вызов cancel) откладывается до момента, когда текущая команда
	// сама завершится естественным образом (см. executeCommand) — то же
	// самое FR E6 "мгновенная остановка не гарантируется", что и в
	// claudecode.Provider.Close.
	cancelRequested bool
}

// Run отправляет задачу taskText модели и обрабатывает агентский цикл
// (см. годок пакета) до тех пор, пока модель не ответит без вызова
// инструмента, либо пока ctx не будет отменён (в т.ч. отменён косвенно через
// Close, см. её годок) — в этом случае Run возвращает ctx.Err().
//
// Run владеет своим внутренним циклом единолично: конкурентно с ним
// допустимо звать ТОЛЬКО Approve (разрешение ранее опубликованных
// command_approval_request) и Close (принудительная остановка). Повторный
// вызов Run на одном Provider не поддерживается.
func (p *Provider) Run(ctx context.Context, taskText string) error {
	runCtx, cancel := context.WithCancel(ctx)
	p.mu.Lock()
	p.cancel = cancel
	p.mu.Unlock()
	defer cancel()

	messages := []apiMessage{userTextMessage(taskText)}

	for {
		resp, err := p.sendRequest(runCtx, messages)
		if err != nil {
			if ctxErr := runCtx.Err(); ctxErr != nil {
				return ctxErr
			}
			return err
		}

		var toolUses []contentBlock
		for _, block := range resp.Content {
			if block.Type == "tool_use" {
				toolUses = append(toolUses, block)
			}
		}

		if resp.StopReason != "tool_use" || len(toolUses) == 0 {
			// Ответ без вызова инструмента — сигнал завершения задачи (см.
			// systemPrompt в wire.go и годок пакета).
			return nil
		}

		messages = append(messages, apiMessage{Role: "assistant", Content: resp.Content})

		toolResults := make([]contentBlock, 0, len(toolUses))
		for _, tu := range toolUses {
			result, err := p.handleToolUse(runCtx, tu)
			if err != nil {
				return err
			}
			toolResults = append(toolResults, result)
		}
		messages = append(messages, apiMessage{Role: "user", Content: toolResults})
	}
}

// handleToolUse обрабатывает один tool_use блок: allowlist-разрешённые
// команды выполняются немедленно; прочие уходят на согласование пользователя
// (публикация command_approval_request + блокирующее ожидание Approve).
// Возвращает ненулевую ошибку ТОЛЬКО если ctx отменён, пока Run ждал решение
// пользователя или выполнение команды — в этом случае вызывающий (Run) обязан
// немедленно вернуть эту ошибку, не продолжая цикл (см. годок Run про
// ctx.Err()).
func (p *Provider) handleToolUse(ctx context.Context, tu contentBlock) (contentBlock, error) {
	command := extractCommand(tu.Input)

	if p.allowChecker.Allowed("bash", command) {
		output, cmdErr := p.executeCommand(ctx, command)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return contentBlock{}, ctxErr
		}
		return toolResultBlock(tu.ID, formatResult(output, cmdErr), cmdErr != nil), nil
	}

	decisionCh := make(chan string, 1)
	p.pendingMu.Lock()
	p.pending[tu.ID] = decisionCh
	p.pendingMu.Unlock()

	if err := p.publishApprovalRequest(tu.ID, command); err != nil {
		p.logger.Warn("claude: не удалось опубликовать command_approval_request",
			"request_id", tu.ID, "error", err)
	}

	var decision string
	select {
	case decision = <-decisionCh:
	case <-ctx.Done():
		p.pendingMu.Lock()
		delete(p.pending, tu.ID)
		p.pendingMu.Unlock()
		return contentBlock{}, ctx.Err()
	}

	if decision == "reject" {
		return toolResultBlock(tu.ID, "команда отклонена пользователем", true), nil
	}

	output, cmdErr := p.executeCommand(ctx, command)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return contentBlock{}, ctxErr
	}
	return toolResultBlock(tu.ID, formatResult(output, cmdErr), cmdErr != nil), nil
}

// executeCommand выполняет command через `sh -c` в Config.WorkDir (тикет 9.7:
// "реальное выполнение команды"), помечая её как критическую операцию на всё
// время выполнения (см. годок поля criticalInFlight — тикет 8.5, FR E6): пока
// exec.Command не вернул управление, Close не отменит runCtx немедленно (см.
// Close), а лишь взведёт cancelRequested — как только команда естественным
// образом завершится (эта функция), отложенная отмена доводится до конца
// здесь же.
func (p *Provider) executeCommand(ctx context.Context, command string) (string, error) {
	p.mu.Lock()
	p.criticalInFlight = true
	p.mu.Unlock()

	cmd := exec.CommandContext(ctx, "sh", "-c", command)
	cmd.Dir = p.cfg.WorkDir
	out, cmdErr := cmd.CombinedOutput()

	p.mu.Lock()
	p.criticalInFlight = false
	deferredCancel := p.cancelRequested
	cancel := p.cancel
	p.mu.Unlock()

	if deferredCancel && cancel != nil {
		cancel()
	}

	return truncateOutput(out), cmdErr
}

// truncateOutput обрезает вывод команды до maxOutputBytes с явной пометкой
// обрезки — реалистичный размер, безопасный для отправки в API, при этом не
// теряющий диагностическую ценность для типичного вывода команд.
func truncateOutput(out []byte) string {
	if len(out) <= maxOutputBytes {
		return string(out)
	}
	return string(out[:maxOutputBytes]) + fmt.Sprintf("\n...[вывод обрезан, превышен лимит %d байт]", maxOutputBytes)
}

// formatResult объединяет вывод команды с текстом ошибки её выполнения (код
// возврата != 0, команда не найдена и т.п.), если она была — модель должна
// увидеть и то, и другое в tool_result.
func formatResult(output string, cmdErr error) string {
	if cmdErr == nil {
		return output
	}
	if output == "" {
		return fmt.Sprintf("ошибка выполнения команды: %v", cmdErr)
	}
	return output + fmt.Sprintf("\n[ошибка выполнения команды: %v]", cmdErr)
}

// publishApprovalRequest публикует событие command_approval_request через
// Publisher — конверт строится по тому же образцу, что
// claudecode.Provider.publishApprovalRequest (bus.NewMessageID, RFC3339 UTC,
// bus.ProtocolVersion, непустой TaskID — событие task-scoped).
func (p *Provider) publishApprovalRequest(requestID, command string) error {
	payload, err := json.Marshal(bus.CommandApprovalRequestPayload{
		RequestID: requestID,
		Command:   command,
		Reason:    `инструмент "bash" вне allowlist требует согласования`,
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

// Approve разрешает ранее опубликованный tool_use по его requestID (id
// tool_use блока, эхом дошедший в command_approval_request.RequestID): decision
// — "approve" или "reject" (тот же словарь, что claudecode.Provider.Approve).
// Разблокирует горутину Run, ожидающую решение именно этого requestID (см.
// handleToolUse).
//
// Неизвестный/уже разрешённый requestID — ErrUnknownRequest (штатная
// ситуация at-least-once, не паника). Decision, отличный от
// "approve"/"reject" — ErrInvalidDecision.
func (p *Provider) Approve(requestID, decision string) error {
	if decision != "approve" && decision != "reject" {
		return ErrInvalidDecision
	}

	p.pendingMu.Lock()
	ch, ok := p.pending[requestID]
	if ok {
		delete(p.pending, requestID)
	}
	p.pendingMu.Unlock()

	if !ok {
		return ErrUnknownRequest
	}

	// ch буферизован на 1 значение — эта отправка никогда не блокируется,
	// даже если handleToolUse уже вышла по отмене ctx (см. её select) и
	// никогда не прочитает канал.
	ch <- decision
	return nil
}

// Close останавливает Run (если он запущен) — но с приоритетом сохранности
// данных при отмене (тикет 8.5, FR E6, тот же принцип, что
// claudecode.Provider.Close): если в момент вызова выполняется разрешённая,
// ещё не завершившаяся команда (criticalInFlight), немедленной отмены НЕ
// происходит — вместо неё Close лишь помечает cancelRequested и публикует
// agent_progress-предупреждение (warnCancelDeferred); реальная отмена
// откладывается до момента, когда команда сама завершится (см.
// executeCommand). Публикация предупреждения — best-effort, тот же принцип,
// что claudecode.Provider.warnCancelDeferred: ошибка Publisher только
// логируется.
//
// Если criticalInFlight==false (нет in-flight разрешённой команды, включая
// случай ожидания согласования пользователя через pending, либо ожидания
// ответа HTTP) — Close отменяет внутренний context немедленно: любой
// заблокированный select (ожидание HTTP-ответа, ожидание Approve) видит
// ctx.Done() и Run возвращает ctx.Err().
//
// Безопасен к повторному вызову и к вызову до Run (в этом случае p.cancel
// ещё nil — no-op).
func (p *Provider) Close() error {
	p.mu.Lock()
	if p.criticalInFlight {
		p.cancelRequested = true
		p.mu.Unlock()
		p.warnCancelDeferred()
		return nil
	}
	cancel := p.cancel
	p.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	return nil
}

// warnCancelDeferred публикует agent_progress-предупреждение о том, что
// остановка отложена до безопасного завершения текущей выполняющейся команды
// (FR E6, "мгновенная остановка не гарантируется") — вызывается Close, когда
// criticalInFlight==true. Публикация best-effort: ошибка только логируется
// (тот же принцип, что publishApprovalRequest) — сама остановка уже
// поставлена в очередь (cancelRequested) независимо от того, дошло ли
// предупреждение.
func (p *Provider) warnCancelDeferred() {
	payload, err := json.Marshal(bus.AgentProgressPayload{
		Text: "отмена получена во время выполнения команды — она будет доведена до безопасного завершения, мгновенная остановка не гарантируется",
	})
	if err != nil {
		p.logger.Warn("claude: маршалинг AgentProgressPayload", "error", err)
		return
	}
	taskID := p.cfg.TaskID
	env := bus.Envelope{
		MessageID:       bus.NewMessageID(),
		TaskID:          &taskID,
		IntegrationID:   p.cfg.IntegrationID,
		Type:            bus.MessageTypeAgentProgress,
		Ts:              time.Now().UTC().Format(time.RFC3339),
		ProtocolVersion: bus.ProtocolVersion,
		Payload:         payload,
	}
	if err := p.cfg.Publisher.Enqueue(env); err != nil {
		p.logger.Warn("claude: не удалось опубликовать agent_progress", "error", err)
	}
}

// sendRequest выполняет один POST {BaseURL}/v1/messages с текущей историей
// messages и объявленным инструментом bash (см. wire.go: bashTool,
// systemPrompt), разбирая ответ в messagesResponse. Ошибка HTTP-статуса != 200
// оборачивается человекочитаемым сообщением (тело ответа — apiErrorResponse).
func (p *Provider) sendRequest(ctx context.Context, messages []apiMessage) (*messagesResponse, error) {
	reqBody := messagesRequest{
		Model:     p.cfg.Model,
		MaxTokens: defaultMaxTokens,
		System:    systemPrompt,
		Messages:  messages,
		Tools:     []toolDef{bashTool()},
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("claude: маршалинг запроса Messages API: %w", err)
	}

	url := strings.TrimSuffix(p.cfg.BaseURL, "/") + "/v1/messages"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("claude: построение HTTP-запроса: %w", err)
	}
	httpReq.Header.Set("x-api-key", p.cfg.APIKey)
	httpReq.Header.Set("anthropic-version", anthropicVersion)
	httpReq.Header.Set("content-type", "application/json")

	resp, err := p.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("claude: HTTP-запрос к Anthropic API: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("claude: чтение тела ответа Anthropic API: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var apiErr apiErrorResponse
		_ = json.Unmarshal(respBody, &apiErr)
		if apiErr.Error.Message != "" {
			return nil, fmt.Errorf("claude: Anthropic API вернул %d (%s): %s", resp.StatusCode, apiErr.Error.Type, apiErr.Error.Message)
		}
		return nil, fmt.Errorf("claude: Anthropic API вернул %d: %s", resp.StatusCode, string(respBody))
	}

	var out messagesResponse
	if err := json.Unmarshal(respBody, &out); err != nil {
		return nil, fmt.Errorf("claude: демаршалинг ответа Anthropic API: %w", err)
	}
	return &out, nil
}
