// agent/task_runner.go — тикет 5.4 (deps: 5.3, 3.4, 4.5).
//
// Назначение (бизнес): «Доставка на машину» (FR E1, docs/User_stories_Gherkin.md
// §4) — оркестратор доставил команду type=task_assigned через мост
// machine.commands (тикет 3.4), wsclient (тикет 3.3, тикет 5.4-Б) принял её и
// вызвал OnTaskAssigned; здесь агент решает, ЧТО делать с текстом задачи:
// выбирает провайдера (пока единственный доступный в MVP — Claude Code,
// тикет 4.5; "claude"/Anthropic API — тикет 9.7, не реализован), запускает его
// подпроцессом в отдельной горутине (сама задача может выполняться долго —
// минуты; hook обязан вернуться быстро, см. годок wsclient.Config.OnTaskAssigned)
// и подтверждает приём оркестратору событием task_accepted — именно оно
// переводит задачу queued→running на стороне оркестратора
// (orchestrator/internal/api/machine_ws.go, handleTaskAccepted).
//
// Границы (осознанно НЕ входит в этот тикет):
//   - выбор МЕЖДУ несколькими провайдерами по данным задачи — вне MVP,
//     провайдер один на агента (весь список cfg.Providers) и определяется
//     конфигурацией машины, а не самой задачей;
//   - обработка ошибок выполнения задачи как отдельный протокольный путь
//     (machine.events type=error) — тикет 5.8;
//   - параллельное выполнение нескольких задач одним агентом — не запрошено,
//     MVP предполагает последовательное исполнение (одна активная задача на
//     машину за раз; дедуп ниже — по task_id заново доставленного
//     task_assigned, а не механизм многозадачности).
//
// Как устроено (тех): taskAcceptor хранит множество task_id уже запущенных,
// но ещё не завершившихся задач (active, под мьютексом) — обязательная
// дедупликация: at-least-once доставка (durable outbox моста/агента) может
// прислать один и тот же task_assigned дважды (например, ack потерялся), и
// повторный запуск ВТОРОГО подпроцесса для той же задачи был бы явно неверным
// поведением. Для уже активного task_id — Run НЕ перезапускается, но
// task_accepted всё равно ставится в outbox повторно (идемпотентно:
// оркестраторный Transition либо применится, либо будет отклонён FSM как
// недопустимый переход — task.go, fsm.go — оба варианта штатны, см. годок
// handleTaskAccepted). Запись в outbox (sendTaskAccepted) выполняется
// СИНХРОННО, ДО возврата из onTaskAssigned — так wsclient успевает отправить
// ack на САМ кадр task_assigned сразу же, не дожидаясь завершения задачи
// (см. годок OnTaskAssigned); собственно выполнение (runTask) идёт в фоновой
// горутине на переданном ctx (тот же ctx, что и у сессии wsclient — отмена
// при остановке агента долетает и до подпроцесса, см. claudecode.Provider.Run).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/yarabey/agentify/agent/internal/provider/claudecode"
	"github.com/yarabey/agentify/internal/bus"
)

// taskRunner — узкий интерфейс исполнения одной задачи провайдером: только
// то, что нужно taskAcceptor (Run). *claudecode.Provider удовлетворяет ему
// структурно, без адаптера (тот же приём сужения интерфейса, что и
// eventSender/Outbox/Querier в других частях проекта) — это позволяет
// TestTaskAcceptor_* подменить провайдера фейком через newProvider, не
// запуская реальный подпроцесс `claude`.
type taskRunner interface {
	// Run выполняет задачу с текстом text и возвращает управление по
	// завершении (успешном или нет) либо по отмене ctx.
	Run(ctx context.Context, text string) error
}

// newProvider — фабрика taskRunner, используемая onTaskAssigned. Отдельная
// переменная (а не прямой вызов claudecode.New) — точка подмены в тестах
// (см. годок taskRunner): в проде остаётся claudecode.New без изменений.
var newProvider = func(cfg claudecode.Config) (taskRunner, error) {
	return claudecode.New(cfg)
}

// errNoProvider возвращает onTaskAssigned/buildRunner, если для машины не
// сконфигурирован ни один поддерживаемый провайдер (MVP: только
// "claude-code", см. hasClaudeCodeConfigured). Ack на task_assigned в этом
// случае НЕ отправляется — агент получит редоставку кадра, пока оператор не
// поправит конфигурацию (AGENT_PROVIDERS/AGENT_CLAUDE_CODE_API_KEY);
// отдельный канал сообщить оркестратору «провайдер не настроен» — вне
// объёма 5.4 (обработка ошибок — тикет 5.8).
var errNoProvider = errors.New("agent: нет доступного провайдера для задачи")

// taskAcceptor реализует wsclient.Config.OnTaskAssigned (тикет 5.4): принимает
// команду task_assigned, запускает провайдера и подтверждает приём
// оркестратору событием task_accepted (см. годок файла).
type taskAcceptor struct {
	cfg config

	// sender — куда ставить event task_accepted (SendEvent, тот же durable
	// путь, что и heartbeat, см. eventSender). Заполняется ПОСЛЕ
	// конструирования wsclient.Client в run() — на момент создания
	// taskAcceptor самого клиента ещё нет (его конструктору нужен уже готовый
	// OnTaskAssigned), см. run().
	sender eventSender

	// publisher — Publisher для claudecode.Config (command_approval_request,
	// тикет 6.3/4.5) — это durable outbox.Store, а не sender: провайдеру
	// нужен именно Enqueue, а не SendEvent (см. claudecode.Publisher).
	publisher claudecode.Publisher

	logger *slog.Logger

	// allowChecker — конфигурируемый allowlist команд CLI (тикет 6.3, FR F3):
	// строится один раз в newTaskAcceptor из cfg.AllowlistPatterns
	// (AGENT_ALLOWLIST_PATTERNS) и передаётся в каждый claudecode.Config в
	// buildRunner. Пустой AllowlistPatterns → пустой PatternAllowChecker,
	// ведёт себя как claudecode.EmptyAllowChecker (docs/MANUAL_STEPS.md,
	// строка 33).
	allowChecker claudecode.AllowChecker

	// mu защищает active — доступ конкурентный: onTaskAssigned вызывается
	// синхронно из read-loop wsclient, а runTask (в отдельной горутине)
	// удаляет task_id по завершении.
	mu     sync.Mutex
	active map[string]struct{}
}

// newTaskAcceptor собирает taskAcceptor с пустым множеством активных задач.
// sender заполняется отдельно, после конструирования wsclient.Client (см.
// run() и годок поля sender). Возвращает ошибку, если cfg.AllowlistPatterns
// (AGENT_ALLOWLIST_PATTERNS) содержит невалидный паттерн allowlist (тикет
// 6.3, см. claudecode.NewPatternAllowChecker) — агент не должен молча
// стартовать с частично разобранной/проигнорированной конфигурацией
// allowlist.
func newTaskAcceptor(cfg config, publisher claudecode.Publisher, logger *slog.Logger) (*taskAcceptor, error) {
	allowChecker, err := claudecode.NewPatternAllowChecker(cfg.AllowlistPatterns)
	if err != nil {
		return nil, fmt.Errorf("agent: сконструировать allowlist: %w", err)
	}
	return &taskAcceptor{
		cfg:          cfg,
		publisher:    publisher,
		logger:       logger,
		allowChecker: allowChecker,
		active:       make(map[string]struct{}),
	}, nil
}

// onTaskAssigned — реализация wsclient.Config.OnTaskAssigned (см. её годок:
// обязана вернуться быстро и передать ошибку, ТОЛЬКО если задачу не удалось
// даже начать). Дедуп по task_id — см. годок файла; для НЕдубликата: разбирает
// payload, выбирает провайдера (buildRunner), при успехе запускает Run в
// отдельной горутине (runTask) и СИНХРОННО (до возврата) ставит task_accepted
// в outbox (sendTaskAccepted) — именно поэтому wsclient успевает отправить
// ack на сам кадр task_assigned, не дожидаясь завершения задачи.
func (a *taskAcceptor) onTaskAssigned(ctx context.Context, env bus.Envelope) error {
	if env.TaskID == nil || *env.TaskID == "" {
		return errors.New("agent: task_assigned без task_id")
	}
	taskID := *env.TaskID

	a.mu.Lock()
	_, dup := a.active[taskID]
	a.mu.Unlock()

	if dup {
		// Повторная доставка уже принятой задачи (например, потерян
		// предыдущий ack task_assigned) — второй подпроцесс НЕ запускаем, но
		// task_accepted всё равно шлём заново: идемпотентно для
		// оркестратора (см. годок файла).
		a.logger.Info("agent: повторная доставка task_assigned уже выполняющейся задачи — Run не перезапускается",
			slog.String("task_id", taskID))
		a.sendTaskAccepted(ctx, env.TaskID)
		return nil
	}

	var payload bus.TaskAssignedPayload
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		return fmt.Errorf("agent: разобрать payload task_assigned: %w", err)
	}

	runner, err := a.buildRunner(taskID)
	if err != nil {
		return err
	}

	a.mu.Lock()
	a.active[taskID] = struct{}{}
	a.mu.Unlock()

	go a.runTask(ctx, runner, taskID, payload.Text)

	a.sendTaskAccepted(ctx, env.TaskID)
	return nil
}

// hasClaudeCodeConfigured — единственная поддерживаемая в MVP проверка
// доступности провайдера (тикет 4.5): "claude-code" должен быть заявлен в
// AGENT_PROVIDERS И для него должен быть задан AGENT_CLAUDE_CODE_API_KEY.
// "claude" (Anthropic API, тикет 9.7) ещё не реализован — намеренно не
// строим здесь абстракцию выбора между несколькими провайдерами, см. годок
// файла.
func (a *taskAcceptor) hasClaudeCodeConfigured() bool {
	if a.cfg.ClaudeCodeAPIKey == "" {
		return false
	}
	for _, p := range a.cfg.Providers {
		if p == "claude-code" {
			return true
		}
	}
	return false
}

// buildRunner выбирает и конструирует провайдера для задачи taskID.
// Возвращает errNoProvider, если "claude-code" не сконфигурирован (см. годок
// errNoProvider) — вызывающий (onTaskAssigned) в этом случае возвращает
// ошибку без ack, ack НЕ отправляется.
func (a *taskAcceptor) buildRunner(taskID string) (taskRunner, error) {
	if !a.hasClaudeCodeConfigured() {
		return nil, errNoProvider
	}
	runner, err := newProvider(claudecode.Config{
		Publisher:     a.publisher,
		TaskID:        taskID,
		IntegrationID: a.cfg.IntegrationUUID,
		Env:           []string{"CLAUDE_CODE_API_KEY=" + a.cfg.ClaudeCodeAPIKey},
		Logger:        a.logger,
		AllowChecker:  a.allowChecker,
	})
	if err != nil {
		return nil, fmt.Errorf("agent: сконструировать провайдера claude-code: %w", err)
	}
	return runner, nil
}

// runTask выполняет задачу провайдером в отдельной горутине (см. годок
// файла) и по завершении (успешном или нет) убирает task_id из активного
// множества (дедуп-очистка) — сбой провайдера НЕ роняет агент, это отдельная
// задача, а не сам демон (тикет 5.8 — обработка ошибок как протокольное
// событие, вне объёма 5.4).
func (a *taskAcceptor) runTask(ctx context.Context, runner taskRunner, taskID, text string) {
	defer func() {
		a.mu.Lock()
		delete(a.active, taskID)
		a.mu.Unlock()
	}()

	if err := runner.Run(ctx, text); err != nil {
		a.logger.Warn("agent: провайдер завершил задачу с ошибкой", slog.String("task_id", taskID), slog.String("error", err.Error()))
		return
	}
	a.logger.Info("agent: провайдер успешно выполнил задачу", slog.String("task_id", taskID))
}

// sendTaskAccepted ставит событие task_accepted в durable outbox через
// sender.SendEvent (см. годок поля sender). Ошибку SendEvent (сбой самой
// durable-записи, см. годок sendHeartbeat — тот же принцип) не пробрасывает
// выше: задача уже успешно начата/добавлена в дедуп-множество, потеря именно
// ЭТОГО события task_accepted — не повод откатывать её приём, а простого
// способа отличить эту ошибку от гонки нет; повторная доставка
// task_assigned (см. дедуп выше) даст ещё одну попытку.
func (a *taskAcceptor) sendTaskAccepted(ctx context.Context, taskID *string) {
	env := bus.Envelope{
		MessageID:       bus.NewMessageID(),
		TaskID:          taskID,
		IntegrationID:   a.cfg.IntegrationUUID,
		Type:            bus.MessageTypeTaskAccepted,
		Ts:              time.Now().UTC().Format(time.RFC3339),
		ProtocolVersion: bus.ProtocolVersion,
		Payload:         json.RawMessage("{}"),
	}
	if err := a.sender.SendEvent(ctx, env); err != nil {
		a.logger.Warn("agent: не удалось поставить task_accepted в outbox", slog.String("task_id", *taskID), slog.String("error", err.Error()))
	}
}
