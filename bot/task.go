package main

// task.go — действия из Telegram: постановка задачи, отмена, ответ на вопрос
// агента (тикет 10.3, FR D1, §4 «Постановка задачи из канала», Примеры:
// канал=telegram).
//
// Назначение (бизнес): пользователь, уже привязавший Telegram-аккаунт
// (`/start <code>`, тикет 10.2), пишет боту:
//   - обычный текст (не команда) — ставит задачу активной/единственной
//     подключённой машине (см. createTaskReplyText). Если машин несколько —
//     бот НЕ угадывает, какую выбрать: перечисляет их и просит повторить
//     явной командой `/task <id машины> <текст>` (createTaskForIntegrationReplyText) —
//     выбор конкретной машины из нескольких это отдельная UX-задача (список/
//     инлайн-кнопки в web/боте), вне объёма этого тикета; здесь — минимально
//     достаточный текстовый интерфейс, не блокирующий пользователя.
//   - `/cancel <id задачи>` — отменяет задачу (cancelTaskReplyText, FR E6,
//     тикет 8.4).
//   - `/answer <id задачи> <id вопроса> <текст ответа>` — отвечает на вопрос
//     агента (answerTaskReplyText, FR F1/F2, тикет 6.1). Доставка самого
//     вопроса пользователю В Telegram — отдельный тикет 7.3/10.4 (consumer
//     уведомлений), ЕЩЁ не реализован; этот тикет закрывает только то, что
//     пользователь, УЖЕ знающий id задачи/вопроса (например, увидел их в
//     web-истории задачи), может ответить из Telegram.
//
// Непривязанный пользователь (нет channel_links, GetActingToken вернул
// orchestrator.ErrNotLinked) получает ОДИНАКОВУЮ для всех трёх действий
// подсказку привязать аккаунт через `/start <code>` — см. actingTokenOrReplyText.
//
// Как устроено (тех): та же архитектура, что и bot/start.go — бизнес-логика
// вынесена в чистые функции (createTaskReplyText/createTaskForIntegrationReplyText/
// cancelTaskReplyText/answerTaskReplyText), не зависящие от telebot.Context,
// покрываются unit-тестами (bot/task_test.go) без реальной сети; тонкие
// telebot-обвязки (newTaskTextHandler/newTaskCommandHandler/newCancelHandler/
// newAnswerHandler) лишь достают поля из tele.Context и зовут чистую функцию.
// telegramActor — узкий интерфейс на используемые методы
// *orchestrator.Client (GetActingToken/ListIntegrations/CreateTask/AnswerTask/
// CancelTask), тот же приём, что и telegramLinker в bot/start.go — позволяет
// тестам подменить клиент фейком и остаётся корректным при nil-интерфейсе
// (BOT_ORCHESTRATOR_URL/BOT_SERVICE_SECRET не заданы, см. bot/main.go).
import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"unicode"

	tele "gopkg.in/telebot.v3"

	"github.com/yarabey/agentify/bot/internal/orchestrator"
)

// telegramActor — узкий интерфейс на методы *orchestrator.Client, нужные
// действиям из Telegram (тикет 10.3). *orchestrator.Client удовлетворяет ему
// структурно; unit-тесты (bot/task_test.go) подставляют фейк без реальной
// сети.
type telegramActor interface {
	GetActingToken(ctx context.Context, telegramUserID string) (string, error)
	ListIntegrations(ctx context.Context, accessToken string) ([]orchestrator.Integration, error)
	CreateTask(ctx context.Context, accessToken, integrationID, text, idempotencyKey string) (orchestrator.Task, error)
	AnswerTask(ctx context.Context, accessToken, taskID, questionID, text string) error
	CancelTask(ctx context.Context, accessToken, taskID string) error
}

// unlinkedAccountReplyText — общий текст для всех действий из Telegram,
// когда telegram_user_id ещё не привязан ни к одному аккаунту agentify (см.
// orchestrator.ErrNotLinked) — та же подсказка, что и у известных исходов
// `/start <code>` в bot/start.go (Gherkin §6, предусловие «Уведомление в
// Telegram»).
const unlinkedAccountReplyText = "Ваш Telegram-аккаунт не привязан к agentify. Откройте настройки в web-приложении, получите код привязки и отправьте боту `/start <code>`."

// actingTokenOrReplyText резолвит telegram_user_id отправителя апдейта в
// acting-токен (orchestrator.Client.GetActingToken, тикет 10.3) — общий
// первый шаг ВСЕХ действий из Telegram этого файла. Возвращает (token, "")
// при успехе, либо ("", готовый ответ пользователю) при любом отказе —
// вызывающая сторона (createTaskReplyText и т.п.) в этом случае просто
// возвращает второе значение, не продолжая действие.
func actingTokenOrReplyText(ctx context.Context, actor telegramActor, senderID int64, hasSender bool, logger *slog.Logger) (token, replyText string) {
	if actor == nil {
		// BOT_ORCHESTRATOR_URL/BOT_SERVICE_SECRET не заданы (см. bot/main.go)
		// — тот же принцип мягкой деградации, что у client==nil в
		// bot/start.go.
		return "", "Бот временно не настроен для действий из Telegram. Попробуйте позже."
	}
	if !hasSender {
		if logger != nil {
			logger.Warn("действие из Telegram: апдейт без Sender()")
		}
		return "", "Не удалось определить ваш Telegram-аккаунт, попробуйте ещё раз."
	}

	telegramUserID := strconv.FormatInt(senderID, 10)
	tok, err := actor.GetActingToken(ctx, telegramUserID)
	switch {
	case err == nil:
		return tok, ""
	case errors.Is(err, orchestrator.ErrNotLinked):
		return "", unlinkedAccountReplyText
	default:
		if logger != nil {
			logger.Error("actingTokenOrReplyText: GetActingToken", slog.Any("error", err))
		}
		return "", "Не удалось выполнить действие из-за внутренней ошибки. Попробуйте позже."
	}
}

// idempotencyKeyForMessage строит Idempotency-Key постановки задачи (FR E7,
// тикет 5.5, §4 «Защита от двойной отправки») из (telegram_user_id,
// message_id): Telegram webhook доставляет апдейты as at-least-once — если
// один и тот же апдейт придёт повторно (сетевой ретрай на стороне Telegram),
// ключ окажется тем же, и оркестратор вернёт УЖЕ созданную задачу вместо
// дубля (см. годок PostTasks, orchestrator/internal/api/tasks.go).
func idempotencyKeyForMessage(senderID int64, messageID int) string {
	return fmt.Sprintf("tg-%d-%d", senderID, messageID)
}

// formatIntegrationList формирует список «id (name)» через ", " — часть
// текста подсказки createTaskReplyText, когда у пользователя больше одной
// подключённой машины и нужно явно указать, какую использовать.
func formatIntegrationList(integrations []orchestrator.Integration) string {
	parts := make([]string, 0, len(integrations))
	for _, in := range integrations {
		parts = append(parts, fmt.Sprintf("%s (%s)", in.ID, in.Name))
	}
	return strings.Join(parts, ", ")
}

// doCreateTask вызывает orchestrator.Client.CreateTask и переводит её исход в
// текст ответа пользователю — общий последний шаг createTaskReplyText и
// createTaskForIntegrationReplyText.
func doCreateTask(ctx context.Context, actor telegramActor, token, integrationID, text, idempotencyKey string, logger *slog.Logger) string {
	created, err := actor.CreateTask(ctx, token, integrationID, text, idempotencyKey)
	switch {
	case err == nil:
		return fmt.Sprintf("Задача поставлена в очередь (id %s).", created.ID)
	case errors.Is(err, orchestrator.ErrNotFound):
		return "Машина не найдена или недоступна — проверьте id интеграции (/task <id машины> <текст>)."
	default:
		if logger != nil {
			logger.Error("doCreateTask: CreateTask", slog.Any("error", err))
		}
		return "Не удалось поставить задачу из-за внутренней ошибки. Попробуйте позже."
	}
}

// createTaskReplyText — бизнес-логика постановки задачи из ОБЫЧНОГО
// текстового сообщения Telegram, не команды (тикет 10.3, FR D1, §4
// «Постановка задачи из канала», Примеры: канал=telegram). Резолвит
// telegram_user_id в acting-токен, получает список подключённых машин
// пользователя (GET /integrations) и, если она ровно одна, ставит задачу
// именно ей; ноль или больше одной машины — просит пользователя уточнить
// (см. годок файла).
func createTaskReplyText(ctx context.Context, actor telegramActor, senderID int64, hasSender bool, messageID int, text string, logger *slog.Logger) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return "Пустое сообщение — нечего ставить в задачу."
	}

	token, replyText := actingTokenOrReplyText(ctx, actor, senderID, hasSender, logger)
	if replyText != "" {
		return replyText
	}

	integrations, err := actor.ListIntegrations(ctx, token)
	if err != nil {
		if logger != nil {
			logger.Error("createTaskReplyText: ListIntegrations", slog.Any("error", err))
		}
		return "Не удалось получить список подключённых машин. Попробуйте позже."
	}

	switch len(integrations) {
	case 0:
		return "У вас нет ни одной подключённой машины. Подключите интеграцию в web-приложении agentify и повторите."
	case 1:
		return doCreateTask(ctx, actor, token, integrations[0].ID, text, idempotencyKeyForMessage(senderID, messageID), logger)
	default:
		return "У вас несколько подключённых машин: " + formatIntegrationList(integrations) +
			". Укажите нужную явно: /task <id машины> <текст задачи>."
	}
}

// splitFirstToken отделяет первый пробельно-разделённый токен от остатка
// строки (остаток — с сохранёнными внутренними пробелами, в отличие от
// tele.Context.Args(), которая режет payload на отдельные слова и теряет
// пробелы внутри текста ответа/задачи — недопустимо для /answer, где текст
// ответа может быть многословным). ok=false — вход пуст после TrimSpace
// (нет вообще ни одного токена).
func splitFirstToken(s string) (first, rest string, ok bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", "", false
	}
	idx := strings.IndexFunc(s, unicode.IsSpace)
	if idx < 0 {
		return s, "", true
	}
	return s[:idx], strings.TrimSpace(s[idx+1:]), true
}

// createTaskForIntegrationReplyText — бизнес-логика `/task <id машины>
// <текст задачи>` (тикет 10.3): явный выбор машины, нужный, когда у
// пользователя несколько подключённых интеграций (см. годок
// createTaskReplyText) — тот же путь постановки задачи (doCreateTask), но
// integration_id берётся из аргумента команды, а не из единственного
// элемента списка.
func createTaskForIntegrationReplyText(ctx context.Context, actor telegramActor, senderID int64, hasSender bool, messageID int, payload string, logger *slog.Logger) string {
	integrationID, text, ok := splitFirstToken(payload)
	if !ok || strings.TrimSpace(text) == "" {
		return "Использование: /task <id машины> <текст задачи>."
	}

	token, replyText := actingTokenOrReplyText(ctx, actor, senderID, hasSender, logger)
	if replyText != "" {
		return replyText
	}
	return doCreateTask(ctx, actor, token, integrationID, text, idempotencyKeyForMessage(senderID, messageID), logger)
}

// cancelTaskReplyText — бизнес-логика `/cancel <id задачи>` (тикет 10.3, FR
// E6, тикет 8.4 «Отмена доходит до машины» — переиспользует тот же
// POST /tasks/{id}/cancel единого API, что и web).
func cancelTaskReplyText(ctx context.Context, actor telegramActor, senderID int64, hasSender bool, payload string, logger *slog.Logger) string {
	taskID, _, ok := splitFirstToken(payload)
	if !ok {
		return "Использование: /cancel <id задачи>."
	}

	token, replyText := actingTokenOrReplyText(ctx, actor, senderID, hasSender, logger)
	if replyText != "" {
		return replyText
	}

	err := actor.CancelTask(ctx, token, taskID)
	switch {
	case err == nil:
		return "Отмена задачи " + taskID + " отправлена машине."
	case errors.Is(err, orchestrator.ErrNotFound):
		return "Задача не найдена или недоступна."
	default:
		if logger != nil {
			logger.Error("cancelTaskReplyText: CancelTask", slog.Any("error", err))
		}
		return "Не удалось отменить задачу из-за внутренней ошибки. Попробуйте позже."
	}
}

// answerTaskReplyText — бизнес-логика `/answer <id задачи> <id вопроса>
// <текст ответа>` (тикет 10.3, FR F1/F2, тикет 6.1 — переиспользует тот же
// POST /tasks/{id}/answer единого API, что и web). Доставка самого вопроса
// пользователю В Telegram — отдельный тикет 7.3/10.4, ещё не реализован (см.
// годок файла); эта команда рассчитана на пользователя, который уже знает
// id задачи/вопроса (например, из web-истории задачи).
func answerTaskReplyText(ctx context.Context, actor telegramActor, senderID int64, hasSender bool, payload string, logger *slog.Logger) string {
	const usage = "Использование: /answer <id задачи> <id вопроса> <текст ответа>."

	taskID, rest, ok := splitFirstToken(payload)
	if !ok {
		return usage
	}
	questionID, text, ok := splitFirstToken(rest)
	if !ok || strings.TrimSpace(text) == "" {
		return usage
	}

	token, replyText := actingTokenOrReplyText(ctx, actor, senderID, hasSender, logger)
	if replyText != "" {
		return replyText
	}

	err := actor.AnswerTask(ctx, token, taskID, questionID, text)
	switch {
	case err == nil:
		return "Ответ отправлен агенту."
	case errors.Is(err, orchestrator.ErrNotFound):
		return "Задача или вопрос не найдены — проверьте id."
	default:
		if logger != nil {
			logger.Error("answerTaskReplyText: AnswerTask", slog.Any("error", err))
		}
		return "Не удалось отправить ответ из-за внутренней ошибки. Попробуйте позже."
	}
}

// senderIDAndMessageID достаёт (senderID, hasSender, messageID) из
// tele.Context — общая мелкая обвязка для всех четырёх telebot-хендлеров
// этого файла, вынесенная, чтобы не повторять её четыре раза.
func senderIDAndMessageID(c tele.Context) (senderID int64, hasSender bool, messageID int) {
	sender := c.Sender()
	hasSender = sender != nil
	if hasSender {
		senderID = sender.ID
	}
	if m := c.Message(); m != nil {
		messageID = m.ID
	}
	return senderID, hasSender, messageID
}

// payload достаёт "хвост" команды (после `/command `) из tele.Context.
// Ровно c.Message().Payload — НЕ c.Args() (которая режет на отдельные слова
// по пробелу и теряет структуру многословного текста, см. годок
// splitFirstToken).
func payload(c tele.Context) string {
	if m := c.Message(); m != nil {
		return m.Payload
	}
	return ""
}

// newTaskTextHandler собирает tele.HandlerFunc для tele.OnText — обычных
// текстовых сообщений (не команд), см. createTaskReplyText. Сообщения,
// начинающиеся с "/" (НЕраспознанная команда — telebot маршрутизирует такие
// в OnText, если нет зарегистрированного хендлера на конкретную команду, см.
// gopkg.in/telebot.v3 ProcessUpdate), НЕ трактуются как текст задачи — это
// была бы путающая семантика (опечатка в команде тихо стала бы текстом
// задачи); вместо этого отвечаем явной подсказкой.
func newTaskTextHandler(actor telegramActor, logger *slog.Logger) tele.HandlerFunc {
	return func(c tele.Context) error {
		text := c.Text()
		if strings.HasPrefix(strings.TrimSpace(text), "/") {
			return c.Send("Неизвестная команда. Чтобы поставить задачу, просто напишите текст. Другие команды: /task <id машины> <текст>, /cancel <id задачи>, /answer <id задачи> <id вопроса> <текст ответа>.")
		}
		senderID, hasSender, messageID := senderIDAndMessageID(c)
		return c.Send(createTaskReplyText(context.Background(), actor, senderID, hasSender, messageID, text, logger))
	}
}

// newTaskCommandHandler собирает tele.HandlerFunc для `/task <id машины>
// <текст задачи>`, см. createTaskForIntegrationReplyText.
func newTaskCommandHandler(actor telegramActor, logger *slog.Logger) tele.HandlerFunc {
	return func(c tele.Context) error {
		senderID, hasSender, messageID := senderIDAndMessageID(c)
		return c.Send(createTaskForIntegrationReplyText(context.Background(), actor, senderID, hasSender, messageID, payload(c), logger))
	}
}

// newCancelHandler собирает tele.HandlerFunc для `/cancel <id задачи>`, см.
// cancelTaskReplyText.
func newCancelHandler(actor telegramActor, logger *slog.Logger) tele.HandlerFunc {
	return func(c tele.Context) error {
		senderID, hasSender, _ := senderIDAndMessageID(c)
		return c.Send(cancelTaskReplyText(context.Background(), actor, senderID, hasSender, payload(c), logger))
	}
}

// newAnswerHandler собирает tele.HandlerFunc для `/answer <id задачи> <id
// вопроса> <текст ответа>`, см. answerTaskReplyText.
func newAnswerHandler(actor telegramActor, logger *slog.Logger) tele.HandlerFunc {
	return func(c tele.Context) error {
		senderID, hasSender, _ := senderIDAndMessageID(c)
		return c.Send(answerTaskReplyText(context.Background(), actor, senderID, hasSender, payload(c), logger))
	}
}
