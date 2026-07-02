// Package orchestrator — узкий HTTP-клиент бота к единому API оркестратора
// (тикеты 10.2/10.3, FR D3/D1; принцип «единый API», см. bot/README.md).
//
// Назначение (бизнес): вся бизнес-логика (проверка кода привязки, FSM задачи,
// запись в БД) живёт в оркестраторе, а не в боте (bot/README.md, «тонкий
// адаптер канала»). Этот пакет — единственное место в bot, где формируются
// HTTP-запросы к оркестратору: обмен одноразового кода привязки (`/start
// <code>`, тикет 10.2, см. bot/start.go, метод LinkTelegram) и, начиная с
// тикета 10.3, действия из Telegram от имени привязанного пользователя —
// постановка/отмена задачи, ответ на вопрос агента (см. bot/task.go).
//
// Действия из Telegram (тикет 10.3) идут в ДВА шага, оба через этот Client:
//  1. GetActingToken резолвит telegram_user_id отправителя апдейта в
//     короткоживущий пользовательский access-JWT (POST
//     /channels/telegram/token — служебный, НЕ пользовательский эндпоинт,
//     защищённый сервисным секретом, а не Bearer, — см. её архитектурный
//     годок в orchestrator/internal/api/channels.go).
//  2. Дальше Client вызывает ОБЫЧНЫЕ защищённые операции контракта
//     (ListIntegrations, CreateTask, AnswerTask, CancelTask) с этим токеном
//     как Bearer — РОВНО как это делал бы web-клиент. У бота НЕТ
//     собственной, Telegram-специфичной логики постановки задачи ни здесь,
//     ни тем более в оркестраторе — это и есть принцип «единый API».
//
// Как устроено (тех): Client оборачивает *http.Client, baseURL оркестратора
// (BOT_ORCHESTRATOR_URL, см. bot/main.go) и serviceSecret (BOT_SERVICE_SECRET,
// тикет 10.3, тот же секрет, что ORCH_BOT_SERVICE_SECRET у оркестратора).
// Запрос/ответ не берутся из сгенерированного клиента (make generate генерит
// только СЕРВЕРНЫЙ интерфейс для Go — см. api/oapi-codegen.server.yaml, годок
// там же) — тела запросов собираются вручную по контракту (api/openapi.yaml).
// Ошибки контракта (404/409) мапятся в сентинелы этого пакета по полю
// Error.code тела ответа (см. writeError в orchestrator/internal/api),
// которые вызывающая сторона (bot/start.go, bot/task.go) разбирает через
// errors.Is и превращает в понятный пользователю текст.
package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// requestTimeout — таймаут HTTP-запроса к оркестратору по умолчанию.
// Обработка апдейта в webhook.Handler синхронна (Synchronous: true, тикет
// 10.1) — короткий таймаут не даёт одному зависшему запросу к оркестратору
// надолго заблокировать обработку конкретного апдейта Telegram.
const requestTimeout = 10 * time.Second

// Ошибки LinkTelegram (FR D3) — сентинелы, мапятся 1:1 на коды ответа
// PostChannelsTelegramLink (orchestrator/internal/api/channels.go):
// link_code_not_found/link_code_expired/link_code_used/channel_already_linked.
// Любая другая ошибка (сеть, 5xx, неожиданный код) возвращается как есть —
// вызывающая сторона (bot/start.go) отвечает пользователю общим текстом.
var (
	// ErrLinkCodeNotFound — код привязки не найден (404).
	ErrLinkCodeNotFound = errors.New("orchestrator: код привязки не найден")
	// ErrLinkCodeExpired — код привязки найден, но истёк (409
	// link_code_expired). Приёмка тикета 10.2: «истёкший код → ошибка».
	ErrLinkCodeExpired = errors.New("orchestrator: код привязки истёк")
	// ErrLinkCodeUsed — код привязки уже был использован ранее (409
	// link_code_used, одноразовость — Gherkin §6).
	ErrLinkCodeUsed = errors.New("orchestrator: код привязки уже использован")
	// ErrAlreadyLinked — этот telegram_user_id уже привязан к другому
	// аккаунту (409 channel_already_linked).
	ErrAlreadyLinked = errors.New("orchestrator: telegram-аккаунт уже привязан к другому пользователю")

	// ErrNotLinked — GetActingToken (тикет 10.3, FR D1): telegram_user_id
	// отправителя апдейта не привязан ни к одному аккаунту agentify (404
	// not_linked, POST /channels/telegram/token). Вызывающая сторона
	// (bot/task.go) отвечает пользователю подсказкой `/start <code>` из
	// web-настроек (тикет 9.6) — тот же текст, что и у ErrLinkCodeNotFound в
	// другом сценарии (bot/start.go).
	ErrNotLinked = errors.New("orchestrator: telegram-аккаунт не привязан ни к одному пользователю agentify")

	// ErrNotFound — постановка/отмена задачи или ответ на вопрос (тикет 10.3)
	// сослались на не существующую (или чужую — единый 404, FR A4/I3) сущность:
	// интеграцию (CreateTask), задачу (AnswerTask/CancelTask) либо вопрос
	// (AnswerTask). Единый сентинел для всех трёх методов, а НЕ отдельные
	// ErrIntegrationNotFound/ErrTaskNotFound/ErrQuestionNotFound: код ответа
	// контракта во всех трёх случаях один и тот же "not_found" (см.
	// writeIntegrationNotFound/writeTaskNotFound, orchestrator/internal/api/
	// integrations.go, и ветку "вопрос не найден" в PostTasksIdAnswer,
	// tasks.go) — различать причину здесь негде, вызывающая сторона
	// (bot/task.go) уже знает КАКОЙ метод вызвала и формулирует пользователю
	// уместный по контексту текст сама.
	ErrNotFound = errors.New("orchestrator: интеграция/задача/вопрос не найдены")
)

// errorCodeToSentinel мапит поле code тела ответа Error (api/openapi.yaml
// components/schemas/Error) на сентинелы пакета — единственное место этого
// соответствия, чтобы не размазывать строковые коды по bot/start.go.
var errorCodeToSentinel = map[string]error{
	"link_code_not_found":    ErrLinkCodeNotFound,
	"link_code_expired":      ErrLinkCodeExpired,
	"link_code_used":         ErrLinkCodeUsed,
	"channel_already_linked": ErrAlreadyLinked,
	"not_linked":             ErrNotLinked,
	"not_found":              ErrNotFound,
}

// decodeAPIError разбирает тело ошибки неуспешного HTTP-ответа (схема Error
// контракта, components/schemas/Error) и мапит поле code на сентинел этого
// пакета через errorCodeToSentinel — общая логика для ВСЕХ методов Client
// (LinkTelegram, GetActingToken, CreateTask, AnswerTask, CancelTask), не
// только LinkTelegram (тикет 10.2), которому изначально принадлежала.
// Неизвестный code/нераспарсиваемое тело — обёрнутая fmt.Errorf с op в
// сообщении, вызывающая сторона отвечает пользователю общим текстом.
func decodeAPIError(op string, resp *http.Response) error {
	var eb errorBody
	if derr := json.NewDecoder(resp.Body).Decode(&eb); derr == nil {
		if sentinel, ok := errorCodeToSentinel[eb.Code]; ok {
			return sentinel
		}
		if eb.Message != "" {
			return fmt.Errorf("orchestrator: %s: %s (%s, статус %d)", op, eb.Message, eb.Code, resp.StatusCode)
		}
	}
	return fmt.Errorf("orchestrator: %s: неожиданный статус %d", op, resp.StatusCode)
}

// Client — HTTP-клиент бота к API оркестратора.
type Client struct {
	baseURL    string
	httpClient *http.Client

	// serviceSecret — общий сервисный секрет между ботом и оркестратором
	// (тикет 10.3, BOT_SERVICE_SECRET/ORCH_BOT_SERVICE_SECRET, см. годок
	// пакета выше), отправляется как заголовок X-Bot-Service-Secret в
	// GetActingToken. Пустая строка допустима (см. её годок про мягкую
	// деградацию — тот же принцип, что у BOT_ORCHESTRATOR_URL в
	// bot/main.go).
	serviceSecret string
}

// NewClient собирает Client поверх базового URL оркестратора (напр.
// `http://orchestrator:8080` внутри docker compose или `https://api.<домен>`
// в проде — см. BOT_ORCHESTRATOR_URL в bot/README.md) и сервисного секрета
// бота (serviceSecret, тикет 10.3, BOT_SERVICE_SECRET — нужен только
// GetActingToken; пустая строка допустима, см. её годок). Завершающий `/` в
// baseURL, если есть, обрезается.
func NewClient(baseURL, serviceSecret string) *Client {
	return &Client{
		baseURL:       strings.TrimSuffix(baseURL, "/"),
		serviceSecret: serviceSecret,
		httpClient:    &http.Client{Timeout: requestTimeout},
	}
}

// linkTelegramRequest — тело запроса, дословно повторяет
// components/schemas/ChannelLinkRequest контракта (api/openapi.yaml).
type linkTelegramRequest struct {
	Code           string `json:"code"`
	TelegramUserID string `json:"telegram_user_id"`
}

// errorBody — тело ошибки, дословно повторяет components/schemas/Error
// контракта.
type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// LinkTelegram обменивает одноразовый код привязки code на привязку
// telegramUserID к аккаунту, выпустившему код (FR D3). Вызывается
// bot/start.go при обработке `/start <code>`.
//
// nil-ошибка — привязка создана (POST /channels/telegram/link ответил 200).
// Известные бизнес-исходы возвращаются как сентинелы этого файла (см.
// errorCodeToSentinel); прочие ошибки (сеть, неожиданный статус/тело) —
// обёрнуты через fmt.Errorf, вызывающая сторона отвечает пользователю общим
// текстом «попробуйте позже».
func (c *Client) LinkTelegram(ctx context.Context, code, telegramUserID string) error {
	body, err := json.Marshal(linkTelegramRequest{Code: code, TelegramUserID: telegramUserID})
	if err != nil {
		return fmt.Errorf("orchestrator: сериализовать тело LinkTelegram: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/channels/telegram/link", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("orchestrator: собрать запрос LinkTelegram: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("orchestrator: запрос LinkTelegram: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusOK {
		return nil
	}
	return decodeAPIError("LinkTelegram", resp)
}

// --- Действия из Telegram (тикет 10.3, FR D1) ---------------------------

// actingTokenRequest — тело запроса, дословно повторяет
// components/schemas/TelegramActingTokenRequest контракта.
type actingTokenRequest struct {
	TelegramUserID string `json:"telegram_user_id"`
}

// actingTokenResponse — тело успешного ответа, дословно повторяет
// components/schemas/TelegramActingToken контракта. expires_at не
// используется вызывающей стороной сейчас (Client не кэширует токены — см.
// годок GetActingToken), но разбирается для полноты соответствия контракту.
type actingTokenResponse struct {
	AccessToken string    `json:"access_token"`
	ExpiresAt   time.Time `json:"expires_at"`
}

// GetActingToken резолвит telegramUserID (отправитель Telegram-апдейта) в
// короткоживущий access-JWT пользователя, к которому он привязан (тикет
// 10.3, FR D1, POST /channels/telegram/token). Аутентифицируется НЕ Bearer
// (у бота нет пользовательского токена), а сервисным секретом
// (X-Bot-Service-Secret: c.serviceSecret) — см. архитектурный годок
// PostChannelsTelegramToken в orchestrator/internal/api/channels.go.
//
// Намеренно НЕ кэширует токен между вызовами: каждое действие пользователя в
// Telegram (новое сообщение/команда) запрашивает свежий токен — это
// единственный обычный HTTP round-trip, приемлемый на фоне сетевого запроса
// к самому Telegram Bot API, и избавляет бот от необходимости хранить
// состояние (TTL/протухание) между апдейтами.
//
// nil-ошибка — token непуст. ErrNotLinked (см. её годок) — самый частый
// бизнес-исход: вызывающая сторона (bot/task.go) отвечает пользователю
// подсказкой `/start <code>`. Прочие ошибки — обёрнуты, вызывающая сторона
// отвечает общим текстом.
func (c *Client) GetActingToken(ctx context.Context, telegramUserID string) (string, error) {
	body, err := json.Marshal(actingTokenRequest{TelegramUserID: telegramUserID})
	if err != nil {
		return "", fmt.Errorf("orchestrator: сериализовать тело GetActingToken: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/channels/telegram/token", bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("orchestrator: собрать запрос GetActingToken: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Bot-Service-Secret", c.serviceSecret)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("orchestrator: запрос GetActingToken: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return "", decodeAPIError("GetActingToken", resp)
	}

	var tr actingTokenResponse
	if derr := json.NewDecoder(resp.Body).Decode(&tr); derr != nil {
		return "", fmt.Errorf("orchestrator: GetActingToken: декодировать тело ответа: %w", derr)
	}
	if tr.AccessToken == "" {
		return "", fmt.Errorf("orchestrator: GetActingToken: пустой access_token в ответе")
	}
	return tr.AccessToken, nil
}

// Integration — сокращённое представление components/schemas/Integration
// контракта: только поля, нужные боту, чтобы предложить пользователю выбор
// машины при постановке задачи (тикет 10.3, см. bot/task.go).
type Integration struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// ListIntegrations возвращает интеграции пользователя, от чьего имени выдан
// accessToken (GET /integrations, owner-scoped на стороне оркестратора — FR
// A4, I3, тот же принцип, что и у web). Используется bot/task.go, чтобы
// определить, к какой машине направить задачу, поставленную обычным текстом
// (без явного /task <integration_id>, см. годок TaskReplyText).
func (c *Client) ListIntegrations(ctx context.Context, accessToken string) ([]Integration, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/integrations", nil)
	if err != nil {
		return nil, fmt.Errorf("orchestrator: собрать запрос ListIntegrations: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("orchestrator: запрос ListIntegrations: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, decodeAPIError("ListIntegrations", resp)
	}

	var integrations []Integration
	if derr := json.NewDecoder(resp.Body).Decode(&integrations); derr != nil {
		return nil, fmt.Errorf("orchestrator: ListIntegrations: декодировать тело ответа: %w", derr)
	}
	return integrations, nil
}

// taskCreateRequest — тело запроса, дословно повторяет
// components/schemas/TaskCreate контракта.
type taskCreateRequest struct {
	IntegrationID string `json:"integration_id"`
	Text          string `json:"text"`
}

// Task — сокращённое представление components/schemas/Task контракта: только
// поля, нужные боту, чтобы подтвердить пользователю id/статус созданной
// задачи (тикет 10.3, см. bot/task.go).
type Task struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

// CreateTask ставит задачу в очередь к машине integrationID от имени
// пользователя, которому принадлежит accessToken (POST /tasks, FR E1, тикет
// 5.3 — ЭТОТ ЖЕ эндпоинт единого API, что использует и web; здесь никакой
// Telegram-специфичной логики). idempotencyKey — дедуп повторной постановки
// (FR E7, тикет 5.5, §4 «Защита от двойной отправки»): bot/task.go передаёт
// сюда значение, детерминированное от (telegram_user_id, message_id) —
// повторная доставка ТОГО ЖЕ Telegram-апдейта (webhook — at-least-once) не
// создаёт вторую задачу.
//
// nil-ошибка — task заполнена. ErrNotFound (см. её годок) — integrationID не
// существует или не принадлежит пользователю; вызывающая сторона (bot/task.go)
// формулирует уместный текст сама.
func (c *Client) CreateTask(ctx context.Context, accessToken, integrationID, text, idempotencyKey string) (Task, error) {
	body, err := json.Marshal(taskCreateRequest{IntegrationID: integrationID, Text: text})
	if err != nil {
		return Task{}, fmt.Errorf("orchestrator: сериализовать тело CreateTask: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/tasks", bytes.NewReader(body))
	if err != nil {
		return Task{}, fmt.Errorf("orchestrator: собрать запрос CreateTask: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Idempotency-Key", idempotencyKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return Task{}, fmt.Errorf("orchestrator: запрос CreateTask: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// 201 — новая задача; 200 — идемпотентный повтор (тот же Idempotency-Key,
	// см. годок PostTasks, orchestrator/internal/api/tasks.go) — для
	// вызывающей стороны (bot/task.go) оба одинаково успешны.
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return Task{}, decodeAPIError("CreateTask", resp)
	}

	var task Task
	if derr := json.NewDecoder(resp.Body).Decode(&task); derr != nil {
		return Task{}, fmt.Errorf("orchestrator: CreateTask: декодировать тело ответа: %w", derr)
	}
	return task, nil
}

// answerRequest — тело запроса, повторяет инлайновую схему requestBody
// POST /tasks/{id}/answer контракта.
type answerRequest struct {
	QuestionID string `json:"question_id"`
	Text       string `json:"text"`
}

// AnswerTask отвечает на вопрос агента questionID в задаче taskID от имени
// пользователя, которому принадлежит accessToken (POST /tasks/{id}/answer,
// FR F1, F2, тикет 6.1 — ЭТОТ ЖЕ эндпоинт единого API, что использует web).
//
// nil-ошибка — ответ принят (202). ErrNotFound (см. её годок) — задача не
// существует/не принадлежит пользователю, ЛИБО question_id не сопоставлен ни
// с одним вопросом ИМЕННО этой задачи (см. годок PostTasksIdAnswer,
// orchestrator/internal/api/tasks.go про то, почему различать эти два случая
// не нужно) — вызывающая сторона (bot/task.go) отвечает общим текстом «задача
// или вопрос не найдены».
func (c *Client) AnswerTask(ctx context.Context, accessToken, taskID, questionID, text string) error {
	body, err := json.Marshal(answerRequest{QuestionID: questionID, Text: text})
	if err != nil {
		return fmt.Errorf("orchestrator: сериализовать тело AnswerTask: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/tasks/"+taskID+"/answer", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("orchestrator: собрать запрос AnswerTask: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+accessToken)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("orchestrator: запрос AnswerTask: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusAccepted {
		return decodeAPIError("AnswerTask", resp)
	}
	return nil
}

// CancelTask отменяет задачу taskID от имени пользователя, которому
// принадлежит accessToken (POST /tasks/{id}/cancel, FR E6, тикет 8.4 — ЭТОТ
// ЖЕ эндпоинт единого API, что использует web).
//
// nil-ошибка — отмена принята (202, доставка команды остановки машине —
// асинхронна, см. годок PostTasksIdCancel). ErrNotFound (см. её годок) —
// задача не существует/не принадлежит пользователю; вызывающая сторона
// (bot/task.go) отвечает уместным текстом сама.
func (c *Client) CancelTask(ctx context.Context, accessToken, taskID string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/tasks/"+taskID+"/cancel", nil)
	if err != nil {
		return fmt.Errorf("orchestrator: собрать запрос CancelTask: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("orchestrator: запрос CancelTask: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusAccepted {
		return decodeAPIError("CancelTask", resp)
	}
	return nil
}
