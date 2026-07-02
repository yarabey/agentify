// Package orchestrator — узкий HTTP-клиент бота к единому API оркестратора
// (тикет 10.2, FR D3; принцип «единый API», см. bot/README.md).
//
// Назначение (бизнес): вся бизнес-логика (проверка кода привязки, запись в
// БД) живёт в оркестраторе, а не в боте (bot/README.md, «тонкий адаптер
// канала»). Этот пакет — единственное место в bot, где формируется HTTP-запрос
// к оркестратору для обмена одноразового кода привязки (`/start <code>`,
// см. bot/start.go) на запись channel_links. Последующие тикеты (10.3 —
// постановка/отмена задачи и ответ на вопрос из Telegram, 10.4 — доставка
// уведомлений) добавят сюда свои методы того же клиента; в этом тикете —
// только LinkTelegram.
//
// Как устроено (тех): Client оборачивает *http.Client и baseURL оркестратора
// (BOT_ORCHESTRATOR_URL, см. bot/main.go). Запрос/ответ не берутся из
// сгенерированного клиента (make generate генерит только СЕРВЕРНЫЙ интерфейс
// для Go — см. api/oapi-codegen.server.yaml, godoc там же) — тело запроса
// собирается вручную по контракту POST /channels/telegram/link
// (api/openapi.yaml). Ошибки контракта (404/409) мапятся в сентинелы этого
// пакета по полю Error.code тела ответа (см. writeError в
// orchestrator/internal/api), которые вызывающая сторона (bot/start.go)
// разбирает через errors.Is и превращает в понятный пользователю текст.
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
)

// errorCodeToSentinel мапит поле code тела ответа Error (api/openapi.yaml
// components/schemas/Error) на сентинелы пакета — единственное место этого
// соответствия, чтобы не размазывать строковые коды по bot/start.go.
var errorCodeToSentinel = map[string]error{
	"link_code_not_found":    ErrLinkCodeNotFound,
	"link_code_expired":      ErrLinkCodeExpired,
	"link_code_used":         ErrLinkCodeUsed,
	"channel_already_linked": ErrAlreadyLinked,
}

// Client — HTTP-клиент бота к API оркестратора.
type Client struct {
	baseURL    string
	httpClient *http.Client
}

// NewClient собирает Client поверх базового URL оркестратора (напр.
// `http://orchestrator:8080` внутри docker compose или `https://api.<домен>`
// в проде — см. BOT_ORCHESTRATOR_URL в bot/README.md). Завершающий `/` в
// baseURL, если есть, обрезается.
func NewClient(baseURL string) *Client {
	return &Client{
		baseURL:    strings.TrimSuffix(baseURL, "/"),
		httpClient: &http.Client{Timeout: requestTimeout},
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

	var eb errorBody
	if derr := json.NewDecoder(resp.Body).Decode(&eb); derr == nil {
		if sentinel, ok := errorCodeToSentinel[eb.Code]; ok {
			return sentinel
		}
		if eb.Message != "" {
			return fmt.Errorf("orchestrator: LinkTelegram: %s (%s, статус %d)", eb.Message, eb.Code, resp.StatusCode)
		}
	}
	return fmt.Errorf("orchestrator: LinkTelegram: неожиданный статус %d", resp.StatusCode)
}
