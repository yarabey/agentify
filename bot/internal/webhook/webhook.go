// Package webhook — приём входящих Telegram-апдейтов бота по webhook.
//
// Назначение (бизнес): бот — тонкий адаптер канала Telegram (FR D1). Telegram
// доставляет апдейты (сообщения, нажатия кнопок, ответы пользователя) HTTP
// POST-запросом на публичный URL за Caddy (`bot.<домен>`), а не long-polling'ом
// — так один инстанс за reverse-proxy получает события без исходящих
// соединений и лишних round-trip'ов к Bot API. Этот пакет — HTTP-обвязка вокруг
// telebot v3: он принимает такой запрос, проверяет секрет, декодирует апдейт,
// логирует его и передаёт в маршрутизатор telebot (bot.ProcessUpdate).
// Конкретные команды (`/start`, привязка, действия) — тикеты 10.2/10.3, здесь
// только каркас приёма (эпик 10, тикет 10.1).
//
// Как устроено (тех): Handler реализует http.Handler и монтируется на общий
// chi-роутер сервиса (internal/platform) по секретному пути вида
// `/webhook/<secret>` (см. Path). Дополнительно, если Telegram настроен с
// secret_token, приходящий заголовок X-Telegram-Bot-Api-Secret-Token сверяется
// с ожидаемым значением (defense in depth поверх секрета в пути). Обработка
// апдейта делегируется UpdateProcessor — его реализует *tele.Bot, но узкий
// интерфейс позволяет юнит-тестам подменять маршрутизатор фейком без реальной
// сети (приёмка FR D1).
package webhook

import (
	"encoding/json"
	"log/slog"
	"net/http"

	tele "gopkg.in/telebot.v3"
)

// secretTokenHeader — HTTP-заголовок, которым Telegram сопровождает каждый
// webhook-запрос, если при setWebhook задан secret_token (docs Telegram Bot
// API). Сверка с ожидаемым значением отсекает запросы, отправленные не
// Telegram'ом, даже если секретный путь стал известен.
const secretTokenHeader = "X-Telegram-Bot-Api-Secret-Token"

// pathPrefix — префикс секретного пути webhook. Полный путь — pathPrefix плюс
// секрет (см. Path), чтобы адрес webhook нельзя было угадать без секрета.
const pathPrefix = "/webhook/"

// UpdateProcessor обрабатывает один входящий Telegram-апдейт (маршрутизация по
// зарегистрированным хендлерам). Реализуется *tele.Bot; интерфейс сужен до
// одного метода, чтобы webhook-каркас не зависел от всего Bot API и покрывался
// юнит-тестами без реальной сети и токена.
type UpdateProcessor interface {
	// ProcessUpdate маршрутизирует апдейт по хендлерам, зарегистрированным в
	// telebot. В тикете 10.1 хендлеров ещё нет — вызов безвреден (no-op);
	// команды добавят тикеты 10.2/10.3.
	ProcessUpdate(u tele.Update)
}

// Handler — HTTP-обработчик входящих webhook-запросов Telegram.
//
// Он проверяет секретный заголовок (если задан), декодирует тело в
// tele.Update, логирует факт приёма и передаёт апдейт в UpdateProcessor.
// Telegram считает доставку успешной по коду 2xx; ошибки уровня приложения
// (маршрутизация/обработка) не должны приводить к повторной доставке того же
// апдейта, поэтому после успешного декодирования всегда отвечаем 200.
type Handler struct {
	processor   UpdateProcessor
	secretToken string
	logger      *slog.Logger
}

// NewHandler собирает Handler.
//
// Параметр processor — маршрутизатор апдейтов (обычно *tele.Bot). secretToken —
// ожидаемое значение заголовка X-Telegram-Bot-Api-Secret-Token; пустая строка
// отключает проверку заголовка (защита остаётся на уровне секретного пути, см.
// Path). logger используется для структурного лога приёма; при nil логирование
// пропускается.
func NewHandler(processor UpdateProcessor, secretToken string, logger *slog.Logger) *Handler {
	return &Handler{processor: processor, secretToken: secretToken, logger: logger}
}

// Path возвращает путь, на котором должен монтироваться webhook: `/webhook/` +
// secret. Секрет в пути делает адрес неугадываемым; тот же секрет задаётся как
// secret_token при регистрации webhook (SetWebhook), чтобы Telegram присылал
// его и в заголовке. Публичный URL для Telegram — это внешний адрес Caddy
// (`https://bot.<домен>`) плюс этот путь.
func Path(secret string) string {
	return pathPrefix + secret
}

// ServeHTTP принимает webhook-запрос Telegram (FR D1): сверяет секретный
// заголовок, декодирует апдейт и передаёт его в маршрутизатор.
//
// Только метод POST допустим (Telegram доставляет апдейты POST'ом); прочие
// методы получают 405. Неверный secret_token → 403, некорректный JSON → 400. На
// успешном приёме апдейт логируется и передаётся в UpdateProcessor, после чего
// возвращается 200.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Сверка secret_token: если Telegram настроен с ним, он приходит в каждом
	// запросе. Пустой h.secretToken означает «заголовок не проверяем» — защита
	// остаётся на секретном пути (см. Path).
	if h.secretToken != "" && r.Header.Get(secretTokenHeader) != h.secretToken {
		if h.logger != nil {
			h.logger.Warn("webhook: отклонён запрос с неверным secret_token")
		}
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	var update tele.Update
	if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
		if h.logger != nil {
			h.logger.Warn("webhook: не удалось декодировать апдейт", slog.Any("error", err))
		}
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	if h.logger != nil {
		// Базовое логирование апдейта (тикет 10.1): id и вид — достаточно, чтобы
		// подтвердить приём, не логируя потенциально приватное содержимое.
		h.logger.Info("webhook: принят апдейт",
			slog.Int("update_id", update.ID),
			slog.String("kind", updateKind(update)),
		)
	}

	h.processor.ProcessUpdate(update)

	w.WriteHeader(http.StatusOK)
}

// updateKind возвращает короткое имя вида апдейта для лога (message, callback и
// т.п.). Полная маршрутизация — забота telebot; здесь лишь грубая
// классификация самых частых видов ради читаемого лога, без раскрытия
// содержимого.
func updateKind(u tele.Update) string {
	switch {
	case u.Message != nil:
		return "message"
	case u.EditedMessage != nil:
		return "edited_message"
	case u.Callback != nil:
		return "callback"
	case u.Query != nil:
		return "inline_query"
	case u.MyChatMember != nil:
		return "my_chat_member"
	case u.ChatMember != nil:
		return "chat_member"
	default:
		return "other"
	}
}
