package api

// channels.go — привязка Telegram-аккаунта: генерация одноразового кода
// (тикет 9.6, FR A2, D3) и его обмен на запись channel_links (тикет 10.2, FR
// D3), Gherkin §6 «Уведомления» — привязка Telegram-аккаунта, предусловие
// сценария «Уведомление в Telegram».
//
// Назначение (бизнес): пользователь ставит задачи и получает уведомления не
// только через web, но и через Telegram. Прежде чем это заработает, нужно
// один раз связать его telegram_user_id с аккаунтом: аутентифицированный
// пользователь на экране «Настройки» генерирует одноразовый код
// (PostChannelsTelegramLinkCode, тикет 9.6, FR A2 — доступ только по
// валидному Bearer, код выпускается СТРОГО для себя), отправляет боту
// `/start <код>`, бот вызывает PostChannelsTelegramLink (см. bot/main.go) —
// этот обработчик проверяет код и создаёт привязку. В отличие от
// PostChannelsTelegramLinkCode маршрут PostChannelsTelegramLink — БЕЗ Bearer
// (`security: []` в api/openapi.yaml): в ЭТОЙ точке нет web-сессии
// пользователя, единственное доказательство права на привязку — сам
// одноразовый код (та же модель доверия, что у registration_token в
// POST /auth/register, тикет 1.2).
//
// Как устроено (тех): вся бизнес-логика (генерация кода — CodeIssuer;
// проверка кода и атомарная транзакция обмена — Linker.Exchange) — в
// orchestrator/internal/channel; здесь — только разбор HTTP-тела/контекста
// запроса и маппинг результатов/сентинел-ошибок на коды ответа контракта (см.
// ChannelLinker/ChannelLinkCodeIssuer в server.go).
import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/yarabey/agentify/orchestrator/internal/channel"
)

// channelTelegram — значение channel в БД/контракте для Telegram (тикет
// 10.2). Единственный поддерживаемый канал в MVP (CHECK-ограничение
// channel_link_codes/channel_links, миграция 00004).
const channelTelegram = "telegram"

// PostChannelsTelegramLink реализует POST /channels/telegram/link — обмен
// одноразового кода привязки на запись channel_links (FR D3).
//
// Бизнес: вызывается ботом при обработке `/start <code>` (тикет 10.2).
// Валидируем тело (code/telegram_user_id непусты), дальше вся проверка
// (существует/не использован/не истёк) и сама запись — в
// channel.Linker.Exchange (одна транзакция, см. его godoc). Успех — 200 с
// ChannelLink; отсутствие кода — 404; использован/истёк/уже привязан к
// другому пользователю — 409 (Gherkin §6 «Истёкший код привязки
// отклоняется/повторное использование отклоняется — прямо не описаны
// отдельными сценариями в docs/User_stories_Gherkin.md §6, но вытекают из FR
// D3 «привязка канала к аккаунту описана явно», см. docs/ТЗ_Оркестратор_бизнес-версия.md).
func (s *Server) PostChannelsTelegramLink(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var req ChannelLinkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_body", "тело запроса не является валидным JSON")
		return
	}
	if strings.TrimSpace(req.Code) == "" {
		writeError(w, http.StatusBadRequest, "validation_error", "code обязателен")
		return
	}
	if strings.TrimSpace(req.TelegramUserId) == "" {
		writeError(w, http.StatusBadRequest, "validation_error", "telegram_user_id обязателен")
		return
	}

	linker := s.getChannelLinker()
	if linker == nil {
		// Инвариант-сбой инициализации сервиса (orchestrator/main.go обязан
		// вызвать SetChannelLinker при наличии БД) — не штатный
		// пользовательский случай, тот же принцип, что у отсутствующего
		// transitioner/commandPublisher (integrations.go, tasks.go).
		s.logError("PostChannelsTelegramLink", errors.New("channelLinker не настроен"))
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	link, err := linker.Exchange(ctx, channelTelegram, req.Code, req.TelegramUserId)
	if err != nil {
		switch {
		case errors.Is(err, channel.ErrLinkCodeNotFound):
			writeError(w, http.StatusNotFound, "link_code_not_found", "код привязки не найден")
		case errors.Is(err, channel.ErrLinkCodeExpired):
			writeError(w, http.StatusConflict, "link_code_expired", "код привязки истёк")
		case errors.Is(err, channel.ErrLinkCodeUsed):
			writeError(w, http.StatusConflict, "link_code_used", "код привязки уже использован")
		case errors.Is(err, channel.ErrAlreadyLinked):
			writeError(w, http.StatusConflict, "channel_already_linked", "этот telegram-аккаунт уже привязан к другому пользователю")
		default:
			s.logError("channel.Linker.Exchange", err)
			writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		}
		return
	}

	// user_id владельца НЕ включаем в ответ: боту (тикет 10.3) он не нужен —
	// действия от имени пользователя бот выполняет через отдельный поиск по
	// (channel, external_id), а не по значению из этого ответа; ChannelLink
	// (контракт) сознательно не содержит user_id.
	writeJSON(w, http.StatusOK, ChannelLink{
		Channel:    &channelTelegramValue,
		ExternalId: &link.ExternalID,
		CreatedAt:  timePtr(link.CreatedAt.Time),
	})
}

// channelTelegramValue — типизированное (ChannelLinkChannel) значение
// channelTelegram для заполнения указательного поля ChannelLink.Channel
// (сгенерированный тип из контракта, тикет 0.2).
var channelTelegramValue = ChannelLinkChannel(channelTelegram)

// timePtr — берёт адрес значения time.Time; нужен, чтобы заполнить
// указательное поле ChannelLink.CreatedAt (сгенерированный тип, omitempty)
// значением из БД без промежуточной именованной переменной на каждом
// вызове.
func timePtr(t time.Time) *time.Time {
	return &t
}

// PostChannelsTelegramLinkCode реализует POST /channels/telegram/link-code —
// генерацию одноразового кода привязки Telegram-аккаунта (тикет 9.6, FR A2,
// D3, экран «Настройки»).
//
// Бизнес: маршрут защищён auth-middleware (в api/openapi.yaml у этой операции
// НЕТ `security: []`, в отличие от PostChannelsTelegramLink выше) — тело
// запроса не содержит и не может содержать "для кого выпустить код": код
// выпускается СТРОГО для вызывающего пользователя (user_id из
// UserIDFromContext, положенного туда authMiddleware после проверки
// access-токена, тикет 1.4), подделать код на чужой аккаунт этим путём
// невозможно (FR A2). Пользователь дальше показывает полученный code боту
// командой `/start <code>` — обмен на привязку (проверка
// существования/истечения/одноразовости) выполняет отдельный путь,
// PostChannelsTelegramLink (см. выше, тикет 10.2); эта операция сама привязку
// НЕ создаёт.
func (s *Server) PostChannelsTelegramLinkCode(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Маршрут защищён auth-middleware (тикет 1.4) — user_id уже должен быть в
	// контексте. Перепроверяем сами (тот же принцип, что и
	// GetAdminRegistrationToken, admin.go) на случай вызова обработчика в
	// обход штатной цепочки.
	userID, ok := UserIDFromContext(ctx)
	if !ok {
		writeUnauthorized(w)
		return
	}

	issuer := s.getChannelLinkCodeIssuer()
	if issuer == nil {
		// Инвариант-сбой инициализации сервиса (orchestrator/main.go обязан
		// вызвать SetChannelLinkCodeIssuer при наличии БД) — не штатный
		// пользовательский случай, тот же принцип, что у отсутствующего
		// channelLinker/transitioner/commandPublisher.
		s.logError("PostChannelsTelegramLinkCode", errors.New("channelLinkCodeIssuer не настроен"))
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	created, err := issuer.IssueLinkCode(ctx, pgtype.UUID{Bytes: userID, Valid: true}, channelTelegram)
	if err != nil {
		s.logError("channel.CodeIssuer.IssueLinkCode", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	writeJSON(w, http.StatusCreated, channelLinkCodeResponse{
		Code:      created.Code,
		ExpiresAt: created.ExpiresAt.Time,
	})
}

// channelLinkCodeResponse — тело успешного (201) ответа
// POST /channels/telegram/link-code (тикет 9.6). Схема этой операции в
// api/openapi.yaml — инлайновый `type: object` без отдельной записи в
// components/schemas (в отличие от ChannelLink/ChannelLinkRequest), поэтому
// oapi-codegen не генерирует для неё именованный Go-тип в types.gen.go — этот
// небольшой ручной тип с совпадающими JSON-тегами закрывает ту же форму
// ответа.
type channelLinkCodeResponse struct {
	Code      string    `json:"code"`
	ExpiresAt time.Time `json:"expires_at"`
}
