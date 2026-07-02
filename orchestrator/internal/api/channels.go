package api

// channels.go — привязка Telegram-аккаунта: генерация одноразового кода
// (тикет 9.6, FR A2, D3) и его обмен на запись channel_links (тикет 10.2, FR
// D3), Gherkin §6 «Уведомления» — привязка Telegram-аккаунта, предусловие
// сценария «Уведомление в Telegram»; и выдача боту короткоживущего
// acting-токена для действий из Telegram от имени привязанного пользователя
// (тикет 10.3, FR D1, §4 «Постановка задачи из канала»).
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
// POST /auth/register, тикет 1.2). Дальше, когда пользователь пишет боту
// текст/команду (постановка задачи, отмена, ответ на вопрос — тикет 10.3),
// бот вызывает PostChannelsTelegramToken, чтобы получить право действовать
// от его имени через ОБЫЧНЫЕ защищённые операции контракта (принцип «единый
// API», docs/01_tech_stack_and_architecture.md) — см. её отдельный godoc
// ниже про архитектурное решение и выбор способа аутентификации бота.
//
// Как устроено (тех): вся бизнес-логика (генерация кода — CodeIssuer;
// проверка кода и атомарная транзакция обмена — Linker.Exchange) — в
// orchestrator/internal/channel; здесь — только разбор HTTP-тела/контекста
// запроса и маппинг результатов/сентинел-ошибок на коды ответа контракта (см.
// ChannelLinker/ChannelLinkCodeIssuer в server.go).
import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/yarabey/agentify/internal/auth"
	"github.com/yarabey/agentify/orchestrator/internal/channel"
	"github.com/yarabey/agentify/orchestrator/internal/db"
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

// PostChannelsTelegramToken реализует POST /channels/telegram/token —
// служебный (не пользовательский) эндпоинт, обменивающий telegram_user_id
// входящего Telegram-апдейта на короткоживущий пользовательский access-JWT
// (тикет 10.3, FR D1, §4 «Постановка задачи из канала»).
//
// АРХИТЕКТУРНОЕ РЕШЕНИЕ (аутентификация бота для действий от имени
// пользователя): постановка/отмена задачи и ответ на вопрос агента из
// Telegram (тикет 10.3) обязаны идти через ТОТ ЖЕ REST API, что и web
// (принцип «единый API», docs/01_tech_stack_and_architecture.md §1) — у
// оркестратора не должно появляться отдельного, Telegram-специфичного пути
// постановки задачи. Проблема: защищённые операции контракта
// (POST /tasks, /tasks/{id}/cancel, /tasks/{id}/answer, GET /integrations)
// аутентифицируют по пользовательскому access-JWT (Authorization: Bearer,
// authMiddleware, тикет 1.4), а у бота такого JWT нет — Telegram присылает
// боту только telegram_user_id отправителя апдейта, у пользователя никакой
// web-сессии в этот момент не существует.
//
// Рассмотренные варианты:
//  1. Выпускать сервисный JWT боту с произвольным claim'ом acting_user_id,
//     проверяемым отдельной веткой authMiddleware. Отклонено: потребовало бы
//     разветвления auth-middleware на «два вида токенов» (пользовательский
//     access-JWT vs сервисный JWT-от-имени), что усложняет ЕДИНСТВЕННУЮ
//     проверочную точку auth.ParseAccessToken/authMiddleware (тикет 1.4) —
//     она перестаёт быть про «кто предъявитель токена» и становится про
//     «кто предъявитель и была ли это подмена от чужого имени», два разных
//     вопроса в одном месте.
//  2. (ВЫБРАНО) Отдельный внутренний service-to-service эндпоинт
//     (этот, PostChannelsTelegramToken), защищённый общим сервисным
//     секретом (X-Bot-Service-Secret, ORCH_BOT_SERVICE_SECRET/
//     BOT_SERVICE_SECRET) — бот передаёт telegram_user_id, оркестратор сам
//     резолвит user_id через channel_links (FR D3, тикет 10.2) и выпускает
//     ОБЫЧНЫЙ access-JWT (auth.IssueAccessToken — та же функция, что и у
//     POST /auth/login, тикет 1.3, тот же формат/TTL). Дальше бот —
//     ОБЫЧНЫЙ клиент контракта: шлёт этот JWT как Bearer в POST /tasks и
//     т.д., проходя ТОТ ЖЕ authMiddleware, что и web, без единой строчки
//     Telegram-специфичной логики авторизации в бизнес-обработчиках задач
//     (tasks.go этим тикетом НЕ меняется вообще). Единственное новое
//     доверенное отношение — между двумя СЕРВИСАМИ (бот↔оркестратор), не
//     между ботом и произвольным пользователем, и оно уже используется в
//     проекте: BOT_ORCHESTRATOR_URL (тикет 10.2) — доверенный внутренний
//     канал compose-сети, доступный боту напрямую по внутреннему DNS,
//     минуя Caddy/публичный интернет (deploy/docker-compose.yml). Секрет
//     передаётся тем же путём, что и остальные секреты проекта (GitHub
//     Secrets/окружение хоста, НЕ коммитится, см. docs/MANUAL_STEPS.md).
//
// Явно НЕ рассмотрено и не нужно: постоянный/непросроченный токен бота на
// пользователя — acting-токен живёт ровно AccessTokenTTL (15 минут, как и
// обычный access-JWT), бот запрашивает новый при каждом действии
// пользователя (см. bot/internal/orchestrator.Client.GetActingToken) — не
// требует отдельного хранилища/ротации токенов на стороне бота.
//
// Алгоритм: сверить X-Bot-Service-Secret с s.botServiceSecret (constant-time
// сравнение, subtle.ConstantTimeCompare — секрет предъявляется по HTTP, та же
// крипто-гигиена, что и у сравнения хэшей паролей/refresh-токенов) → пустой
// настроенный секрет ИЛИ несовпадение → 401 (фича мягко выключена без
// настроенного секрета — деталь НЕ раскрывается вызывающему тем же кодом
// unauthorized, что и обычный 401 auth-middleware, чтобы не давать сигнал,
// сконфигурирован ли сервис) → декодировать/провалидировать тело
// (telegram_user_id обязателен) → резолвить user_id через
// GetChannelLinkByChannelAndExternalID(channel="telegram", external_id) →
// не найдено (пользователь ещё не выполнил /start <code>, тикет 10.2) → 404
// not_linked (бот транслирует это в понятную пользователю подсказку, см.
// bot/internal/orchestrator.ErrNotLinked) → выпустить access-JWT
// (auth.IssueAccessToken, TTL = auth.AccessTokenTTL, тот же ключ подписи
// s.jwtSigningKey, что и у /auth/login) → 200 с access_token/expires_at.
func (s *Server) PostChannelsTelegramToken(w http.ResponseWriter, r *http.Request, params PostChannelsTelegramTokenParams) {
	ctx := r.Context()

	secret := s.getBotServiceSecret()
	// Пустой настроенный секрет — фича мягко выключена (см. godoc поля
	// botServiceSecret, server.go): ConstantTimeCompare с пустым срезом при
	// пустом presented-значении дал бы true, поэтому пустой secret
	// проверяется явно и ВСЕГДА отклоняется, вне зависимости от заголовка.
	presented := []byte(params.XBotServiceSecret)
	if len(secret) == 0 || len(presented) != len(secret) || subtle.ConstantTimeCompare(presented, secret) != 1 {
		writeUnauthorized(w)
		return
	}

	var req TelegramActingTokenRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_body", "тело запроса не является валидным JSON")
		return
	}
	if strings.TrimSpace(req.TelegramUserId) == "" {
		writeError(w, http.StatusBadRequest, "validation_error", "telegram_user_id обязателен")
		return
	}

	link, err := s.queries.GetChannelLinkByChannelAndExternalID(ctx, db.GetChannelLinkByChannelAndExternalIDParams{
		Channel:    channelTelegram,
		ExternalID: req.TelegramUserId,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "not_linked", "telegram-аккаунт не привязан к аккаунту agentify")
			return
		}
		s.logError("GetChannelLinkByChannelAndExternalID", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	userID := uuid.UUID(link.UserID.Bytes)
	now := time.Now().UTC()
	accessToken, err := auth.IssueAccessToken(userID.String(), s.jwtSigningKey, now)
	if err != nil {
		s.logError("auth.IssueAccessToken (telegram acting token)", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	expiresAt := now.Add(auth.AccessTokenTTL)
	writeJSON(w, http.StatusOK, TelegramActingToken{
		AccessToken: &accessToken,
		ExpiresAt:   &expiresAt,
	})
}
