package api

// middleware.go — auth-middleware оркестратора (тикет 1.4, FR A3, D2, Gherkin
// §1 «Доступ к API по токену»).
//
// Назначение (бизнес): большинство операций контракта требуют, чтобы клиент
// был аутентифицирован access-токеном, выданным при логине/рефреше (тикет
// 1.3) — Gherkin §1 «Дано я вошёл и получил токен доступа / Когда я вызываю
// метод API с этим токеном / Тогда система определяет меня как владельца
// токена». Это middleware — единственное место, где Authorization-заголовок
// разбирается и проверяется для REST API оркестратора: при успехе кладёт
// идентификатор пользователя в context.Context запроса (получить — через
// UserIDFromContext), чтобы дальнейшие обработчики (и будущая изоляция данных
// тикета 1.5, FR A4/I3) могли действовать «от имени» предъявителя токена, не
// перепроверяя токен повторно. При отсутствии/невалидности/истечении токена —
// единый 401 в том же формате (Error{code,message}), что и у остальных
// auth-ошибок проекта (writeInvalidCredentials, writeInvalidRefreshToken).
//
// Как устроено (тех) — выборочное применение к защищённым маршрутам:
// openapi.yaml объявляет корневой `security: [{bearerAuth: []}]` и явно
// снимает требование (`security: []`) только для публичных/предавторизационных
// операций (GET /healthz, POST /auth/register, /auth/login, /auth/refresh,
// GET /machine/ws — у machine/ws своя авторизация по UUID интеграции через
// WebSocket, см. docs/protocol.md). Не дублируем этот список вручную (риск
// рассинхронизации со спекой): oapi-codegen уже кодирует его в
// server.gen.go — ServerInterfaceWrapper для каждой ЗАЩИЩЁННОЙ операции перед
// вызовом цепочки HandlerMiddlewares кладёт в контекст маркер
// `context.WithValue(ctx, BearerAuthScopes, []string{})` (см. сгенерированные
// GetAdminRegistrationToken/PostAuthLogout/GetTasks и т.д.), а для операций с
// `security: []` — НЕ кладёт. HandlerMiddlewares — официальный механизм
// oapi-codegen-chi (ChiServerOptions.Middlewares), выполняется ПОСЛЕ того, как
// этот маркер уже в контексте запроса, но ДО самого обработчика. Поэтому
// authMiddleware подключается через ChiServerOptions.Middlewares в
// NewRouter (server.go) и просто проверяет наличие BearerAuthScopes в
// контексте: есть — операция защищена контрактом, требуем валидный Bearer;
// нет — операция публична, пропускаем без проверки. Это даёт выборочное
// применение без ручного списка путей и без правок сгенерированного кода:
// единственный источник истины — security-блоки в api/openapi.yaml.
//
// Как следствие, POST /auth/logout ТОЖЕ защищён этим middleware: в контракте
// у него нет `security: []` (в отличие от /auth/register|login|refresh), то
// есть он наследует корневой bearerAuth — клиент должен прислать Authorization
// заголовок в дополнение к refresh_token в теле. Сама реализация
// PostAuthLogout (auth.go, тикет 1.3) не меняется этим тикетом — она по
// прежнему отзывает refresh-токен по телу запроса и не читает user_id из
// контекста; добавление Bearer-проверки происходит исключительно на уровне
// маршрутизации/middleware.

import (
	"context"
	"net/http"
	"strings"

	"github.com/google/uuid"

	"github.com/yarabey/agentify/internal/auth"
)

// userIDContextKey — приватный типобезопасный ключ контекста для user_id.
//
// Тип объявлен локально (не string/int), чтобы исключить коллизии с ключами
// сторонних пакетов в одном context.Context (стандартная рекомендация
// go vet/staticcheck SA1029).
type userIDContextKeyType struct{}

var userIDContextKey = userIDContextKeyType{}

// UserIDFromContext возвращает id пользователя, аутентифицированного
// authMiddleware, и true — либо uuid.Nil и false, если в контексте запроса
// нет идентификатора пользователя (запрос к публичному маршруту, либо вызов
// вне HTTP-цепочки оркестратора, напр. в тесте).
//
// Тип результата (google/uuid, не pgtype.UUID) совпадает с тем, что уже
// возвращает auth.ParseAccessToken (тикет 1.3) — middleware не вводит
// дополнительного UUID-типа, а обработчики, которым нужно сходить в БД
// (sqlc-модели orchestrator/internal/db используют pgtype.UUID), сами
// конвертируют значение через pgtype.UUID{Bytes: id, Valid: true} в месте
// использования (тикет 1.5 — изоляция данных).
func UserIDFromContext(ctx context.Context) (uuid.UUID, bool) {
	id, ok := ctx.Value(userIDContextKey).(uuid.UUID)
	return id, ok
}

// authMiddleware проверяет Authorization: Bearer <access-JWT> для операций,
// защищённых контрактом (см. godoc файла выше), и при успехе кладёт user_id в
// контекст запроса.
//
// Подписан как MiddlewareFunc (server.gen.go) и подключается через
// ChiServerOptions.Middlewares в NewRouter (server.go). Метод на *Server, а не
// свободная функция — нужен доступ к ключу подписи (s.jwtSigningKey),
// настроенному при создании сервера (NewServer, JWT_SIGNING_KEY из
// окружения).
func (s *Server) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Маркер BearerAuthScopes в контексте кладёт сгенерированный
		// ServerInterfaceWrapper только для операций без `security: []` в
		// openapi.yaml (см. godoc файла) — его отсутствие означает, что текущий
		// маршрут публичный и проверка токена не требуется.
		if r.Context().Value(BearerAuthScopes) == nil {
			next.ServeHTTP(w, r)
			return
		}

		token, ok := bearerToken(r.Header.Get("Authorization"))
		if !ok {
			writeUnauthorized(w)
			return
		}

		userID, err := auth.ParseAccessToken(token, s.jwtSigningKey)
		if err != nil {
			// Битый/мусорный токен, неверная подпись и истёкший срок действия —
			// auth.ParseAccessToken схлопывает все причины в единый
			// ErrInvalidAccessToken (тикет 1.3); middleware намеренно не
			// различает их в ответе по тому же принципу единого 401, что и
			// /auth/login, /auth/refresh.
			writeUnauthorized(w)
			return
		}

		ctx := context.WithValue(r.Context(), userIDContextKey, userID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// bearerToken разбирает заголовок Authorization вида "Bearer <token>" и
// возвращает сам токен. Схема сравнивается без учёта регистра (RFC 7235 не
// требует точного регистра для auth-scheme); пустой заголовок, отсутствие
// разделителя, любая схема кроме Bearer или пустой токен после неё — отказ.
func bearerToken(header string) (string, bool) {
	scheme, token, found := strings.Cut(header, " ")
	if !found || !strings.EqualFold(scheme, "Bearer") {
		return "", false
	}
	token = strings.TrimSpace(token)
	if token == "" {
		return "", false
	}
	return token, true
}

// writeUnauthorized отвечает единым 401 в формате схемы Error контракта
// (components/responses/Unauthorized) — тем же форматом и хелпером
// (writeError), что уже используют writeInvalidCredentials и
// writeInvalidRefreshToken (auth.go, тикет 1.3) для остальных 401-ответов
// проекта.
func writeUnauthorized(w http.ResponseWriter) {
	writeError(w, http.StatusUnauthorized, "unauthorized", "требуется валидный токен доступа")
}
