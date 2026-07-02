package api

// auth.go — обработчики логина/refresh/logout (тикет 1.3, FR A3).
//
// Назначение (бизнес): после регистрации (тикет 1.2) пользователю нужен способ
// войти и поддерживать сессию без повторного ввода пароля при каждом запросе:
//   - POST /auth/login — проверка username/password, выдача пары токенов;
//   - POST /auth/refresh — обмен непротухшего refresh на новую пару (ротация:
//     старый refresh немедленно становится недействителен — повторное
//     использование украденного/перехваченного refresh после легитимной ротации
//     отклоняется, это и есть защита от replay для FR A3);
//   - POST /auth/logout — явный отзыв refresh-токена (пользователь захотел
//     завершить сессию на этом устройстве).
//
// Во всех трёх случаях ошибки относительно учётных данных/токена намеренно не
// различаются по причине в HTTP-ответе (единый 401 «invalid_credentials» /
// «invalid_refresh_token»): подсказка «пароль неверный, а не username» или
// «токен отозван, а не истёк» — это утечка информации атакующему без пользы
// для легитимного клиента.
//
// Как устроено (тех): бизнес-логика здесь, SQL — в db (sqlc, тикет 1.1,
// переиспользуется как есть), хэширование пароля — в internal/auth (тикет 1.2),
// выпуск/хэширование токенов — в internal/auth (jwt.go, refresh_token.go,
// этот тикет). Access-токен — JWT (golang-jwt/jwt/v5, HS256, TTL см.
// auth.AccessTokenTTL), refresh — непрозрачная случайная строка, в БД
// сохраняется только её хэш (см. refreshTokenTTL ниже).

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/yarabey/agentify/internal/auth"
	"github.com/yarabey/agentify/orchestrator/internal/db"
)

// refreshTokenTTL — срок жизни refresh-токена: 30 дней.
//
// Решение принято на уровне тикета 1.3: ни docs/protocol.md, ни бизнес-ТЗ не
// фиксируют конкретное значение явно (зафиксирован только TTL access-токена —
// 15 минут). 30 дней — типичный баланс для MVP с одним пользователем на
// аккаунт: достаточно долго, чтобы не разлогинивать активного пользователя
// между сессиями работы (web PWA, Telegram), но конечно — токен из давно
// неиспользуемого устройства в итоге протухает сам, без необходимости ручного
// отзыва. При необходимости значение можно будет вынести в конфиг отдельным
// тикетом, не меняя контракт API (TTL не часть ответа, кроме access-токена).
const refreshTokenTTL = 30 * 24 * time.Hour

// PostAuthLogin реализует POST /auth/login — выдачу пары токенов по
// username/password (FR A3).
//
// Бизнес: пароль проверяется argon2id-сравнением (internal/auth, тикет 1.2);
// неверные креды — username не найден ИЛИ пароль не совпал — отвечают единым
// 401, чтобы не раскрывать, какая часть неверна. При успехе выдаётся пара:
// access-JWT (TTL 15 минут) и непрозрачный refresh (в БД — только его хэш).
func (s *Server) PostAuthLogin(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var req LoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_body", "тело запроса не является валидным JSON")
		return
	}
	if strings.TrimSpace(req.Username) == "" || strings.TrimSpace(req.Password) == "" {
		writeError(w, http.StatusBadRequest, "validation_error", "username и password обязательны")
		return
	}

	user, err := s.queries.GetUserByUsername(ctx, req.Username)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeInvalidCredentials(w)
			return
		}
		s.logError("GetUserByUsername", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	ok, err := auth.VerifyPassword(req.Password, user.PasswordHash)
	if err != nil {
		// Повреждённый/несовместимый хэш — ошибка данных, а не «пароль неверен», но
		// наружу всё равно отдаём единый 401 (FR A3): протокол не должен
		// сигнализировать, что проблема на стороне сервера для конкретного аккаунта.
		s.logError("VerifyPassword", err)
		writeInvalidCredentials(w)
		return
	}
	if !ok {
		writeInvalidCredentials(w)
		return
	}

	pair, err := s.issueTokenPair(ctx, user.ID)
	if err != nil {
		s.logError("issueTokenPair", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}
	writeJSON(w, http.StatusOK, pair)
}

// PostAuthRefresh реализует POST /auth/refresh — ротацию пары токенов по
// валидному refresh-токену (FR A3).
//
// Бизнес: находим refresh по хэшу предъявленного токена; если не найден, уже
// отозван (revoked) или истёк (expires_at в прошлом) — единый 401, без
// уточнения причины. Иначе — ротация: старый refresh немедленно отзывается,
// выдаётся новая пара (новый access + новый refresh). Старый отзывается ДО
// выдачи новой пары, чтобы при ошибке выдачи новой пары не остаться в
// состоянии «и старый, и новый токен рабочие» (FR A3 — повторное использование
// старого refresh после ротации должно быть невозможно).
func (s *Server) PostAuthRefresh(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var req PostAuthRefreshJSONRequestBody
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_body", "тело запроса не является валидным JSON")
		return
	}
	if strings.TrimSpace(req.RefreshToken) == "" {
		writeError(w, http.StatusBadRequest, "validation_error", "refresh_token обязателен")
		return
	}

	hash := auth.HashRefreshToken(req.RefreshToken)
	existing, err := s.queries.GetRefreshTokenByHash(ctx, hash)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeInvalidRefreshToken(w)
			return
		}
		s.logError("GetRefreshTokenByHash", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}
	if !refreshTokenUsable(existing) {
		writeInvalidRefreshToken(w)
		return
	}

	// Ротация: старый токен отзываем ДО выдачи новой пары (см. godoc выше).
	if err := s.queries.RevokeRefreshTokenByHash(ctx, hash); err != nil {
		s.logError("RevokeRefreshTokenByHash", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	pair, err := s.issueTokenPair(ctx, existing.UserID)
	if err != nil {
		s.logError("issueTokenPair", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}
	writeJSON(w, http.StatusOK, pair)
}

// PostAuthLogout реализует POST /auth/logout — отзыв refresh-токена (FR A3).
//
// Бизнес: помечает предъявленный refresh-токен revoked=true и отвечает 204
// независимо от того, существовал ли такой токен (повторный logout,
// logout с уже отозванным/чужим токеном — везде одинаковый 204): это операция
// «убедиться, что сессия закрыта», а не запрос, требующий подтверждения
// валидности токена — раскрывать через код ответа, существовал ли токен,
// смысла нет (FR A3).
func (s *Server) PostAuthLogout(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var req PostAuthLogoutJSONRequestBody
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_body", "тело запроса не является валидным JSON")
		return
	}
	if strings.TrimSpace(req.RefreshToken) == "" {
		writeError(w, http.StatusBadRequest, "validation_error", "refresh_token обязателен")
		return
	}

	hash := auth.HashRefreshToken(req.RefreshToken)
	if err := s.queries.RevokeRefreshTokenByHash(ctx, hash); err != nil {
		s.logError("RevokeRefreshTokenByHash", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// issueTokenPair выпускает новую пару (access-JWT + непрозрачный refresh) для
// пользователя userID и сохраняет хэш refresh-токена в БД.
//
// Общий шаг для PostAuthLogin и PostAuthRefresh (FR A3) — вынесен, чтобы не
// дублировать выпуск/сохранение токенов в двух обработчиках.
func (s *Server) issueTokenPair(ctx context.Context, userID pgtype.UUID) (TokenPair, error) {
	now := time.Now()

	access, err := auth.IssueAccessToken(userID.String(), s.jwtSigningKey, now)
	if err != nil {
		return TokenPair{}, err
	}

	refreshToken, refreshHash, err := auth.GenerateRefreshToken()
	if err != nil {
		return TokenPair{}, err
	}
	if _, err := s.queries.CreateRefreshToken(ctx, db.CreateRefreshTokenParams{
		UserID:    userID,
		TokenHash: refreshHash,
		ExpiresAt: pgtype.Timestamptz{Time: now.Add(refreshTokenTTL), Valid: true},
	}); err != nil {
		return TokenPair{}, err
	}

	expiresIn := int(auth.AccessTokenTTL.Seconds())
	return TokenPair{
		AccessToken:  access,
		RefreshToken: refreshToken,
		ExpiresIn:    &expiresIn,
	}, nil
}

// refreshTokenUsable сообщает, можно ли обменять refresh-токен на новую пару:
// не отозван и срок действия ещё не истёк (FR A3).
func refreshTokenUsable(t db.RefreshToken) bool {
	if t.Revoked {
		return false
	}
	if !t.ExpiresAt.Valid {
		return false
	}
	return time.Now().Before(t.ExpiresAt.Time)
}

// writeInvalidCredentials отвечает единым 401 на неверные username/password
// (FR A3) — без уточнения, какая часть неверна.
func writeInvalidCredentials(w http.ResponseWriter) {
	writeError(w, http.StatusUnauthorized, "invalid_credentials", "неверные учётные данные")
}

// writeInvalidRefreshToken отвечает единым 401 на нерабочий refresh-токен
// (не найден / отозван / истёк, FR A3) — без уточнения причины.
func writeInvalidRefreshToken(w http.ResponseWriter) {
	writeError(w, http.StatusUnauthorized, "invalid_refresh_token", "refresh-токен недействителен")
}
