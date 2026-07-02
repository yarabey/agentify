package auth

// jwt.go — выпуск и проверка access-JWT (FR A3, тикет 1.3).
//
// Назначение (бизнес): после успешного логина (POST /auth/login) или ротации
// refresh-токена (POST /auth/refresh) пользователь получает короткоживущий
// access-токен, который web/Telegram-клиент шлёт в заголовке
// Authorization: Bearer <jwt> на каждый запрос. Короткий TTL (15 минут)
// ограничивает окно злоупотребления при утечке токена; долгоживущая часть
// сессии — refresh-токен (см. refresh_token.go), который можно отозвать
// (logout, FR A3). Полноценный auth-middleware, подставляющий user_id в
// контекст каждого запроса, — отдельный тикет 1.4; здесь же закладывается
// единственная проверочная точка (ParseAccessToken), которой тикет 1.4
// переиспользует напрямую, не дублируя логику парсинга/валидации JWT.
//
// Как устроено (тех): используется github.com/golang-jwt/jwt/v5 — выбор
// зафиксирован в docs/01_tech_stack_and_architecture.md §3 («JWT |
// golang-jwt/jwt v5 | access-токен»). Алгоритм подписи — HS256 (симметричный
// HMAC), ключ — JWT_SIGNING_KEY из окружения (docs/MANUAL_STEPS.md,
// генерируется `openssl rand -base64 48`); ключ передаётся вызывающей
// стороной как []byte и НИКОГДА не хранится/не генерируется в этом пакете —
// дефолтного/пустого ключа здесь нет принципиально, чтобы исключить тихий
// fallback на предсказуемый секрет в проде. claims — стандартные
// RegisteredClaims: Subject = id пользователя (UUID-строка), IssuedAt и
// ExpiresAt. Библиотека jwt/v5 сама проверяет exp/nbf/iat при ParseWithClaims,
// поэтому отдельной ручной проверки срока не требуется.

import (
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

// AccessTokenTTL — время жизни access-JWT: 15 минут (зафиксировано тикетом
// 1.3 и docs/01_tech_stack_and_architecture.md §3, FR A3).
const AccessTokenTTL = 15 * time.Minute

// ErrInvalidAccessToken возвращается ParseAccessToken, когда токен не
// проходит проверку: битая подпись, истёкший срок, неожиданный алгоритм или
// нераспознаваемый subject. Единая ошибка — намеренно: вызывающей стороне
// (HTTP-обработчику) не нужно различать причину, чтобы не давать атакующему
// лишний сигнал (тот же принцип единого 401, что и при логине).
var ErrInvalidAccessToken = errors.New("auth: access-токен недействителен")

// accessClaims — JWT-claims access-токена. Встраивает RegisteredClaims
// (стандартные iss/sub/exp/iat и т.д.); собственных полей сверх Subject не
// заводим — этого достаточно для идентификации пользователя (FR A3).
type accessClaims struct {
	jwt.RegisteredClaims
}

// IssueAccessToken выпускает подписанный access-JWT для пользователя userID
// (UUID-строка), действующий AccessTokenTTL от момента now.
//
// Бизнес: вызывается при успешном логине (POST /auth/login) и при ротации
// refresh-токена (POST /auth/refresh), выдавая клиенту новый короткоживущий
// токен доступа (FR A3). Параметр now передаётся явно (а не берётся внутри
// функции из time.Now()), чтобы вызывающий код и тесты могли детерминированно
// получить уже истёкший токен (см. ParseAccessToken и accessTokenExpiry тесты)
// без подмены глобального времени. signingKey — секрет HMAC (JWT_SIGNING_KEY
// из окружения), пустой ключ не отвергается на этом уровне намеренно: решение
// «что делать с пустым JWT_SIGNING_KEY на старте» принимается на уровне
// конфигурации сервиса (orchestrator/main.go), а не здесь.
func IssueAccessToken(userID string, signingKey []byte, now time.Time) (string, error) {
	claims := accessClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   userID,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(AccessTokenTTL)),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString(signingKey)
	if err != nil {
		return "", fmt.Errorf("auth: подпись access-токена: %w", err)
	}
	return signed, nil
}

// ParseAccessToken проверяет подпись и срок действия access-JWT и возвращает
// id пользователя из Subject claim'а.
//
// Бизнес: единственная проверочная точка для access-токенов в тикете 1.3 —
// используется при выдаче (косвенно, через симметричность с IssueAccessToken)
// и тестами «истёкший access-токен → 401»; тикет 1.4 превратит её в
// HTTP-middleware (Authorization: Bearer), не дублируя логику. Отклоняет
// токен при: неверной подписи, истёкшем sub/exp (библиотека jwt/v5 проверяет
// exp автоматически), неожиданном алгоритме подписи (защита от atk «alg:
// none» и подобных) или нераспознаваемом Subject (должен быть валидный UUID,
// так как IssueAccessToken всегда кладёт туда users.id). Все причины отказа
// схлопываются в единый ErrInvalidAccessToken — вызывающей стороне не нужно
// различать их семантически в HTTP-ответе.
func ParseAccessToken(tokenString string, signingKey []byte) (uuid.UUID, error) {
	claims := &accessClaims{}
	token, err := jwt.ParseWithClaims(tokenString, claims, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("auth: неожиданный метод подписи %v", t.Header["alg"])
		}
		return signingKey, nil
	})
	if err != nil || !token.Valid {
		return uuid.Nil, ErrInvalidAccessToken
	}

	userID, err := uuid.Parse(claims.Subject)
	if err != nil {
		return uuid.Nil, ErrInvalidAccessToken
	}
	return userID, nil
}
