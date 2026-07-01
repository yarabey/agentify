package auth

// refresh_token.go — генерация и хэширование непрозрачных refresh-токенов
// (FR A3, тикет 1.3).
//
// Назначение (бизнес): refresh-токен — долгоживущая часть сессии, которой
// клиент обменивает на новую пару токенов (POST /auth/refresh) без повторного
// ввода пароля, и которую можно отозвать по запросу (POST /auth/logout) или
// при ротации (старый помечается revoked при каждом /auth/refresh). В отличие
// от access-токена это НЕ JWT: это случайная строка без встроенной полезной
// нагрузки («непрозрачный», opaque token), поэтому единственный способ её
// «отозвать» — пометить запись в БД revoked, чего с самоподписанным JWT
// сделать нельзя без отдельного списка отзыва. Сам токен НИКОГДА не попадает
// в БД — хранится только его хэш (token_hash, см.
// orchestrator/migrations/00001_users_and_auth.sql), чтобы утечка БД не
// давала готовых рабочих сессий, аналогично тому, как пароли не хранятся в
// открытом виде (FR I1) для password.go.
//
// Как устроено (тех): токен — 32 случайных байта (256 бит энтропии, тот же
// порядок, что у access-токенов и распространённых opaque-токенов наподобие
// GitHub PAT) из crypto/rand, закодированные base64 (URL-safe, без паддинга)
// для удобной передачи в JSON/заголовках. Хэш — SHA-256 от самого токена,
// hex-encoded. Сознательно НЕ argon2id: argon2id — memory-hard KDF для
// низкоэнтропийных секретов (паролей), которые атакующий мог бы перебрать;
// здесь токен уже обладает 256 битами криптографической энтропии, перебор
// невозможен в принципе, а memory-hard хэширование только добавило бы
// задержку на каждый /auth/refresh без выигрыша в безопасности. Простой
// быстрый SHA-256 — стандартный паттерн для хэширования высокоэнтропийных
// opaque-токенов и соответствует комментарию схемы БД («sha-256/HMAC от
// непрозрачного токена»); HMAC с серверным ключом не даёт здесь
// дополнительной защиты (подделать токен без знания самого токена всё равно
// нельзя), поэтому выбран более простой вариант без отдельного ключа.

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
)

// refreshTokenBytes — длина случайной части refresh-токена в байтах (256 бит
// энтропии, см. godoc пакета).
const refreshTokenBytes = 32

// GenerateRefreshToken генерирует новый непрозрачный refresh-токен и
// возвращает его вместе с хэшем для хранения в БД.
//
// Бизнес: вызывается при логине (FR A3) и при каждой ротации /auth/refresh —
// клиенту отдаётся token (в теле ответа TokenPair.refresh_token), в БД
// (refresh_tokens.token_hash) сохраняется ТОЛЬКО hash. Возвращает ошибку лишь
// при сбое источника случайности.
func GenerateRefreshToken() (token string, hash string, err error) {
	b := make([]byte, refreshTokenBytes)
	if _, err := rand.Read(b); err != nil {
		return "", "", fmt.Errorf("auth: генерация refresh-токена: %w", err)
	}
	token = base64.RawURLEncoding.EncodeToString(b)
	return token, HashRefreshToken(token), nil
}

// HashRefreshToken возвращает hex-encoded SHA-256 от token — то же значение,
// что хранится в refresh_tokens.token_hash и по которому ведётся поиск при
// /auth/refresh и /auth/logout (FR A3). Детерминирована: одинаковый token
// всегда даёт одинаковый hash, что и требуется для поиска по точному
// совпадению (UNIQUE на token_hash).
func HashRefreshToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}
