-- refresh_tokens.sql — запросы непрозрачных refresh-токенов (FR A3).
--
-- Назначение (бизнес): refresh/logout-поток (FR A3). Храним только ХЭШ токена
-- (sha-256/HMAC), сам токен не персистится; сверка и хэширование — в сервисном
-- слое (тикеты 1.3/1.7). Эти запросы — слой данных: вставка, поиск по хэшу, отзыв.
-- Схема — orchestrator/migrations/00001_users_and_auth.sql (не редактируется).

-- name: CreateRefreshToken :one
-- Сохраняет выданный refresh-токен (хэш, владелец, срок). token_hash уникален.
INSERT INTO refresh_tokens (user_id, token_hash, expires_at)
VALUES ($1, $2, $3)
RETURNING *;

-- name: GetRefreshTokenByHash :one
-- Находит токен по хэшу — для валидации при refresh (проверку revoked/expires_at
-- делает сервисный слой; FR A3).
SELECT * FROM refresh_tokens
WHERE token_hash = $1;

-- name: RevokeRefreshTokenByHash :exec
-- Отзывает конкретный токен по хэшу (logout текущей сессии; FR A3).
UPDATE refresh_tokens
SET revoked = TRUE
WHERE token_hash = $1;

-- name: RevokeAllRefreshTokensForUser :exec
-- Отзывает все токены пользователя (logout со всех устройств / смена пароля; FR A3).
UPDATE refresh_tokens
SET revoked = TRUE
WHERE user_id = $1;
