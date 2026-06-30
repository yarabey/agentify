-- registration_tokens.sql — запросы секретного токена регистрации (FR A1, A2).
--
-- Назначение (бизнес): регистрация закрыта и идёт только по активному токену
-- (FR A1); токен виден администратору в plaintext (FR A2). В MVP — один активный
-- токен, без ротации/срока. Эти запросы — слой данных под тикеты 1.2/1.6; сверку
-- предъявленного токена с активным делает сервисный слой.
-- Схема — orchestrator/migrations/00001_users_and_auth.sql (не редактируется).

-- name: CreateRegistrationToken :one
-- Создаёт активный токен регистрации (token — секрет, генерируется в сервисе).
INSERT INTO registration_tokens (token)
VALUES ($1)
RETURNING *;

-- name: GetActiveRegistrationToken :one
-- Текущий активный токен — для проверки при регистрации (FR A1) и показа админу
-- (FR A2). В MVP активный токен один; берём самый свежий на случай инвариант-сбоя.
SELECT * FROM registration_tokens
WHERE is_active = TRUE
ORDER BY created_at DESC
LIMIT 1;

-- name: DeactivateRegistrationToken :exec
-- Деактивирует токен по id (ротация/отзыв) — переводит is_active в FALSE.
UPDATE registration_tokens
SET is_active = FALSE
WHERE id = $1;
