-- users.sql — CRUD-запросы аккаунтов (FR A1, A2).
--
-- Назначение (бизнес): доступ закрытый, регистрация только по токену (FR A1).
-- Эти запросы — слой данных под регистрацию/логин (тикеты 1.2/1.3); бизнес-логика
-- (проверка токена, хэширование пароля) живёт в сервисном слое, здесь — только SQL.
-- Схема таблицы — orchestrator/migrations/00001_users_and_auth.sql (не редактируется).

-- name: CreateUser :one
-- Создаёт аккаунт. password_hash — argon2id (FR I1), считается в сервисном слое.
-- is_admin задаётся явно: первый/привилегированный аккаунт видит токен (FR A2).
INSERT INTO users (username, password_hash, is_admin)
VALUES ($1, $2, $3)
RETURNING *;

-- name: GetUserByUsername :one
-- Аккаунт по уникальному username — путь логина (FR A3) и проверки занятости при
-- регистрации (FR A1).
SELECT * FROM users
WHERE username = $1;

-- name: GetUserByID :one
-- Аккаунт по id — для авторизованных запросов после валидации токена доступа.
SELECT * FROM users
WHERE id = $1;

-- name: SetUserAdmin :one
-- Помечает аккаунт администратором (или снимает флаг). Админ видит токен
-- регистрации в plaintext (FR A2).
UPDATE users
SET is_admin = $2
WHERE id = $1
RETURNING *;
