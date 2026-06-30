-- integrations.sql — CRUD-запросы интеграций (FR B1, B2, B5).
--
-- Назначение (бизнес): пользователь описывает свои машины как интеграции
-- (Gherkin §2 «Управление интеграциями»), чтобы направлять в них задачи.
-- Изоляция владельца — на уровне самого SQL (owner-scoped WHERE), а не только
-- в Go-коде: defense-in-depth с RLS-архитектурой тикета 1.5, чужая
-- интеграция должна быть неотличима от несуществующей (единый 404).
-- Бизнес-логика (генерация UUID, HMAC/AEAD) живёт в сервисном слое (тикет 2.2,
-- internal/crypto); здесь — только SQL. Схема — orchestrator/migrations/00002_integrations.sql
-- (не редактируется).

-- name: CreateIntegration :one
-- Вставляет новую интеграцию владельца user_id. uuid_hmac/uuid_enc уже
-- посчитаны в сервисном слое (HMAC и AEAD от случайно сгенерированного UUID);
-- status стартует как 'offline' (машина ещё не подключалась, FR B4).
INSERT INTO integrations (user_id, name, ip_hint, uuid_hmac, uuid_enc, status)
VALUES ($1, $2, $3, $4, $5, 'offline')
RETURNING *;

-- name: ListIntegrationsByUser :many
-- Список интеграций владельца — только свои (FR A4, I3, Gherkin §2). Без
-- секрета (uuid_enc) в результате сознательно НЕ ограничиваем: вызывающая
-- сторона (Server.GetIntegrations) просто не кладёт его в ответ.
SELECT * FROM integrations
WHERE user_id = $1
ORDER BY created_at;

-- name: GetIntegrationByIDAndUser :one
-- Интеграция по id, owner-scoped прямо в SQL: чужая интеграция не найдётся
-- (pgx.ErrNoRows), что отдаёт владельцу единый 404 — не давая атакующему
-- сигнал о существовании чужого id (тот же принцип, что и в auth).
SELECT * FROM integrations
WHERE id = $1 AND user_id = $2;

-- name: UpdateIntegration :one
-- Частичное обновление name/ip_hint (PATCH /integrations/{id}, FR B5):
-- значения для записи считает сервисный слой (берёт текущие там, где поле
-- не пришло в теле запроса), здесь — безусловная перезапись + updated_at.
-- owner-scoped WHERE — как и у Get, не находит чужую строку.
UPDATE integrations
SET name = $3, ip_hint = $4, updated_at = now()
WHERE id = $1 AND user_id = $2
RETURNING *;

-- name: GetIntegrationByUUIDHMAC :one
-- Назначение (бизнес): аутентификация машины на WS-handshake /machine/ws
-- (тикет 2.3, FR B3, B6) — агент в первом кадре (`hello`) предъявляет
-- plaintext UUID-секрет; сервисный слой считает его HMAC-отпечаток (тем же
-- ключом, что и при создании, internal/crypto) и ищет интеграцию по нему.
-- НАМЕРЕННО без фильтра по user_id (в отличие от GetIntegrationByIDAndUser):
-- на этом шаге владелец ещё не известен — сам поиск по uuid_hmac и есть
-- способ его установить (тот же паттерн, что у GetUserByUsername при логине,
-- см. orchestrator/internal/api/auth.go). Не находит — pgx.ErrNoRows;
-- сервисный слой схлопывает это с несовпадением ip_hint в единый отказ
-- WS-аутентификации (close 4401), без утечки причины.
SELECT * FROM integrations
WHERE uuid_hmac = $1;
