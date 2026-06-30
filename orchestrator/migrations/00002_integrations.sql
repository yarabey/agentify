-- +goose Up
-- ============================================================================
-- 00002 — Интеграции (машины разработки)
-- Бизнес: пользователь описывает свои машины как интеграции, чтобы слать туда задачи.
-- Закрывает: FR B1–B6, Gherkin §2.
-- ============================================================================

CREATE TABLE integrations (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id      UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name         TEXT NOT NULL,                        -- обязательное название (FR B1)
    ip_hint      TEXT,                                 -- опциональная доп. проверка IP (FR B3)
    -- UUID-секрет машины. Храним HMAC для аутентификации (FR B6) и зашифрованный
    -- оригинал для показа владельцу (FR B2, нужен ему при настройке машины). Шифр — app-level AEAD (FR I1).
    uuid_hmac    TEXT NOT NULL UNIQUE,
    uuid_enc     BYTEA NOT NULL,
    status       TEXT NOT NULL DEFAULT 'offline'
                    CHECK (status IN ('online','offline')),  -- FR B4
    last_seen_at TIMESTAMPTZ,                          -- последний heartbeat (FR B4)
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_integrations_user ON integrations(user_id);
COMMENT ON TABLE integrations IS 'Машины пользователя. Аутентификация по UUID-секрету (FR B3,B6).';
COMMENT ON COLUMN integrations.uuid_hmac IS 'HMAC от UUID — для аутентификации машины без хранения секрета в открытом виде.';
COMMENT ON COLUMN integrations.uuid_enc IS 'Зашифрованный UUID — чтобы показать владельцу при настройке (FR B2).';
COMMENT ON COLUMN integrations.status IS 'online/offline по heartbeat через шину, без живого соединения (FR B4).';

-- +goose Down
DROP TABLE integrations;
