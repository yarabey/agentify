-- +goose Up
-- ============================================================================
-- 00004 — Привязка каналов (Telegram)
-- Бизнес: связать Telegram-аккаунт пользователя с аккаунтом в системе понятным флоу.
-- Закрывает: FR D3, Gherkin §6.
-- ============================================================================

-- Одноразовый код привязки: web генерирует, бот обменивает на привязку.
CREATE TABLE channel_link_codes (
    code       TEXT PRIMARY KEY,
    user_id    UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    channel    TEXT NOT NULL CHECK (channel IN ('telegram')),
    expires_at TIMESTAMPTZ NOT NULL,
    used_at    TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE channel_link_codes IS 'Одноразовые коды привязки канала (FR D3).';

-- Готовая привязка внешнего канала к аккаунту.
CREATE TABLE channel_links (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id           UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    channel           TEXT NOT NULL CHECK (channel IN ('telegram')),
    external_id       TEXT NOT NULL,                   -- напр. telegram_user_id
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (channel, external_id)
);
CREATE INDEX idx_channel_links_user ON channel_links(user_id);
COMMENT ON TABLE channel_links IS 'Привязка внешнего канала (Telegram) к аккаунту (FR D3).';

-- +goose Down
DROP TABLE channel_links;
DROP TABLE channel_link_codes;
