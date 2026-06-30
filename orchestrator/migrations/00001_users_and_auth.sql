-- +goose Up
-- ============================================================================
-- 00001 — Пользователи и аутентификация
-- Бизнес: закрытый доступ по токену регистрации; межпользовательская изоляция.
-- Закрывает: FR A1–A4, Gherkin §1.
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS pgcrypto; -- для gen_random_uuid()

CREATE TABLE users (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username      TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,                       -- argon2id, не plaintext (FR I1)
    is_admin      BOOLEAN NOT NULL DEFAULT FALSE,      -- админ видит токен регистрации (FR A2)
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE users IS 'Аккаунты. Доступ закрытый, регистрация только по токену (FR A1).';

-- Токен регистрации. В MVP — один активный, виден админу в plaintext (FR A2);
-- ротация/отзыв и срок — post-MVP, поэтому пока без сложной модели.
CREATE TABLE registration_tokens (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    token      TEXT NOT NULL UNIQUE,                   -- виден администратору (FR A2)
    is_active  BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE registration_tokens IS 'Секретный токен регистрации. Утечка не критична (FR A2).';

-- Refresh-токены: храним ХЭШ, чтобы поддержать logout/отзыв (FR A3).
CREATE TABLE refresh_tokens (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id    UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash TEXT NOT NULL UNIQUE,                   -- sha-256/HMAC от непрозрачного токена
    revoked    BOOLEAN NOT NULL DEFAULT FALSE,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_refresh_tokens_user ON refresh_tokens(user_id);
COMMENT ON TABLE refresh_tokens IS 'Непрозрачные refresh-токены (хэш). Поддержка logout/refresh (FR A3).';

-- +goose Down
DROP TABLE refresh_tokens;
DROP TABLE registration_tokens;
DROP TABLE users;
