// Unit-тесты непрозрачных refresh-токенов (тикет 1.3, FR A3).
//
// Проверяют ключевые инварианты: токены уникальны и непредсказуемы, хэш не
// раскрывает токен (аналог FR I1 для refresh-сессий) и детерминирован для
// одного и того же токена (нужно для поиска по token_hash при /auth/refresh
// и /auth/logout).
package auth

import (
	"strings"
	"testing"
)

func TestGenerateRefreshTokenUniqueAndHashed(t *testing.T) {
	token1, hash1, err := GenerateRefreshToken()
	if err != nil {
		t.Fatalf("GenerateRefreshToken #1: %v", err)
	}
	token2, hash2, err := GenerateRefreshToken()
	if err != nil {
		t.Fatalf("GenerateRefreshToken #2: %v", err)
	}

	if token1 == token2 {
		t.Fatal("два сгенерированных refresh-токена совпали")
	}
	if hash1 == hash2 {
		t.Fatal("хэши двух разных токенов совпали")
	}

	// Хэш не должен содержать сам токен в открытом виде (аналог FR I1).
	if strings.Contains(hash1, token1) {
		t.Fatal("hash содержит исходный токен в открытом виде")
	}
	if token1 == "" || hash1 == "" {
		t.Fatal("пустой токен или хэш")
	}
}

// TestHashRefreshTokenDeterministic — одинаковый токен всегда даёт одинаковый
// хэш (нужно для точного поиска по UNIQUE token_hash при /auth/refresh).
func TestHashRefreshTokenDeterministic(t *testing.T) {
	token, hash, err := GenerateRefreshToken()
	if err != nil {
		t.Fatalf("GenerateRefreshToken: %v", err)
	}
	if got := HashRefreshToken(token); got != hash {
		t.Fatalf("HashRefreshToken(token) = %q, ожидалось %q (равно хэшу из GenerateRefreshToken)", got, hash)
	}
}

// TestHashRefreshTokenDiffersForDifferentInputs — разные токены дают разные
// хэши (без коллизий в простых случаях).
func TestHashRefreshTokenDiffersForDifferentInputs(t *testing.T) {
	if HashRefreshToken("token-a") == HashRefreshToken("token-b") {
		t.Fatal("разные токены дали одинаковый хэш")
	}
}
