// Unit-тесты генерации кода привязки (тикет 9.6, FR D3) БЕЗ БД: формат/алфавит
// кода и дефолт TTL — чистые функции, интеграционный сценарий с реальной
// вставкой в channel_link_codes и проверкой TTL — в
// codegen_integration_test.go (тег integration).
package channel

import (
	"strings"
	"testing"
	"time"
)

// TestGenerateLinkCode_FormatAndAlphabet — код состоит РОВНО из
// linkCodeLength символов алфавита linkCodeAlphabet (тикет 9.6): удобство
// ручного ввода в Telegram (`/start <code>`) и отсутствие визуально
// спутываемых символов.
func TestGenerateLinkCode_FormatAndAlphabet(t *testing.T) {
	for i := 0; i < 100; i++ {
		code, err := generateLinkCode()
		if err != nil {
			t.Fatalf("generateLinkCode: %v", err)
		}
		if len(code) != linkCodeLength {
			t.Fatalf("len(code) = %d, ожидалось %d (code=%q)", len(code), linkCodeLength, code)
		}
		for _, r := range code {
			if !strings.ContainsRune(linkCodeAlphabet, r) {
				t.Fatalf("код %q содержит символ %q вне linkCodeAlphabet", code, r)
			}
		}
	}
}

// TestGenerateLinkCode_Randomness — 200 подряд генераций не дают дубля (40
// бит энтропии, см. godoc linkCodeLength) — грубая проверка, что
// crypto/rand реально используется как источник случайности, а не
// возвращается предсказуемое значение.
func TestGenerateLinkCode_Randomness(t *testing.T) {
	seen := make(map[string]struct{}, 200)
	for i := 0; i < 200; i++ {
		code, err := generateLinkCode()
		if err != nil {
			t.Fatalf("generateLinkCode: %v", err)
		}
		if _, dup := seen[code]; dup {
			t.Fatalf("код %q повторился за 200 генераций — подозрение на недостаточную энтропию", code)
		}
		seen[code] = struct{}{}
	}
}

// TestNewCodeIssuer_NonPositiveTTLFallsBackToDefault — неположительный ttl
// (включая нулевое значение непереданного/нулевого конфига) заменяется на
// DefaultLinkCodeTTL (см. godoc NewCodeIssuer) — так по ошибке нельзя
// выпустить код с уже истёкшим или фактически бессрочным сроком действия.
func TestNewCodeIssuer_NonPositiveTTLFallsBackToDefault(t *testing.T) {
	for _, ttl := range []time.Duration{0, -time.Minute} {
		issuer := NewCodeIssuer(nil, ttl)
		if issuer.ttl != DefaultLinkCodeTTL {
			t.Fatalf("ttl = %v при переданном %v, ожидался DefaultLinkCodeTTL (%v)", issuer.ttl, ttl, DefaultLinkCodeTTL)
		}
	}
}

// TestNewCodeIssuer_PositiveTTLIsUsedAsIs — положительный ttl сохраняется как
// есть, без подмены на дефолт.
func TestNewCodeIssuer_PositiveTTLIsUsedAsIs(t *testing.T) {
	const custom = 30 * time.Minute
	issuer := NewCodeIssuer(nil, custom)
	if issuer.ttl != custom {
		t.Fatalf("ttl = %v, ожидался переданный %v", issuer.ttl, custom)
	}
}
