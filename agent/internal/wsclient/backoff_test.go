package wsclient

import (
	"testing"
	"time"
)

// TestBackoffDelayWithinCap проверяет full-jitter инвариант backoffDelay
// (см. godoc): для любой попытки задержка лежит в [0, min(max, base*2^attempt)).
func TestBackoffDelayWithinCap(t *testing.T) {
	base := 10 * time.Millisecond
	maxDelay := 100 * time.Millisecond

	cases := []struct {
		attempt int
		wantCap time.Duration
	}{
		{0, 10 * time.Millisecond},
		{1, 20 * time.Millisecond},
		{2, 40 * time.Millisecond},
		{3, 80 * time.Millisecond},
		{4, 100 * time.Millisecond}, // capped at max (160ms would exceed)
		{50, 100 * time.Millisecond},
	}
	for _, tc := range cases {
		for i := 0; i < 50; i++ {
			d := backoffDelay(tc.attempt, base, maxDelay)
			if d < 0 || d >= tc.wantCap {
				t.Fatalf("attempt=%d: delay=%s вне ожидаемого диапазона [0, %s)", tc.attempt, d, tc.wantCap)
			}
		}
	}
}

// TestBackoffDelayNegativeAttemptTreatedAsZero проверяет, что отрицательный
// attempt трактуется как 0 (см. godoc backoffDelay).
func TestBackoffDelayNegativeAttemptTreatedAsZero(t *testing.T) {
	base := 10 * time.Millisecond
	maxDelay := 100 * time.Millisecond
	for i := 0; i < 50; i++ {
		d := backoffDelay(-5, base, maxDelay)
		if d < 0 || d >= base {
			t.Fatalf("attempt=-5: delay=%s вне ожидаемого диапазона [0, %s)", d, base)
		}
	}
}

// TestBackoffDelayDefaultsOnNonPositiveBounds проверяет, что
// base<=0/max<=0 заменяются дефолтами, а не приводят к панике/нулевой
// задержке навсегда.
func TestBackoffDelayDefaultsOnNonPositiveBounds(t *testing.T) {
	d := backoffDelay(0, 0, 0)
	if d < 0 || d >= defaultBackoffBase {
		t.Fatalf("delay=%s вне ожидаемого диапазона [0, %s) при дефолтах", d, defaultBackoffBase)
	}
}
