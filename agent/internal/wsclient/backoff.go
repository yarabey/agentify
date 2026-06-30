package wsclient

import (
	"math/rand/v2"
	"time"
)

// defaultBackoffBase/defaultBackoffMax — дефолты экспоненциального backoff
// реконнекта (тикет 3.3, докс MVP_TICKETS.md §3.3 "авто-реконнект с
// бэкоффом"). Конкретные значения не зафиксированы протоколом — 1s база и
// 30s потолок типичны для AWS-style full jitter и не дают агенту ни
// слишком агрессивно долбить оркестратор сразу после падения, ни ждать
// неоправданно долго при кратковременном сетевом сбое.
const (
	defaultBackoffBase = 1 * time.Second
	defaultBackoffMax  = 30 * time.Second
)

// backoffDelay вычисляет задержку перед попыткой реконнекта номер attempt
// (0-индексация: 0 — сразу после первого провала) по алгоритму
// экспоненциального backoff с full jitter (AWS-style,
// https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/):
//
//	cap   = min(max, base * 2^attempt)
//	delay = rand[0, cap)
//
// Полный (full) jitter — НЕ половинный/decorrelated — выбран как самый
// простой вариант, дающий равномерный разброс попыток реконнекта по всему
// диапазону [0, cap) и тем самым избегающий синхронизированных всплесков
// переподключений множества агентов после общего сбоя оркестратора.
//
// attempt < 0 трактуется как 0. base <= 0 / max <= 0 заменяются дефолтами
// (defaultBackoffBase/defaultBackoffMax) — вызывающий (New/WithBackoff) уже
// не должен такое пропускать, но backoffDelay остаётся безопасной даже при
// прямом вызове с нулевыми значениями.
func backoffDelay(attempt int, base, maxDelay time.Duration) time.Duration {
	if base <= 0 {
		base = defaultBackoffBase
	}
	if maxDelay <= 0 {
		maxDelay = defaultBackoffMax
	}
	if attempt < 0 {
		attempt = 0
	}

	delayCap := backoffCap(base, maxDelay, attempt)
	if delayCap <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(delayCap)))
}

// backoffCap вычисляет min(maxDelay, base*2^attempt), останавливая цикл
// удвоения, как только потолок достигнут или умножение переполнило бы
// time.Duration (int64 наносекунд) — для разумных base/maxDelay/attempt,
// используемых конфигом клиента, это не более нескольких десятков итераций.
func backoffCap(base, maxDelay time.Duration, attempt int) time.Duration {
	d := base
	for i := 0; i < attempt; i++ {
		if d >= maxDelay {
			return maxDelay
		}
		next := d * 2
		if next <= d { // переполнение time.Duration
			return maxDelay
		}
		d = next
	}
	if d > maxDelay {
		return maxDelay
	}
	return d
}
