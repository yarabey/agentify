package bus

import (
	"sync"
	"testing"
	"time"
)

// TestDeduperFirstSeenThenRepeat проверяет ядро дедупа: первый раз message_id
// виден (true → обработать), повтор гасится (false → пропустить). Это и есть
// «дубль применяется один раз» на уровне логики (FR E7).
func TestDeduperFirstSeenThenRepeat(t *testing.T) {
	d := NewDeduper(0, 0)
	if !d.MarkProcessed("msg-1") {
		t.Fatal("первый message_id должен быть новым (true)")
	}
	if d.MarkProcessed("msg-1") {
		t.Fatal("повтор message_id должен гаситься (false)")
	}
	if d.MarkProcessed("msg-2") {
		// другой id — снова новый.
	} else {
		t.Fatal("другой message_id должен быть новым (true)")
	}
}

// TestDeduperTTLExpiry проверяет, что после истечения TTL запись считается
// отсутствующей и id регистрируется заново (граница оперативного дедупа; durable —
// идемпотентное применение в БД).
func TestDeduperTTLExpiry(t *testing.T) {
	d := NewDeduper(10, time.Minute)
	base := time.Unix(0, 0)
	cur := base
	d.now = func() time.Time { return cur }

	if !d.MarkProcessed("m") {
		t.Fatal("первый раз — новый")
	}
	if d.MarkProcessed("m") {
		t.Fatal("в окне TTL — повтор гасится")
	}
	cur = base.Add(2 * time.Minute) // вышли за TTL.
	if !d.MarkProcessed("m") {
		t.Fatal("после TTL id снова новый")
	}
}

// TestDeduperCapacityEviction проверяет LRU-вытеснение по размеру: при превышении
// ёмкости самый старый id забывается, и его повтор снова считается новым.
func TestDeduperCapacityEviction(t *testing.T) {
	d := NewDeduper(2, time.Hour)
	d.MarkProcessed("a")
	d.MarkProcessed("b")
	d.MarkProcessed("c") // вытесняет "a" (самый старый).

	if d.Len() != 2 {
		t.Fatalf("ожидалось 2 записи, got=%d", d.Len())
	}
	if !d.MarkProcessed("a") {
		t.Fatal("вытесненный 'a' должен снова считаться новым")
	}
	if d.MarkProcessed("c") {
		t.Fatal("'c' ещё в кэше — повтор гасится")
	}
}

// TestDeduperConcurrentSingleApply проверяет атомарность check-and-set: при
// конкурентной обработке одного message_id ровно один вызов получает true
// (сообщение применяется один раз даже при гонке потоков). Trace: FR E7.
func TestDeduperConcurrentSingleApply(t *testing.T) {
	d := NewDeduper(0, 0)
	const n = 200
	var wg sync.WaitGroup
	var trueCount int64
	var mu sync.Mutex
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			if d.MarkProcessed("same-id") {
				mu.Lock()
				trueCount++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if trueCount != 1 {
		t.Fatalf("ожидался ровно один true (применяется один раз), got=%d", trueCount)
	}
}

// TestNewDeduperDefaults проверяет, что нулевые параметры заменяются на дефолты.
func TestNewDeduperDefaults(t *testing.T) {
	d := NewDeduper(0, 0)
	if d.capacity != DefaultDedupCapacity || d.ttl != DefaultDedupTTL {
		t.Fatalf("дефолты не применены: cap=%d ttl=%v", d.capacity, d.ttl)
	}
}
