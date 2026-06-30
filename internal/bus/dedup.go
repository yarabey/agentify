package bus

// Слой дедупа по message_id (protocol.md §2, §5; ADR 0001 «at-least-once + дедуп
// по message_id»). Доставка в шине — at-least-once, поэтому один и тот же
// message_id может прийти несколько раз (переотправка после реконнекта/таймаута
// ACK). Этот слой гасит повторы на горячем пути консьюмера.
//
// ГРАНИЦА ОТВЕТСТВЕННОСТИ (важно). Этот in-memory дедуп — оперативный, НЕ durable:
// он покрывает «свежие» повторы в окне TTL/размера и переживает только время жизни
// процесса. Долговечная (durable) гарантия «применяется ровно один раз» обеспечивается
// ИДЕМПОТЕНТНЫМ ПРИМЕНЕНИЕМ на стороне приёмника — запись в БД по message_id /
// идемпотентный переход FSM (protocol.md §5: «приёмник хранит обработанные id /
// полагается на offset + идемпотентное применение»). БД сюда не тащим намеренно:
// транспортный слой bus не знает про схему приёмника. Trace: FR E7 (защита от
// дублей), §5 (ACK/offset).

import (
	"container/list"
	"sync"
	"time"
)

// DefaultDedupCapacity — максимальное число запомненных message_id по умолчанию.
// При переполнении вытесняется самый старый (LRU по времени вставки) — кэш не
// растёт бесконечно на одном VPS (MVP, ADR 0001).
const DefaultDedupCapacity = 100_000

// DefaultDedupTTL — время жизни записи о message_id по умолчанию. Покрывает
// разумное окно повторной доставки after-ACK-таймаута (protocol.md §5); за его
// пределами durable-дедуп обеспечивает идемпотентное применение в БД.
const DefaultDedupTTL = 30 * time.Minute

// Deduper — потокобезопасный кэш обработанных message_id с ограничением по
// размеру (LRU-вытеснение) и TTL. Используется консьюмером, чтобы повторный
// message_id не вызывал Handler повторно. Не durable — см. границу в начале файла.
type Deduper struct {
	mu       sync.Mutex
	capacity int
	ttl      time.Duration
	now      func() time.Time // подменяемо в тестах; в проде — time.Now.
	entries  map[string]*list.Element
	order    *list.List // front = самый старый, back = самый новый.
}

// dedupEntry — элемент LRU-списка: message_id и момент его добавления (для TTL).
type dedupEntry struct {
	id    string
	added time.Time
}

// NewDeduper создаёт Deduper с заданными ёмкостью и TTL. Нулевые/отрицательные
// значения заменяются на DefaultDedupCapacity / DefaultDedupTTL — конструктор
// всегда возвращает рабочий кэш.
func NewDeduper(capacity int, ttl time.Duration) *Deduper {
	if capacity <= 0 {
		capacity = DefaultDedupCapacity
	}
	if ttl <= 0 {
		ttl = DefaultDedupTTL
	}
	return &Deduper{
		capacity: capacity,
		ttl:      ttl,
		now:      time.Now,
		entries:  make(map[string]*list.Element, capacity),
		order:    list.New(),
	}
}

// MarkProcessed атомарно проверяет и регистрирует message_id. Возвращает true,
// если id ВИДЕН ВПЕРВЫЕ (вызывающий должен обработать сообщение), и false, если
// это повтор в окне TTL/размера (сообщение уже обрабатывалось — пропустить).
//
// Атомарность check-and-set критична: при конкурентной обработке двух копий
// одного message_id ровно один вызов получит true (Trace: FR E7, «применяется
// один раз»). Истёкшая по TTL запись считается отсутствующей и id регистрируется
// заново как новый.
func (d *Deduper) MarkProcessed(messageID string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := d.now()
	if el, ok := d.entries[messageID]; ok {
		ent := el.Value.(*dedupEntry)
		if now.Sub(ent.added) < d.ttl {
			return false // свежий повтор — гасим.
		}
		// Запись протухла: удаляем, далее зарегистрируем заново как новую.
		d.order.Remove(el)
		delete(d.entries, messageID)
	}

	el := d.order.PushBack(&dedupEntry{id: messageID, added: now})
	d.entries[messageID] = el
	d.evictLocked(now)
	return true
}

// evictLocked вытесняет протухшие по TTL записи с головы списка и, если кэш
// всё ещё переполнен, — самые старые до соблюдения capacity. Вызывается под
// d.mu.
func (d *Deduper) evictLocked(now time.Time) {
	// Сначала чистим протухшие с головы (front = самый старый).
	for {
		front := d.order.Front()
		if front == nil {
			break
		}
		ent := front.Value.(*dedupEntry)
		if now.Sub(ent.added) < d.ttl {
			break
		}
		d.order.Remove(front)
		delete(d.entries, ent.id)
	}
	// Затем соблюдаем ограничение по размеру (LRU по времени вставки).
	for d.order.Len() > d.capacity {
		front := d.order.Front()
		if front == nil {
			break
		}
		ent := front.Value.(*dedupEntry)
		d.order.Remove(front)
		delete(d.entries, ent.id)
	}
}

// Len возвращает текущее число запомненных message_id (для метрик/тестов).
func (d *Deduper) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.order.Len()
}
