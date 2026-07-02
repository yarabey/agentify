// Package outbox — локальный durable outbox агента (тикет 3.5,
// docs/protocol.md §1, §5 "События агент → оркестратор").
//
// Назначение (бизнес): агент держит исходящее WS-соединение к оркестратору
// (agent/internal/wsclient, тикет 3.3), но сама «труба» — не durable: она
// рвётся при падении/рестарте ЛЮБОЙ из сторон (сеть, рестарт оркестратора,
// рестарт самого агента). Протокол (§1) требует, чтобы надёжность жила «на
// обоих концах»: на сервере — Redpanda-бэкбон (тикет 3.4), на агенте —
// локальный durable outbox. Это и есть тот outbox: событие агент→оркестратор
// сначала durable-записывается СЮДА (переживает падение процесса агента,
// потому что физически лежит в файле на диске), и только потом делается
// попытка отправить его по сети; удаляется запись отсюда ТОЛЬКО после
// подтверждения (`ack`) оркестратора (§5, шаги 1-3). Если оркестратор упал
// между отправкой и ack — событие остаётся в Store и будет переотправлено
// при следующем подключении (реплей, FR E3, §125) — таков ровно сценарий
// приёмки тикета 3.5 «убить оркестратор на время → события агента не
// потеряны».
//
// Как устроено (тех): Store — тонкая обёртка над одним файлом bbolt
// (go.etcd.io/bbolt, docs/01_tech_stack_and_architecture.md строка 130:
// встроенный, без внешних зависимостей движок key-value на диске,
// ACID-транзакции — то, что даёт нам durability «пережить SIGKILL
// процесса»). Два bucket'а:
//   - eventsBucket — FIFO-очередь: ключ — 8-байтовый big-endian счётчик
//     bbolt.NextSequence() (monotonic, per-bucket), значение — JSON конверта
//     (bus.Envelope.Marshal). Ключ по счётчику, а НЕ по message_id (UUID) —
//     принципиально: UUID не сортируется по времени вставки, а порядок
//     вставки — это и есть порядок доставки (в рамках task_id это следствие
//     общего FIFO, см. godoc wsclient).
//   - indexBucket — message_id → ключ eventsBucket той же записи. Нужен,
//     чтобы Delete(messageID) (вызывается ПОСЛЕ ack, см. wsclient) находил
//     нужную запись за O(1), не сканируя всю очередь.
//
// Оба bucket'а живут в одной bbolt-транзакции на запись (Update) — Enqueue и
// Delete атомарны относительно обоих индексов одновременно.
package outbox

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go.etcd.io/bbolt"

	"github.com/yarabey/agentify/internal/bus"
)

// openTimeout — сколько Open ждёт файловую блокировку bbolt (flock), прежде
// чем сдаться. bbolt-файл эксклюзивно блокируется процессом, который его
// открыл (docs/01_tech_stack_and_architecture.md: одиночный процесс агента
// на файл outbox) — конечный таймаут вместо бесконечного ожидания нужен,
// чтобы сломанный/зависший предыдущий процесс агента на той же машине не
// вешал запуск нового процесса навечно, а давал понятную ошибку.
const openTimeout = 5 * time.Second

// eventsBucket — FIFO-очередь исходящих событий: seq-ключ → JSON конверта
// (см. godoc пакета).
var eventsBucket = []byte("events")

// indexBucket — message_id → seq-ключ соответствующей записи eventsBucket
// (см. godoc пакета).
var indexBucket = []byte("message_index")

// ErrEmptyMessageID — Enqueue вызван с конвертом без message_id: Delete по
// такому событию впоследствии не сможет его найти (индекс строится по
// message_id), поэтому это ошибка вызывающего, а не тихо принимаемый случай.
var ErrEmptyMessageID = errors.New("outbox: пустой message_id конверта")

// Store — durable-очередь исходящих событий агента поверх одного файла
// bbolt (см. godoc пакета). Собирается Open; закрывается Close.
type Store struct {
	db *bbolt.DB
}

// Open открывает (создавая при отсутствии — включая родительские каталоги)
// файл outbox по указанному пути и заводит оба bucket'а (см. godoc пакета),
// если их ещё нет — идемпотентно относительно повторных запусков агента:
// уже существующий файл открывается как есть, накопленные ранее
// неподтверждённые события никуда не деваются (это и есть durability across
// restart, приёмка тикета).
func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("outbox: создание каталога для %s: %w", path, err)
	}

	db, err := bbolt.Open(path, 0o600, &bbolt.Options{Timeout: openTimeout})
	if err != nil {
		return nil, fmt.Errorf("outbox: открытие bbolt-файла %s: %w", path, err)
	}

	if err := db.Update(func(tx *bbolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(eventsBucket); err != nil {
			return fmt.Errorf("bucket %s: %w", eventsBucket, err)
		}
		if _, err := tx.CreateBucketIfNotExists(indexBucket); err != nil {
			return fmt.Errorf("bucket %s: %w", indexBucket, err)
		}
		return nil
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("outbox: инициализация bucket'ов: %w", err)
	}

	return &Store{db: db}, nil
}

// Enqueue durable-записывает событие в конец очереди (см. godoc пакета —
// вызывающий, wsclient.Client.SendEvent, обязан делать это ДО любой попытки
// сетевой отправки: сама запись в bbolt-файл (fsync внутри bbolt-транзакции)
// и есть гарантия «событие переживёт падение процесса агента»). Возвращает
// ErrEmptyMessageID, если у конверта не проставлен message_id (Delete по
// нему впоследствии был бы невозможен).
func (s *Store) Enqueue(env bus.Envelope) error {
	if env.MessageID == "" {
		return ErrEmptyMessageID
	}
	data, err := env.Marshal()
	if err != nil {
		return fmt.Errorf("outbox: маршалинг конверта message_id=%s: %w", env.MessageID, err)
	}

	return s.db.Update(func(tx *bbolt.Tx) error {
		events := tx.Bucket(eventsBucket)
		index := tx.Bucket(indexBucket)

		seq, err := events.NextSequence()
		if err != nil {
			return fmt.Errorf("следующий seq: %w", err)
		}
		key := seqKey(seq)

		if err := events.Put(key, data); err != nil {
			return fmt.Errorf("запись события: %w", err)
		}
		if err := index.Put([]byte(env.MessageID), key); err != nil {
			return fmt.Errorf("запись индекса message_id: %w", err)
		}
		return nil
	})
}

// Pending возвращает все неподтверждённые (ещё не удалённые Delete) события
// в порядке добавления (FIFO по seq-ключу eventsBucket, см. godoc пакета) —
// именно в этом порядке wsclient.Client переотправляет их при (пере)подключении.
func (s *Store) Pending() ([]bus.Envelope, error) {
	var out []bus.Envelope
	err := s.db.View(func(tx *bbolt.Tx) error {
		events := tx.Bucket(eventsBucket)
		return events.ForEach(func(_, data []byte) error {
			var env bus.Envelope
			if err := json.Unmarshal(data, &env); err != nil {
				return fmt.Errorf("демаршалинг события из outbox: %w", err)
			}
			out = append(out, env)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Delete убирает событие с данным message_id из очереди (вызывается ПОСЛЕ
// получения ack от оркестратора, см. godoc пакета и wsclient). Неизвестный
// (уже удалённый / никогда не существовавший) message_id — безопасный
// no-op: at-least-once допускает повторные/поздние ack, дублирующийся вызов
// Delete не должен быть ошибкой.
func (s *Store) Delete(messageID string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		index := tx.Bucket(indexBucket)
		key := index.Get([]byte(messageID))
		if key == nil {
			return nil // неизвестный id — no-op, см. godoc.
		}
		events := tx.Bucket(eventsBucket)
		if err := events.Delete(key); err != nil {
			return fmt.Errorf("удаление события: %w", err)
		}
		if err := index.Delete([]byte(messageID)); err != nil {
			return fmt.Errorf("удаление индекса message_id: %w", err)
		}
		return nil
	})
}

// Close закрывает файл bbolt, снимая файловую блокировку (см. openTimeout) —
// без вызова Close повторный Open того же пути из другого процесса/теста
// заблокируется до истечения таймаута.
func (s *Store) Close() error {
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("outbox: закрытие bbolt-файла: %w", err)
	}
	return nil
}

// seqKey кодирует monotonic-счётчик bbolt.NextSequence() как 8-байтовый
// big-endian ключ — сортировка ключей bbolt (лексикографическая по байтам)
// тем самым совпадает с числовым порядком счётчика, то есть с порядком
// вставки (FIFO, см. godoc пакета).
func seqKey(seq uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, seq)
	return key
}
