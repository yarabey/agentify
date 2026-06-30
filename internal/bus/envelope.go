package bus

// Конверт сообщения шины (protocol.md §2). Любое сообщение машина ↔ оркестратор —
// JSON со стабильным конвертом; на нём держатся дедуп (по message_id) и порядок
// (по ключу партиции, ADR 0001). Этот файл — машинное представление §2: тип
// Envelope, хелперы (де)сериализации, генерация message_id и валидация
// обязательных полей. Trace: FR E3 (реплей/надёжная доставка событий), FR E7
// (защита от дублей/идемпотентность), бизнес-ТЗ §126 (порядок в рамках задачи).

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

// ProtocolVersion — текущая версия протокола конверта (protocol.md §2, §7).
// Каждый исходящий конверт несёт это значение в поле protocol_version; контроль
// совместимости — отдельный тикет 4.7 (здесь лишь проставляем версию). Trace: FR C5.
const ProtocolVersion = "1"

// Ошибки валидации конверта. Возвращаются Validate и (Un)marshal-хелперами;
// объявлены как sentinel-значения, чтобы вызывающий мог сверять через errors.Is.
var (
	// ErrMissingMessageID — пустой message_id: дедуп (protocol.md §2) невозможен.
	ErrMissingMessageID = errors.New("bus: пустой message_id")
	// ErrMissingIntegrationID — пустой integration_id: адресация машины невозможна.
	ErrMissingIntegrationID = errors.New("bus: пустой integration_id")
	// ErrMissingType — пустой type: получатель не знает, как трактовать payload.
	ErrMissingType = errors.New("bus: пустой type сообщения")
	// ErrMissingTimestamp — пустой ts: нарушает контракт §2 (RFC3339-метка времени).
	ErrMissingTimestamp = errors.New("bus: пустой ts")
	// ErrMissingProtocolVersion — пустой protocol_version: контроль совместимости (§7) невозможен.
	ErrMissingProtocolVersion = errors.New("bus: пустой protocol_version")
)

// Envelope — конверт сообщения шины строго по protocol.md §2. Сериализуется в
// JSON и кладётся в value записи Redpanda; ключ партиции (ADR 0001) — отдельно,
// его выставляет Producer из соответствующего поля (см. PartitionKey).
//
// Поле TaskID — указатель (*string): для machine-level сообщений (hello/heartbeat)
// task_id == null (protocol.md §2, §4). Payload хранится как json.RawMessage —
// конверт стабилен, а форма payload зависит от type (§4) и разбирается приёмником.
//
// Trace: дедуп по MessageID (FR E7), порядок по ключу партиции (§126), реплей
// событий по конверту (FR E3).
type Envelope struct {
	// MessageID — глобально уникальный идентификатор сообщения; основа дедупа
	// (protocol.md §2). Генерируется NewMessageID при создании конверта.
	MessageID string `json:"message_id"`
	// TaskID — идентификатор задачи; null (nil) только для machine-level
	// сообщений (hello/heartbeat), у остальных типов обязателен (protocol.md §2, §4).
	TaskID *string `json:"task_id"`
	// IntegrationID — идентификатор интеграции (машины); ключ адресации и
	// партиционирования команд (ADR 0001). Обязателен всегда.
	IntegrationID string `json:"integration_id"`
	// Type — тип сообщения (protocol.md §4), определяет трактовку Payload.
	Type string `json:"type"`
	// Seq — порядковый номер в рамках TaskID (монотонный, protocol.md §2);
	// по нему приёмник проверяет порядок событий задачи (§126).
	Seq int64 `json:"seq"`
	// Ts — метка времени в формате RFC3339 (protocol.md §2).
	Ts string `json:"ts"`
	// ProtocolVersion — версия протокола конверта для контроля совместимости
	// (protocol.md §2, §7; FR C5).
	ProtocolVersion string `json:"protocol_version"`
	// Payload — полезная нагрузка, форма зависит от Type (protocol.md §4);
	// конверт хранит её как сырой JSON, не разбирая.
	Payload json.RawMessage `json:"payload"`
}

// NewMessageID возвращает новый глобально уникальный message_id (UUIDv4) — основа
// дедупа (protocol.md §2). Вынесено в функцию, чтобы продьюсеры не дублировали
// генерацию и тесты могли опираться на единый источник идентификаторов.
func NewMessageID() string {
	return uuid.NewString()
}

// Validate проверяет обязательные поля конверта (protocol.md §2). Возвращает
// sentinel-ошибку (errors.Is) на первое нарушение. TaskID не проверяется на
// непустоту намеренно: он nullable для machine-level сообщений (§4) — связь
// type↔task_id валидируется на уровне приёмника/моста, не в транспортном конверте.
func (e *Envelope) Validate() error {
	switch {
	case e.MessageID == "":
		return ErrMissingMessageID
	case e.IntegrationID == "":
		return ErrMissingIntegrationID
	case e.Type == "":
		return ErrMissingType
	case e.Ts == "":
		return ErrMissingTimestamp
	case e.ProtocolVersion == "":
		return ErrMissingProtocolVersion
	}
	return nil
}

// Marshal сериализует конверт в JSON для записи в Redpanda, предварительно
// проверив обязательные поля (Validate). Возвращает ошибку валидации или
// маршалинга — недовалидный конверт в шину не попадёт.
func (e *Envelope) Marshal() ([]byte, error) {
	if err := e.Validate(); err != nil {
		return nil, fmt.Errorf("bus: невалидный конверт: %w", err)
	}
	b, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("bus: маршалинг конверта: %w", err)
	}
	return b, nil
}

// Unmarshal разбирает JSON-конверт из записи Redpanda и валидирует обязательные
// поля (protocol.md §2). Невалидный (битый/неполный) конверт возвращает ошибку,
// чтобы приёмник не применял мусор.
func Unmarshal(data []byte) (Envelope, error) {
	var e Envelope
	if err := json.Unmarshal(data, &e); err != nil {
		return Envelope{}, fmt.Errorf("bus: демаршалинг конверта: %w", err)
	}
	if err := e.Validate(); err != nil {
		return Envelope{}, fmt.Errorf("bus: невалидный конверт: %w", err)
	}
	return e, nil
}

// PartitionKey возвращает значение ключа партиции для конверта согласно ADR 0001
// и переданному имени поля-ключа (PartitionKeyIntegrationID / PartitionKeyTaskID).
// Это хелпер-проверка для продьюсера: гарантирует, что ключ берётся из конверта
// консистентно (а не из произвольной строки), сохраняя порядок по ключу (§126).
//
// Для PartitionKeyTaskID при nil TaskID возвращает ошибку: событие уровня задачи
// обязано иметь task_id, иначе порядок в рамках задачи негарантируем; machine-level
// сообщения должны партиционироваться по integration_id (ADR 0001).
func (e *Envelope) PartitionKey(keyField string) (string, error) {
	switch keyField {
	case PartitionKeyIntegrationID:
		if e.IntegrationID == "" {
			return "", ErrMissingIntegrationID
		}
		return e.IntegrationID, nil
	case PartitionKeyTaskID:
		if e.TaskID == nil || *e.TaskID == "" {
			return "", fmt.Errorf("bus: ключ партиции %q требует непустой task_id (machine-level сообщения партиционируются по %q, ADR 0001)", PartitionKeyTaskID, PartitionKeyIntegrationID)
		}
		return *e.TaskID, nil
	default:
		return "", fmt.Errorf("bus: неизвестное поле ключа партиции %q (ожидались %q/%q, ADR 0001)", keyField, PartitionKeyIntegrationID, PartitionKeyTaskID)
	}
}
