// Package api — каркас HTTP-API оркестратора и реальные обработчики тикетов
// 1.2/1.3/1.4/2.2/2.3/2.4.
//
// Назначение (бизнес): здесь живёт каркас единого REST-API оркестратора (через
// который ходят web PWA и Telegram-бот, см. orchestrator/README.md), а реально
// реализован весь auth-поток: регистрация по токену — POST /auth/register
// (FR A1, Gherkin §1), логин/refresh/logout — POST /auth/login,
// POST /auth/refresh, POST /auth/logout (FR A3), и auth-middleware,
// определяющий пользователя по access-токену на защищённых маршрутах
// (FR A3, D2, Gherkin §1 «Доступ к API по токену», см. middleware.go), CRUD
// интеграций с выдачей UUID-секрета — GET/POST /integrations,
// GET/PATCH/DELETE /integrations/{id} (FR B1, B2, B5, Gherkin §2, см.
// integrations.go; удаление — мягкое, ADR 0004, тикет 2.6, корректно
// отменяет активные задачи интеграции через FSM при confirm=true), а также
// аутентификация машины по UUID на WS-handshake — GET /machine/ws,
// вкл. вытеснение повторного соединения той же интеграции (FR B3, B6,
// Gherkin §2, ADR 0002, см. machine_ws.go). Доступ в систему закрытый:
// аккаунт создаётся лишь при предъявлении активного секретного токена
// регистрации; без него — отказ (FR A1). Остальные операции контракта (задачи)
// пока отвечают 501 Not Implemented и будут реализованы в своих тикетах, но
// уже сейчас проходят через auth-middleware наравне с готовыми защищёнными
// операциями.
//
// Как устроено (тех): Server реализует сгенерированный из openapi.yaml
// api.ServerInterface. Чтобы не писать все операции сразу, Server встраивает
// сгенерированный api.Unimplemented (каждый его метод отдаёт 501) и переопределяет
// только готовые операции — GetHealthz, PostAuthRegister,
// PostAuthLogin/PostAuthRefresh/PostAuthLogout (см. auth.go),
// GetIntegrations/PostIntegrations/GetIntegrationsId/PatchIntegrationsId/
// DeleteIntegrationsId (см. integrations.go) и GetMachineWs (см.
// machine_ws.go), которая после
// успешного hello разбирает входящие кадры машины и пересылает ack-кадры
// зарегистрированному AckSink — мосту оркестратора (machine.commands →
// WS, commit-after-ACK, тикет 3.4, protocol.md §5, см. SetAckSink/MachineConn
// и godoc machine_ws.go). NewRouter монтирует
// chi-роутер из сгенерированного HandlerWithOptions (он же поднимает
// GET /healthz и все маршруты API от корня — Caddy роутит /api/* со стрипом
// префикса, поэтому пути монтируются от корня: /auth/register, /healthz),
// подключая auth-middleware выборочно к защищённым маршрутам (см. NewRouter и
// middleware.go). Слой данных — sqlc *db.Queries поверх pgxpool;
// бизнес-логика (проверка токена, хэширование пароля, шифрование UUID-секрета
// через internal/crypto) живёт в обработчике, SQL — в db.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync"

	"github.com/coder/websocket"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/yarabey/agentify/internal/auth"
	"github.com/yarabey/agentify/internal/bus"
	"github.com/yarabey/agentify/internal/crypto"
	"github.com/yarabey/agentify/orchestrator/internal/db"
	"github.com/yarabey/agentify/orchestrator/internal/notify"
	"github.com/yarabey/agentify/orchestrator/internal/task"
)

// pgUniqueViolation — код ошибки PostgreSQL «нарушение уникального ограничения»
// (SQLSTATE 23505). Используется, чтобы отличить занятый username при вставке и
// вернуть 409, не делая лишнего предварительного SELECT.
const pgUniqueViolation = "23505"

// Querier — узкий интерфейс слоя данных, нужный обработчикам тикетов 1.2/1.3.
//
// Сужает *db.Queries до фактически используемых auth-обработчиками методов: так
// Server не зависит от всего сгенерированного API БД, а тесты могут при
// необходимости подменить слой данных. Реализуется *db.Queries (sqlc) поверх
// pgxpool.
type Querier interface {
	// GetActiveRegistrationToken возвращает действующий токен регистрации (FR A1).
	GetActiveRegistrationToken(ctx context.Context) (db.RegistrationToken, error)
	// CreateUser создаёт аккаунт с argon2id-хэшем пароля (FR A1, I1).
	CreateUser(ctx context.Context, arg db.CreateUserParams) (db.User, error)
	// GetUserByUsername ищет аккаунт по username — путь логина (FR A3).
	GetUserByUsername(ctx context.Context, username string) (db.User, error)
	// GetUserByID ищет аккаунт по id — проверка is_admin для показа токена
	// регистрации (FR A2, тикет 1.6).
	GetUserByID(ctx context.Context, id pgtype.UUID) (db.User, error)
	// CreateRefreshToken сохраняет хэш нового refresh-токена (FR A3).
	CreateRefreshToken(ctx context.Context, arg db.CreateRefreshTokenParams) (db.RefreshToken, error)
	// GetRefreshTokenByHash ищет refresh-токен по хэшу — путь /auth/refresh (FR A3).
	GetRefreshTokenByHash(ctx context.Context, tokenHash string) (db.RefreshToken, error)
	// RevokeRefreshTokenByHash отзывает refresh-токен по хэшу — ротация при
	// /auth/refresh и явный отзыв при /auth/logout (FR A3).
	RevokeRefreshTokenByHash(ctx context.Context, tokenHash string) error

	// CreateIntegration вставляет новую интеграцию владельца с уже посчитанными
	// uuid_hmac/uuid_enc (FR B1, B2, тикет 2.2).
	CreateIntegration(ctx context.Context, arg db.CreateIntegrationParams) (db.Integration, error)
	// ListIntegrationsByUser возвращает интеграции владельца — только свои
	// (FR A4, I3, тикет 2.2).
	ListIntegrationsByUser(ctx context.Context, userID pgtype.UUID) ([]db.Integration, error)
	// GetIntegrationByIDAndUser ищет интеграцию по id, owner-scoped прямо в SQL
	// (FR A4, I3) — чужая/несуществующая неотличимы (pgx.ErrNoRows → 404).
	GetIntegrationByIDAndUser(ctx context.Context, arg db.GetIntegrationByIDAndUserParams) (db.Integration, error)
	// UpdateIntegration частично обновляет name/ip_hint владельца (FR B5).
	UpdateIntegration(ctx context.Context, arg db.UpdateIntegrationParams) (db.Integration, error)
	// GetIntegrationByUUIDHMAC ищет интеграцию по отпечатку UUID-секрета —
	// путь аутентификации машины на WS-handshake /machine/ws, БЕЗ фильтра по
	// user_id: владелец на этом шаге ещё не известен (FR B3, B6, тикет 2.3,
	// см. machine_ws.go).
	GetIntegrationByUUIDHMAC(ctx context.Context, uuidHmac string) (db.Integration, error)
	// ListActiveTaskIDsByIntegration возвращает id активных (не терминальных)
	// задач интеграции — используется DeleteIntegrationsId (тикет 2.6, FR B5)
	// для решения о 409 (без confirm=true) и для отмены через FSM при
	// confirm=true (см. integrations.go).
	ListActiveTaskIDsByIntegration(ctx context.Context, integrationID pgtype.UUID) ([]pgtype.UUID, error)
	// SoftDeleteIntegration помечает интеграцию удалённой (deleted_at, ADR
	// 0004, docs/adr/0004-integration-soft-delete.md) — owner-scoped,
	// идемпотентно относительно повторного вызова (тикет 2.6, FR B5, см.
	// integrations.go).
	SoftDeleteIntegration(ctx context.Context, arg db.SoftDeleteIntegrationParams) (pgtype.UUID, error)

	// CreateTask вставляет новую задачу владельца со статусом 'created'
	// (тикет 5.3, FR E1) — постановку в очередь выполняет отдельно
	// task.Transitioner (см. PostTasks в tasks.go).
	CreateTask(ctx context.Context, arg db.CreateTaskParams) (db.Task, error)
	// GetTaskByUserAndIdempotencyKey ищет задачу по (user_id, idempotency_key)
	// после коллизии CreateTask на uq_tasks_idempotency (SQLSTATE 23505) —
	// дедуп повторной постановки (тикет 5.5, FR E7): повтор возвращает уже
	// созданную задачу с 200 вместо дубля (см. PostTasks в tasks.go).
	GetTaskByUserAndIdempotencyKey(ctx context.Context, arg db.GetTaskByUserAndIdempotencyKeyParams) (db.Task, error)

	// GetTaskByIDAndUser ищет задачу по id, owner-scoped прямо в SQL (FR A4,
	// I3) — чужая/несуществующая неотличимы (pgx.ErrNoRows → 404). Используется
	// PostTasksIdAnswer (тикет 6.1) для проверки владения задачей перед
	// применением ответа пользователя (см. tasks.go).
	GetTaskByIDAndUser(ctx context.Context, arg db.GetTaskByIDAndUserParams) (db.Task, error)
	// GetTaskByIDAndIntegration ищет задачу по id, scoped по integration_id
	// (а не user_id) — путь WS-события агента (тикет 6.1, handleAgentQuestion
	// в machine_ws.go), где аутентифицирована машина, не пользователь.
	GetTaskByIDAndIntegration(ctx context.Context, arg db.GetTaskByIDAndIntegrationParams) (db.Task, error)
	// ListAgentQuestionEventsByTask возвращает события agent_question задачи
	// (самые новые первыми) — источник сопоставления ответа пользователя
	// (question_id) с конкретной записью task_events (тикет 6.1, FR F2, см.
	// PostTasksIdAnswer в tasks.go).
	ListAgentQuestionEventsByTask(ctx context.Context, taskID pgtype.UUID) ([]db.TaskEvent, error)
	// ListCommandApprovalRequestEventsByTask возвращает события command_approval_request
	// задачи (самые новые первыми) — источник сопоставления решения пользователя
	// (request_id из тела PostTasksIdApprove) с конкретной записью task_events (тикет
	// 6.4, FR F3, см. PostTasksIdApprove в tasks.go).
	ListCommandApprovalRequestEventsByTask(ctx context.Context, taskID pgtype.UUID) ([]db.TaskEvent, error)
	// ListTasksByUser возвращает задачи владельца, owner-scoped прямо в SQL
	// (FR A4, I3), с опциональными фильтрами integration_id/status (тикет 8.6,
	// FR H1, Gherkin §10 «Состав записи о задаче», см. GetTasks в tasks.go).
	ListTasksByUser(ctx context.Context, arg db.ListTasksByUserParams) ([]db.Task, error)
	// ListTaskEventsByTask возвращает полный хронологический журнал событий
	// задачи (по seq по возрастанию) — источник для GET /tasks/{id}/events
	// (тикет 8.6, FR H1, Gherkin §10, см. GetTasksIdEvents в tasks.go).
	// Владение задачей проверяется отдельно, до вызова этого метода.
	ListTaskEventsByTask(ctx context.Context, taskID pgtype.UUID) ([]db.TaskEvent, error)
}

// Server — реализация сгенерированного api.ServerInterface для оркестратора.
//
// Встраивает api.Unimplemented (501 для ещё не реализованных операций) и
// переопределяет готовые. Хранит слой данных (queries), логгер и ключ подписи
// access-JWT. Создаётся через NewServer; HTTP-роутер собирается через NewRouter.
type Server struct {
	Unimplemented

	queries       Querier
	logger        *slog.Logger
	jwtSigningKey []byte

	// integrationUUIDAEADKey/integrationUUIDHMACKey — подключи тикета 2.2,
	// выведенные из мастер-ключа шифрования (encryptionKey параметр NewServer)
	// через crypto.DeriveKey под разными purpose: один — для AEAD-шифрования
	// UUID-секрета интеграции (показ владельцу, FR B2), другой — для его
	// HMAC-отпечатка (поиск при аутентификации машины, тикет 2.3, FR B6).
	// Мастер-ключ намеренно НЕ используется напрямую ни в Encrypt, ни в
	// HMACSHA256 — reuse одного ключа в двух разных крипто-примитивах плохая
	// крипто-гигиена (см. godoc internal/crypto.DeriveKey).
	integrationUUIDAEADKey []byte
	integrationUUIDHMACKey []byte

	// machineConns — реестр активных WS-соединений машины по integration_id
	// (тикет 2.4, FR B6, ADR 0002 "защита от повторного UUID"). Один процесс
	// оркестратора в MVP (docs/01_tech_stack_and_architecture.md [РЕШЕНИЕ 5]) —
	// in-memory достаточно, распределённой координации (Redis и т.п.) не
	// требуется. Под machineConnsMu: используется и для вытеснения старого
	// соединения новым с тем же integration_id (registerMachineConn), и для
	// compare-and-delete снятия с регистрации при дисконнекте
	// (unregisterMachineConn) — см. godoc обеих функций в machine_ws.go про
	// гонку «новое соединение зарегистрировалось раньше, чем старое дошло до
	// своего defer».
	machineConns   map[uuid.UUID]*websocket.Conn
	machineConnsMu sync.Mutex

	// ackSink — получатель ack-кадров машины (тикет 3.4, см. AckSink). nil по
	// умолчанию — ack-кадры просто игнорируются (штатно, если Redpanda-мост не
	// настроен, например в тестах/каркасных прогонах без ORCH_REDPANDA_SEEDS,
	// см. orchestrator/main.go). Регистрируется один раз при старте через
	// SetAckSink, читается под ackSinkMu, т.к. может устанавливаться уже после
	// NewServer, но до начала обслуживания WS-трафика.
	ackSink   AckSink
	ackSinkMu sync.RWMutex

	// eventSink — получатель кадров-событий машины (heartbeat и далее, тикет
	// 3.6, FR B4, см. EventSink). nil по умолчанию — событийные кадры (в т.ч.
	// heartbeat) молча игнорируются (штатно, если presence-подсистема не
	// настроена, например в тестах/каркасных прогонах без
	// ORCH_REDPANDA_SEEDS, см. orchestrator/main.go). Регистрируется один раз
	// при старте через SetEventSink, читается под eventSinkMu — тот же
	// принцип, что и у ackSink/ackSinkMu.
	eventSink   EventSink
	eventSinkMu sync.RWMutex

	// notifier — получатель доменных событий уведомления (тикет 7.1, FR G1,
	// см. Notifier). nil по умолчанию — событие уведомления тогда просто не
	// формируется (штатно, пока notify-подсистема не настроена, например до
	// реализации тикетов 7.2/7.3, и в тестах, не относящихся к тикету 7.1).
	// Регистрируется один раз при старте через SetNotifier, читается под
	// notifierMu — тот же принцип, что и у ackSink/eventSink.
	notifier   Notifier
	notifierMu sync.RWMutex

	// transitioner — единственная точка смены статуса задачи (тикет 5.2, см.
	// taskTransitioner). Устанавливается один раз при старте через
	// SetTransitioner (orchestrator/main.go), сразу после NewServer — в
	// отличие от ackSink/eventSink НЕ является опциональной фичей: если nil,
	// PostTasks отвечает 500 (это означало бы ошибку инициализации сервиса,
	// не штатный случай).
	transitioner   taskTransitioner
	transitionerMu sync.RWMutex

	// commandPublisher — см. CommandPublisher. nil по умолчанию — штатно, если
	// Redpanda-подсистема не настроена (ORCH_REDPANDA_SEEDS пуст, тесты без
	// брокера); PostTasks в этом случае отвечает 500 (задачу нельзя доставить
	// без шины). Регистрируется один раз при старте через
	// SetCommandPublisher.
	commandPublisher   CommandPublisher
	commandPublisherMu sync.RWMutex
}

// AckSink — получатель ack-кадров от машины (protocol.md §5): тикет 3.4
// нуждается в том, чтобы GetMachineWs (machine_ws.go) сообщал мосту
// оркестратора о получении ack{ack_message_id}, не зная ничего о его
// внутреннем устройстве (Redpanda, ожидание/коммит конкретной записи) — этот
// узкий интерфейс и есть граница между транспортным слоем (api) и мостом
// (orchestrator/internal/bridge.Bridge реализует его структурно, без
// импорта пакета api пакетом bridge и наоборот — зависимость только в одну
// сторону, от orchestrator/main.go, которая и связывает Server с Bridge через
// SetAckSink).
type AckSink interface {
	// HandleAck обрабатывает ack с данным ack_message_id (message_id
	// подтверждаемой команды, bus.AckPayload). Реализация ОБЯЗАНА быть
	// безопасной к повторному и к неизвестному message_id (дубль ack,
	// просроченный ack уже отретраенной команды и т.п., at-least-once,
	// protocol.md §5) — никогда не паниковать, просто проигнорировать
	// несовпавший вызов.
	HandleAck(ackMessageID string)
}

// EventSink — получатель конвертов-событий от машины (heartbeat и далее,
// тикет 3.6, FR B4, protocol.md §6): GetMachineWs (machine_ws.go) публикует
// через него события, не зная ничего об их внутреннем устройстве (Redpanda,
// топик machine.events) — та же граница между транспортным слоем (api) и
// бизнес-подсистемой, что и у AckSink/бриджа (orchestrator/internal/presence.Sink
// реализует EventSink структурно, без импорта пакета api пакетом presence и
// наоборот — зависимость только в одну сторону, от orchestrator/main.go,
// которая и связывает Server с presence.Sink через SetEventSink).
type EventSink interface {
	// HandleEvent публикует конверт события (env.IntegrationID уже
	// перезаписан вызывающим на аутентифицированный DB id, см.
	// handleMachineFrame/handleMachineEvent в machine_ws.go — агенту доверять
	// нельзя, он присылает секрет интеграции, а не DB id). Ошибка —
	// публикация не удалась, ack агенту отправлять НЕЛЬЗЯ (агент повторит
	// через свой durable outbox, тикет 3.5).
	HandleEvent(ctx context.Context, env bus.Envelope) error
}

// Notifier — получатель доменных событий уведомления (тикет 7.1, FR G1):
// handleAgentQuestion (machine_ws.go) сообщает о вопросе агента, не зная
// ничего о реальных каналах доставки (web WebSocket — тикет 7.2, Telegram —
// тикет 7.3) — та же граница между транспортным/API-слоем и
// бизнес-подсистемой, что и у AckSink/EventSink выше. nil по умолчанию —
// тогда уведомление просто не формируется (штатно, если notify-подсистема ещё
// не настроена, например до реализации 7.2/7.3, и в тестах, не относящихся к
// тикету 7.1).
type Notifier interface {
	// Notify публикует доменное событие уведомления. Ошибка — публикация не
	// удалась; вызывающий (handleAgentQuestion) ТОЛЬКО логирует её и
	// продолжает штатно отправлять ack агенту — уведомление вторично
	// относительно смены статуса задачи, его сбой не должен блокировать или
	// дублировать основной поток вопрос/ответ.
	Notify(ctx context.Context, n notify.Notification) error
}

// CommandPublisher — публикатор команд машине в топик machine.commands
// (тикет 5.3, FR E1): PostTasks (tasks.go) публикует конверт task_assigned,
// не зная ничего о его внутреннем устройстве (Redpanda) — та же граница
// между транспортным/API-слоем и шиной, что и у AckSink/EventSink выше.
// Сигнатура НАМЕРЕННО совпадает с бизнес-методом *bus.Producer.PublishKeyed
// — тем самым сам *bus.Producer уже удовлетворяет этому интерфейсу БЕЗ
// отдельного адаптера (тот же структурный приём, что и у AckSink/EventSink,
// реализуемых bridge.Bridge/presence.Sink без импорта пакета api).
type CommandPublisher interface {
	PublishKeyed(ctx context.Context, topic, keyField string, env bus.Envelope) error
}

// taskTransitioner — узкий интерфейс на *task.Transitioner.{Transition,
// TransitionWithEvent}, нужный PostTasks (тикет 5.3) для перевода новой
// задачи created→queued и PostTasksIdAnswer/handleAgentQuestion (тикет 6.1)
// для переходов running↔waiting_user с атомарной записью
// agent_question/user_answer (FR E1, единственная точка смены статуса —
// тикет 5.2). Сужение — для юнит-тестов: обработчики не завязаны на весь
// *task.Transitioner (который сам требует *pgxpool.Pool, недоступный
// обработчикам через узкий Querier) — тесты подставляют фейк, реализующий
// только эти методы (см. tasks_test.go). *task.Transitioner удовлетворяет
// этому интерфейсу структурно.
type taskTransitioner interface {
	Transition(ctx context.Context, taskID pgtype.UUID, trigger task.Trigger) (from, to task.Status, err error)
	// TransitionWithEvent — см. task.Transitioner.TransitionWithEvent (тикет
	// 6.1): атомарно пишет бизнес-событие (agent_question/user_answer) перед
	// status_change в одной транзакции с самим переходом.
	TransitionWithEvent(ctx context.Context, taskID pgtype.UUID, trigger task.Trigger, eventType string, refEventID pgtype.UUID, eventPayload []byte) (from, to task.Status, err error)
	// RecordEvent — см. task.Transitioner.RecordEvent (тикет 8.5): атомарно
	// пишет событие БЕЗ смены статуса (agent_progress, FR E6).
	RecordEvent(ctx context.Context, taskID pgtype.UUID, eventType string, refEventID pgtype.UUID, eventPayload []byte) (seq int64, err error)
}

// integrationUUIDAEADKeyPurpose/integrationUUIDHMACKeyPurpose — строки purpose
// для crypto.DeriveKey, под которые выводятся подключи тикета 2.2. Значения
// произвольны, но должны быть СТАБИЛЬНЫ между рестартами процесса (иначе ранее
// зашифрованные/хэшированные UUID-секреты интеграций перестанут
// расшифровываться/находиться) и РАЗНЫ между собой (иначе AEAD и HMAC делили
// бы один и тот же фактический ключ).
const (
	integrationUUIDAEADKeyPurpose = "integration-uuid-aead"
	integrationUUIDHMACKeyPurpose = "integration-uuid-hmac"
)

// NewServer собирает обработчик API оркестратора поверх слоя данных, логгера,
// ключа подписи access-JWT и мастер-ключа шифрования.
//
// queries — sqlc-запросы (обычно db.New(pool)); logger — логгер сервиса (nil
// допустим, тогда серверные ошибки не логируются); jwtSigningKey — секрет HMAC
// для подписи/проверки access-токенов (JWT_SIGNING_KEY из окружения,
// docs/MANUAL_STEPS.md); encryptionKey — мастер-ключ шифрования, РОВНО 32
// декодированных байта (APP_ENCRYPTION_KEY из окружения, тикет 2.2,
// internal/crypto) — из него здесь же выводятся независимые подключи под
// UUID-секрет интеграции (см. поля Server). Как и jwtSigningKey, НЕ
// генерируется и не подставляется по умолчанию здесь: вызывающая сторона
// (orchestrator/main.go) отвечает за то, что оба ключа непусты и корректной
// длины в проде. Возвращает *Server, готовый к монтированию через NewRouter.
func NewServer(queries Querier, logger *slog.Logger, jwtSigningKey []byte, encryptionKey []byte) *Server {
	return &Server{
		queries:                queries,
		logger:                 logger,
		jwtSigningKey:          jwtSigningKey,
		integrationUUIDAEADKey: crypto.DeriveKey(encryptionKey, integrationUUIDAEADKeyPurpose),
		integrationUUIDHMACKey: crypto.DeriveKey(encryptionKey, integrationUUIDHMACKeyPurpose),
		machineConns:           make(map[uuid.UUID]*websocket.Conn),
	}
}

// NewRouter монтирует chi-роутер оркестратора: общие middleware (recover,
// request-id), auth-middleware (тикет 1.4) и все маршруты из сгенерированного
// контракта поверх переданного Server.
//
// Маршруты монтируются от корня (Caddy стрипает префикс /api/*), включая
// GET /healthz и POST /auth/register. Возвращаемый chi.Router передаётся в
// platform.Service.SetHandler, сохраняя единый graceful shutdown.
//
// auth-middleware (s.authMiddleware, см. middleware.go) подключается через
// ChiServerOptions.Middlewares — официальный per-operation хук
// oapi-codegen-chi: сгенерированный ServerInterfaceWrapper прогоняет его для
// каждой операции, но контекст запроса к этому моменту уже содержит маркер
// BearerAuthScopes ТОЛЬКО для операций, защищённых контрактом (без
// `security: []` в openapi.yaml) — так middleware применяется выборочно к
// защищённым маршрутам, не трогая публичные (/healthz, /auth/register,
// /auth/login, /auth/refresh, /machine/ws), без ручного списка путей и без
// правок сгенерированного кода (подробности — godoc middleware.go).
func NewRouter(s *Server) chi.Router {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.Recoverer)
	handler := HandlerWithOptions(s, ChiServerOptions{
		BaseRouter:  r,
		Middlewares: []MiddlewareFunc{s.authMiddleware},
	})
	return handler.(chi.Router)
}

// MachineConn возвращает активное WS-соединение интеграции integrationID,
// если оно сейчас есть (ok==true), — переиспользование реестра s.machineConns
// (тикет 2.4) мостом оркестратора (тикет 3.4) как источника «активное WS для
// integration_id», предусмотренное ADR 0002. ok==false означает «машина
// сейчас оффлайн» (нет активного WS) — вызывающий (мост) не должен
// коммитить соответствующую запись Redpanda и обязан отложить доставку до
// переподключения машины (protocol.md §5).
func (s *Server) MachineConn(integrationID uuid.UUID) (*websocket.Conn, bool) {
	s.machineConnsMu.Lock()
	defer s.machineConnsMu.Unlock()
	conn, ok := s.machineConns[integrationID]
	return conn, ok
}

// SetAckSink регистрирует получателя ack-кадров машины (тикет 3.4, см. godoc
// AckSink). Вызывается ОДИН раз при старте (orchestrator/main.go), после
// конструирования моста и до начала обслуживания HTTP/WS-трафика; nil —
// допустимое значение (в т.ч. явный сброс) — тогда GetMachineWs молча
// игнорирует ack-кадры (см. machine_ws.go), что штатно при отключённом
// Redpanda-мосте (ORCH_REDPANDA_SEEDS пуст) и в тестах, не относящихся к
// тикету 3.4.
func (s *Server) SetAckSink(sink AckSink) {
	s.ackSinkMu.Lock()
	defer s.ackSinkMu.Unlock()
	s.ackSink = sink
}

// getAckSink читает текущий AckSink под ackSinkMu (см. godoc полей Server).
func (s *Server) getAckSink() AckSink {
	s.ackSinkMu.RLock()
	defer s.ackSinkMu.RUnlock()
	return s.ackSink
}

// SetEventSink регистрирует получателя кадров-событий машины (тикет 3.6, см.
// godoc EventSink). Вызывается ОДИН раз при старте (orchestrator/main.go),
// после конструирования presence.Sink и до начала обслуживания HTTP/WS-трафика;
// nil — допустимое значение (в т.ч. явный сброс) — тогда GetMachineWs молча
// игнорирует событийные кадры (см. machine_ws.go), что штатно при отключённой
// presence-подсистеме (ORCH_REDPANDA_SEEDS пуст) и в тестах, не относящихся к
// тикету 3.6.
func (s *Server) SetEventSink(sink EventSink) {
	s.eventSinkMu.Lock()
	defer s.eventSinkMu.Unlock()
	s.eventSink = sink
}

// getEventSink читает текущий EventSink под eventSinkMu (см. godoc полей Server).
func (s *Server) getEventSink() EventSink {
	s.eventSinkMu.RLock()
	defer s.eventSinkMu.RUnlock()
	return s.eventSink
}

// SetNotifier регистрирует получателя доменных событий уведомления (тикет
// 7.1, см. godoc Notifier). Вызывается ОДИН раз при старте
// (orchestrator/main.go), после конструирования notify-подсистемы (тикеты
// 7.2/7.3) и до начала обслуживания HTTP/WS-трафика; nil — допустимое
// значение (в т.ч. явный сброс) — тогда handleAgentQuestion молча не
// формирует уведомление, что штатно, пока notify-подсистема не настроена
// (например, до реализации тикетов 7.2/7.3) и в тестах, не относящихся к
// тикету 7.1.
func (s *Server) SetNotifier(n Notifier) {
	s.notifierMu.Lock()
	defer s.notifierMu.Unlock()
	s.notifier = n
}

// getNotifier читает текущий Notifier под notifierMu (см. godoc полей Server).
func (s *Server) getNotifier() Notifier {
	s.notifierMu.RLock()
	defer s.notifierMu.RUnlock()
	return s.notifier
}

// SetTransitioner регистрирует единственную точку смены статуса задачи
// (тикет 5.2, см. taskTransitioner). Вызывается ОДИН раз при старте
// (orchestrator/main.go), сразу после NewServer, до начала обслуживания
// HTTP-трафика.
func (s *Server) SetTransitioner(t taskTransitioner) {
	s.transitionerMu.Lock()
	defer s.transitionerMu.Unlock()
	s.transitioner = t
}

// getTransitioner читает текущий taskTransitioner под transitionerMu (см.
// godoc полей Server).
func (s *Server) getTransitioner() taskTransitioner {
	s.transitionerMu.RLock()
	defer s.transitionerMu.RUnlock()
	return s.transitioner
}

// SetCommandPublisher регистрирует публикатора команд машине (тикет 5.3, см.
// CommandPublisher). Вызывается ОДИН раз при старте (orchestrator/main.go),
// после создания Redpanda-продьюсера и до начала обслуживания HTTP-трафика;
// nil — допустимое значение (в т.ч. явный сброс) — тогда PostTasks отвечает
// 500 (см. godoc поля commandPublisher).
func (s *Server) SetCommandPublisher(p CommandPublisher) {
	s.commandPublisherMu.Lock()
	defer s.commandPublisherMu.Unlock()
	s.commandPublisher = p
}

// getCommandPublisher читает текущий CommandPublisher под
// commandPublisherMu (см. godoc полей Server).
func (s *Server) getCommandPublisher() CommandPublisher {
	s.commandPublisherMu.RLock()
	defer s.commandPublisherMu.RUnlock()
	return s.commandPublisher
}

// GetHealthz отвечает 200 на liveness-проверку.
//
// Переопределяет 501-заглушку Unimplemented: /healthz сгенерирован контрактом и
// нужен docker compose / Caddy / smoke-тесту (тикет 0.3). Тело совпадает по духу
// с health-роутером platform: {"status":"ok","service":"orchestrator"}.
func (s *Server) GetHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "orchestrator"})
}

// PostAuthRegister реализует POST /auth/register — регистрацию по токену (FR A1).
//
// Бизнес: доступ в систему закрытый — аккаунт создаётся ТОЛЬКО при предъявлении
// действующего секретного токена регистрации, чтобы система оставалась приватной
// (FR A1, Gherkin §1 «Регистрация без токена запрещена»). Логика:
//   - валидируем тело (username/password/registration_token непусты);
//   - берём активный токен; если его нет ИЛИ предъявленный не совпал — 403;
//   - argon2id-хэшируем пароль (не plaintext, FR I1);
//   - вставляем пользователя; занятый username (unique violation) — 409;
//   - успех — 201 (Gherkin §1 «Успешная регистрация по валидному токену»).
func (s *Server) PostAuthRegister(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var req RegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_body", "тело запроса не является валидным JSON")
		return
	}
	if msg, ok := validateRegister(req); !ok {
		writeError(w, http.StatusBadRequest, "validation_error", msg)
		return
	}

	// Доступ закрытый: сверяем предъявленный токен с действующим активным (FR A1).
	// Любая причина «нет совпадения» (нет активного токена / не совпал) — 403, без
	// раскрытия, какая именно: это секрет, а не учётные данные.
	active, err := s.queries.GetActiveRegistrationToken(ctx)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusForbidden, "registration_forbidden", "регистрация по токену недоступна")
			return
		}
		s.logError("GetActiveRegistrationToken", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}
	// Сравнение в постоянном времени не требуется: токен не выводится наружу и
	// проверяется на точное равенство; тайминг-канал тут не даёт полезного сигнала.
	if req.RegistrationToken != active.Token {
		writeError(w, http.StatusForbidden, "registration_forbidden", "неверный токен регистрации")
		return
	}

	hash, err := auth.HashPassword(req.Password)
	if err != nil {
		s.logError("HashPassword", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	user, err := s.queries.CreateUser(ctx, db.CreateUserParams{
		Username:     req.Username,
		PasswordHash: hash,
		IsAdmin:      false, // обычный пользователь; админ назначается отдельно (FR A2, тикеты 1.6/1.7).
	})
	if err != nil {
		// Занятый username ловим по коду unique violation (23505) — не делаем
		// предварительный SELECT (TOCTOU + лишний запрос).
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation {
			writeError(w, http.StatusConflict, "username_taken", "имя пользователя занято")
			return
		}
		s.logError("CreateUser", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	// 201 без тела: контракт описывает только «Аккаунт создан». Логин — отдельным
	// вызовом POST /auth/login (тикет 1.3).
	w.Header().Set("X-User-Id", user.ID.String())
	w.WriteHeader(http.StatusCreated)
}

// validateRegister проверяет обязательные непустые поля тела регистрации.
//
// Возвращает (сообщение, false) при первом нарушении, иначе ("", true). Контракт
// делает все три поля обязательными; пустые/пробельные значения недопустимы.
func validateRegister(req RegisterRequest) (string, bool) {
	switch {
	case strings.TrimSpace(req.Username) == "":
		return "username обязателен", false
	case strings.TrimSpace(req.Password) == "":
		return "password обязателен", false
	case strings.TrimSpace(req.RegistrationToken) == "":
		// Пустой токен — это «регистрация без токена»: тоже отказ, но на уровне
		// валидации тела (400). Семантический отказ по неверному токену — 403 ниже.
		return "registration_token обязателен", false
	default:
		return "", true
	}
}

// logError пишет ошибку обработчика в лог сервиса (если логгер задан).
func (s *Server) logError(op string, err error) {
	if s.logger != nil {
		s.logger.Error("ошибка обработчика API", slog.String("op", op), slog.Any("error", err))
	}
}

// writeJSON сериализует v в тело ответа с указанным статусом и заголовком JSON.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeError отправляет ошибку в формате схемы Error контракта (code+message).
func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, Error{Code: code, Message: message})
}
