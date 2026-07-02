//go:build bdd

// world_test.go — общее состояние одного BDD-сценария (тикет 11.2) и
// хелперы, которыми пользуются все steps_*_test.go файлы этого пакета.
//
// Назначение (бизнес): каждый Gherkin-сценарий из orchestrator/features
// оперирует бизнес-понятиями («я вошёл в систему», «у меня есть
// интеграция», «агент задаёт вопрос») — World переводит эти понятия в
// вызовы РЕАЛЬНОГО HTTP/WS API оркестратора (тот же api.NewRouter, что и в
// проде) поверх настоящего Postgres, и хранит результаты (токены,
// id интеграций/задач, последний HTTP-ответ) между шагами одного сценария.
//
// Как устроено (тех): godog вызывает ScenarioInitializer (см. suite_test.go)
// один раз НА КАЖДЫЙ сценарий (проверено по исходнику godog, run.go
// runPickle: `r.scenarioInitializer(&sc)` вызывается внутри цикла по
// pickles) — поэтому `w := &World{}`, объявленный внутри
// InitializeScenario, автоматически даёт свежий World каждому сценарию без
// явного сброса полей; общий для ВСЕХ сценариев ресурс — один Postgres
// testcontainer на весь прогон (см. sharedPostgres в suite_test.go,
// TestSuiteInitializer) — держать по контейнеру на сценарий было бы кратно
// дороже по времени, поэтому изоляция между сценариями — TRUNCATE всех
// таблиц перед каждым (см. (*World).reset).
package bddsteps

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/yarabey/agentify/internal/auth"
	"github.com/yarabey/agentify/internal/bus"
	"github.com/yarabey/agentify/orchestrator/internal/api"
	"github.com/yarabey/agentify/orchestrator/internal/db"
	"github.com/yarabey/agentify/orchestrator/internal/task"
)

// bddJWTSigningKey/bddEncryptionKey32 — тестовые ключи харнесса, тот же
// приём, что и testJWTSigningKey/testEncryptionKey32 в
// orchestrator/internal/api/*_integration_test.go: не секреты, используются
// только в тестовом процессе.
const (
	bddJWTSigningKey   = "bdd-harness-jwt-signing-key"
	bddEncryptionKey32 = "bdd-harness-encryption-key-32b!"
)

// defaultWaitTimeout — таймаут по умолчанию для степов, ожидающих
// асинхронный эффект WS-кадра/фонового воркера (см. (*World).waitTaskStatus)
// — переходы FSM, вызванные кадром машины, обрабатываются read loop'ом
// сервера в отдельной горутине, не синхронно с моментом отправки кадра.
const defaultWaitTimeout = 5 * time.Second

// bddTablesToTruncate — все таблицы схемы (orchestrator/migrations),
// сбрасываемые перед каждым сценарием. Порядок не важен — TRUNCATE ...
// CASCADE снимает и FK-зависимости (tasks.integration_id и т.п.).
var bddTablesToTruncate = []string{
	"task_events",
	"tasks",
	"channel_link_codes",
	"channel_links",
	"integrations",
	"refresh_tokens",
	"registration_tokens",
	"users",
}

// bddUser — заведённый в рамках сценария пользователь: пароль хранится в
// открытом виде (нужен для сценариев логина через POST /auth/login),
// AccessToken выдаётся сразу при создании (auth.IssueAccessToken,
// в обход /auth/login — тот же приём, что и createTestUserWithToken в
// orchestrator/internal/api/*_integration_test.go) для сценариев, где сам
// логин не предмет проверки.
type bddUser struct {
	ID          uuid.UUID
	Username    string
	Password    string
	AccessToken string
	IsAdmin     bool
}

// bddIntegration — заведённая в рамках сценария интеграция: Secret — то же
// значение, что показывается владельцу один раз при создании
// (IntegrationWithSecret.Uuid, FR B2) и предъявляется машиной в hello-кадре.
type bddIntegration struct {
	ID     uuid.UUID
	Secret uuid.UUID
	Name   string
	Owner  string // alias пользователя-владельца (ключ w.users)
}

// bddTask — заведённая в рамках сценария задача.
type bddTask struct {
	ID             uuid.UUID
	IntegrationID  uuid.UUID
	Owner          string
	IdempotencyKey string
}

// clientSession — WS-соединение браузера (тикет 7.2, GetWs) вместе с
// каналом полученных уведомлений: readLoop (см. connectClient) пишет туда
// каждый разобранный кадр, степы читают с таймаутом (см. waitNotification).
type clientSession struct {
	conn *websocket.Conn

	mu   sync.Mutex
	msgs []clientNotification
	ch   chan clientNotification
}

// clientNotification — то, что реально приходит браузеру (см.
// clientNotificationFrame в orchestrator/internal/api/client_ws.go):
// {kind, task_id, created_at}. Дублируем форму здесь — тот символ
// неэкспортирован (деталь реализации пакета api, не часть контракта,
// который стоило бы импортировать).
type clientNotification struct {
	Kind      string `json:"kind"`
	TaskID    string `json:"task_id"`
	CreatedAt string `json:"created_at"`
}

// fakePublisher реализует api.CommandPublisher в памяти (без Redpanda) —
// записывает каждый опубликованный конверт, степы затем проверяют форму
// последнего/всех конвертов адресата (orchestrator/features/README.md
// объясняет, почему в этом харнессе нет настоящей Redpanda). Тот же
// структурный приём, что и noopCommandPublisher в
// orchestrator/internal/api/tasks_integration_test.go, только с памятью
// вместо no-op.
type fakePublisher struct {
	mu   sync.Mutex
	envs []bus.Envelope
}

// PublishKeyed сохраняет конверт в памяти (потокобезопасно — обработчики
// machine WS и HTTP-хендлеры могут публиковать из разных горутин).
func (p *fakePublisher) PublishKeyed(_ context.Context, _, _ string, env bus.Envelope) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.envs = append(p.envs, env)
	return nil
}

// Envelopes возвращает копию списка опубликованных конвертов.
func (p *fakePublisher) Envelopes() []bus.Envelope {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]bus.Envelope, len(p.envs))
	copy(out, p.envs)
	return out
}

// Last возвращает последний опубликованный конверт данного type, если есть.
func (p *fakePublisher) Last(msgType string) (bus.Envelope, bool) {
	envs := p.Envelopes()
	for i := len(envs) - 1; i >= 0; i-- {
		if envs[i].Type == msgType {
			return envs[i], true
		}
	}
	return bus.Envelope{}, false
}

// World — состояние одного BDD-сценария (см. godoc пакета).
type World struct {
	pool         *pgxpool.Pool
	queries      *db.Queries
	server       *api.Server
	router       http.Handler
	httpServer   *httptest.Server
	publisher    *fakePublisher
	transitioner *task.Transitioner

	users        map[string]*bddUser
	integrations map[string]*bddIntegration
	tasks        map[string]*bddTask
	questions    map[string]string // alias("текущий"/"второй"/...) -> question_id
	approvals    map[string]string // alias -> request_id

	machineConns map[string]*websocket.Conn
	clientConns  map[string]*clientSession

	regToken string // активный токен регистрации текущего сценария (§1)

	lastStatus int
	lastBody   []byte

	// lastVisibleTasks/lastVisibleIntegrations — снимки GET /tasks и
	// GET /integrations конкретного пользователя (степы «Изоляция данных
	// между пользователями», §1); lastActorAlias — чьим токеном они были
	// получены.
	lastVisibleTasks        []api.Task
	lastVisibleIntegrations []api.Integration
	lastActorAlias          string

	staleWorkerCancel context.CancelFunc
}

// reset готовит World к новому сценарию: TRUNCATE всех таблиц на общем
// Postgres-контейнере (см. godoc пакета — почему общий, не по контейнеру на
// сценарий), сборка свежего api.Server/router/httptest.Server, обнуление
// карт состояния сценария. Вызывается из Before-хука ScenarioContext (см.
// suite_test.go).
func (w *World) reset(ctx context.Context) error {
	if w.httpServer != nil {
		w.httpServer.Close()
	}
	for _, c := range w.machineConns {
		_ = c.Close(websocket.StatusNormalClosure, "scenario teardown")
	}
	for _, cs := range w.clientConns {
		_ = cs.conn.Close(websocket.StatusNormalClosure, "scenario teardown")
	}
	if w.staleWorkerCancel != nil {
		w.staleWorkerCancel()
	}

	w.pool = pgPool
	w.queries = db.New(w.pool)

	// TRUNCATE ... RESTART IDENTITY CASCADE: таблицы этого проекта используют
	// UUID (gen_random_uuid()), RESTART IDENTITY здесь не имеет значения для
	// самих id, но безвредно и на будущее (если появится serial-колонка).
	stmt := "TRUNCATE TABLE " + strings.Join(bddTablesToTruncate, ", ") + " RESTART IDENTITY CASCADE"
	if _, err := w.pool.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("truncate перед сценарием: %w", err)
	}

	w.publisher = &fakePublisher{}
	// WithMasterKey — ОБЯЗАТЕЛЬНО тот же мастер-ключ, что передан api.NewServer
	// ниже (bddEncryptionKey32): Transitioner — единственный писатель
	// task_events.payload_enc (шифрует at-rest, FR I1, тикет 11.1), а
	// Server.decryptEventPayload читает его тем же подключом
	// (task.EventPayloadKeyPurpose) — иначе GCM-тег не сойдётся и вся история
	// (agent_question/user_answer/agent_completed/…) окажется недоступна через
	// GET /tasks/{id}/events. Тот же приём, что в orchestrator/main.go и
	// orchestrator/internal/api/crypto_atrest_integration_test.go.
	w.transitioner = task.NewTransitioner(w.pool, task.WithMasterKey([]byte(bddEncryptionKey32)))

	w.server = api.NewServer(w.queries, nil, []byte(bddJWTSigningKey), []byte(bddEncryptionKey32))
	w.server.SetTransitioner(w.transitioner)
	w.server.SetCommandPublisher(w.publisher)
	// ClientConnHub как Notifier — тот же порядок вызовов, что в
	// orchestrator/main.go (SetNotifier(server.ClientConnHub())), нужен для
	// §6 «Уведомление в web по WebSocket» и для наблюдения agent_question/
	// agent_completed/command_approval_request уведомлений в §5/§7.
	w.server.SetNotifier(w.server.ClientConnHub())
	w.router = api.NewRouter(w.server)
	w.httpServer = httptest.NewServer(w.router)

	w.users = map[string]*bddUser{}
	w.integrations = map[string]*bddIntegration{}
	w.tasks = map[string]*bddTask{}
	w.questions = map[string]string{}
	w.approvals = map[string]string{}
	w.machineConns = map[string]*websocket.Conn{}
	w.clientConns = map[string]*clientSession{}
	w.regToken = ""
	w.lastStatus = 0
	w.lastBody = nil
	w.staleWorkerCancel = nil

	return nil
}

// teardown закрывает ресурсы сценария (вызывается из After-хука).
func (w *World) teardown() {
	if w.staleWorkerCancel != nil {
		w.staleWorkerCancel()
	}
	for _, c := range w.machineConns {
		_ = c.Close(websocket.StatusNormalClosure, "scenario done")
	}
	for _, cs := range w.clientConns {
		_ = cs.conn.Close(websocket.StatusNormalClosure, "scenario done")
	}
	if w.httpServer != nil {
		w.httpServer.Close()
	}
}

// --- HTTP -------------------------------------------------------------

// doRequest шлёт HTTP-запрос через httptest.Server (не httptest.Recorder —
// этому пакету, в отличие от *_integration_test.go пакета api, WS-хендлеры
// нужны наравне с REST, поэтому единообразно используется настоящий
// httptest.Server для всего). Результат сохраняется в w.lastStatus/lastBody
// для генерик-степов вида «Тогда система отклоняет...».
func (w *World) doRequest(ctx context.Context, method, path, bearerToken string, body any, extraHeaders map[string]string) error {
	var reader *bytes.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal тела запроса: %w", err)
		}
		reader = bytes.NewReader(raw)
	} else {
		reader = bytes.NewReader(nil)
	}
	req, err := http.NewRequestWithContext(ctx, method, w.httpServer.URL+path, reader)
	if err != nil {
		return fmt.Errorf("собрать запрос %s %s: %w", method, path, err)
	}
	req.Header.Set("Content-Type", "application/json")
	if bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+bearerToken)
	}
	for k, v := range extraHeaders {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("выполнить запрос %s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(resp.Body); err != nil {
		return fmt.Errorf("прочитать тело ответа %s %s: %w", method, path, err)
	}
	w.lastStatus = resp.StatusCode
	w.lastBody = buf.Bytes()
	return nil
}

// decodeLastBody разбирает w.lastBody в out (указатель на структуру ответа).
func (w *World) decodeLastBody(out any) error {
	if len(w.lastBody) == 0 {
		return fmt.Errorf("тело последнего ответа пусто")
	}
	if err := json.Unmarshal(w.lastBody, out); err != nil {
		return fmt.Errorf("разобрать тело ответа (%s): %w", string(w.lastBody), err)
	}
	return nil
}

// --- Пользователи -------------------------------------------------------

// defaultUserAlias — псевдоним «текущего» пользователя для сценариев, где
// Gherkin не называет пользователя явно (везде, кроме §1 «Изоляция данных»,
// где явно фигурируют «Аня»/«Борис»).
const defaultUserAlias = "я"

// createUser заводит пользователя НАПРЯМУЮ через db.Queries (в обход
// POST /auth/register — тот же приём, что createTestUserWithToken в
// orchestrator/internal/api/*_integration_test.go), с РЕАЛЬНЫМ хэшем
// пароля (auth.HashPassword), чтобы сценарии логина (POST /auth/login) тоже
// работали. Access-токен выпускается сразу (auth.IssueAccessToken), чтобы
// не заставлять каждый сценарий явно логиниться, если логин не предмет
// проверки именно этого сценария.
func (w *World) createUser(ctx context.Context, alias, username, password string, isAdmin bool) (*bddUser, error) {
	hash, err := auth.HashPassword(password)
	if err != nil {
		return nil, fmt.Errorf("HashPassword: %w", err)
	}
	row, err := w.queries.CreateUser(ctx, db.CreateUserParams{
		Username:     username,
		PasswordHash: hash,
		IsAdmin:      isAdmin,
	})
	if err != nil {
		return nil, fmt.Errorf("CreateUser(%s): %w", username, err)
	}
	userID := uuid.UUID(row.ID.Bytes)
	accessToken, err := auth.IssueAccessToken(userID.String(), []byte(bddJWTSigningKey), time.Now())
	if err != nil {
		return nil, fmt.Errorf("IssueAccessToken(%s): %w", username, err)
	}
	u := &bddUser{ID: userID, Username: username, Password: password, AccessToken: accessToken, IsAdmin: isAdmin}
	w.users[alias] = u
	return u, nil
}

// ensureUser возвращает пользователя alias, заводя его при первом
// обращении (со случайным username/паролем) — для степов, чьи сценарии не
// начинаются с явного «Дано я вошёл...».
func (w *World) ensureUser(ctx context.Context, alias string) (*bddUser, error) {
	if u, ok := w.users[alias]; ok {
		return u, nil
	}
	username := fmt.Sprintf("bdd-%s-%s", strings.ToLower(alias), uuid.NewString()[:8])
	return w.createUser(ctx, alias, username, "correct horse battery staple", false)
}

// --- Интеграции -----------------------------------------------------------

const defaultIntegrationAlias = "интеграция"

// createIntegration заводит интеграцию через РЕАЛЬНЫЙ POST /integrations
// (не напрямую в БД) — сам эндпоинт и есть предмет §2, поэтому степы должны
// проходить через него, а не подставлять данные в обход.
func (w *World) createIntegration(ctx context.Context, ownerAlias, integrationAlias, name string, ipHint *string) (*bddIntegration, error) {
	owner, err := w.ensureUser(ctx, ownerAlias)
	if err != nil {
		return nil, err
	}
	if err := w.doRequest(ctx, http.MethodPost, "/integrations", owner.AccessToken, api.IntegrationCreate{Name: name, IpHint: ipHint}, nil); err != nil {
		return nil, err
	}
	if w.lastStatus != http.StatusCreated {
		return nil, fmt.Errorf("POST /integrations: статус = %d, тело = %s", w.lastStatus, string(w.lastBody))
	}
	var created api.IntegrationWithSecret
	if err := w.decodeLastBody(&created); err != nil {
		return nil, err
	}
	if created.Id == nil || created.Uuid == nil {
		return nil, fmt.Errorf("POST /integrations: пустой id/uuid в ответе")
	}
	integ := &bddIntegration{ID: *created.Id, Secret: *created.Uuid, Name: name, Owner: ownerAlias}
	w.integrations[integrationAlias] = integ
	return integ, nil
}

// ensureIntegration возвращает интеграцию alias, заводя её (и владельца)
// при первом обращении.
func (w *World) ensureIntegration(ctx context.Context, alias string) (*bddIntegration, error) {
	if integ, ok := w.integrations[alias]; ok {
		return integ, nil
	}
	return w.createIntegration(ctx, defaultUserAlias, alias, "bdd-integration-"+uuid.NewString()[:8], nil)
}

// --- Задачи и симуляция машины (машина ↔ оркестратор напрямую по WS, БЕЗ
// Redpanda — см. orchestrator/features/README.md; agent_question/
// agent_completed/command_approval_request/task_accepted обрабатываются
// machine_ws.go синхронно, ЕЗДА в Redpanda нужна только heartbeat'у, см.
// handleMachineEvent) ------------------------------------------------------

const defaultTaskAlias = "задача"

// dialMachineWS открывает WS-соединение к /machine/ws тестового сервера и
// сразу отправляет hello с секретом интеграции — тот же протокол, что и
// настоящий агент (docs/protocol.md §4). Соединение регистрируется в
// w.machineConns[integrationAlias] для последующей отправки кадров агента.
func (w *World) dialMachineWS(ctx context.Context, integrationAlias string) (*websocket.Conn, error) {
	integ, err := w.ensureIntegration(ctx, integrationAlias)
	if err != nil {
		return nil, err
	}
	wsURL := strings.Replace(w.httpServer.URL, "http://", "ws://", 1) + "/machine/ws"
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("websocket.Dial(/machine/ws): %w", err)
	}
	env := struct {
		MessageID       string  `json:"message_id"`
		TaskID          *string `json:"task_id"`
		IntegrationID   string  `json:"integration_id"`
		Type            string  `json:"type"`
		Seq             int64   `json:"seq"`
		Ts              string  `json:"ts"`
		ProtocolVersion string  `json:"protocol_version"`
		Payload         struct {
			UUID         string   `json:"uuid"`
			AgentVersion string   `json:"agent_version"`
			Providers    []string `json:"providers"`
		} `json:"payload"`
	}{
		MessageID:       uuid.NewString(),
		IntegrationID:   uuid.Nil.String(),
		Type:            bus.MessageTypeHello,
		Seq:             1,
		Ts:              time.Now().UTC().Format(time.RFC3339),
		ProtocolVersion: bus.ProtocolVersion,
	}
	env.Payload.UUID = integ.Secret.String()
	env.Payload.AgentVersion = "bdd-agent/0.0.0"
	env.Payload.Providers = []string{"claude"}
	raw, err := json.Marshal(env)
	if err != nil {
		_ = conn.CloseNow()
		return nil, fmt.Errorf("marshal hello: %w", err)
	}
	if err := conn.Write(ctx, websocket.MessageText, raw); err != nil {
		_ = conn.CloseNow()
		return nil, fmt.Errorf("write hello: %w", err)
	}
	w.machineConns[integrationAlias] = conn
	return conn, nil
}

// ensureMachineConn возвращает уже открытое WS-соединение "машины" для
// интеграции, открывая его при первом обращении.
func (w *World) ensureMachineConn(ctx context.Context, integrationAlias string) (*websocket.Conn, error) {
	if conn, ok := w.machineConns[integrationAlias]; ok {
		return conn, nil
	}
	return w.dialMachineWS(ctx, integrationAlias)
}

// sendMachineFrame сериализует и отправляет конверт машины по уже открытому
// WS-соединению интеграции.
func (w *World) sendMachineFrame(ctx context.Context, integrationAlias string, env bus.Envelope) error {
	conn, err := w.ensureMachineConn(ctx, integrationAlias)
	if err != nil {
		return err
	}
	raw, err := env.Marshal()
	if err != nil {
		return fmt.Errorf("marshal конверта %s: %w", env.Type, err)
	}
	if err := conn.Write(ctx, websocket.MessageText, raw); err != nil {
		return fmt.Errorf("write конверта %s: %w", env.Type, err)
	}
	return nil
}

// createTask ставит задачу через РЕАЛЬНЫЙ POST /tasks (owner — владелец
// интеграции) и сохраняет её под taskAlias.
func (w *World) createTask(ctx context.Context, taskAlias, integrationAlias, text string) (*bddTask, error) {
	integ, err := w.ensureIntegration(ctx, integrationAlias)
	if err != nil {
		return nil, err
	}
	owner := w.users[integ.Owner]
	if owner == nil {
		return nil, fmt.Errorf("владелец интеграции %q не найден в World", integrationAlias)
	}
	idempotencyKey := uuid.NewString()
	if err := w.doRequest(ctx, http.MethodPost, "/tasks", owner.AccessToken, api.TaskCreate{IntegrationId: integ.ID, Text: text},
		map[string]string{"Idempotency-Key": idempotencyKey}); err != nil {
		return nil, err
	}
	if w.lastStatus != http.StatusCreated {
		return nil, fmt.Errorf("POST /tasks: статус = %d, тело = %s", w.lastStatus, string(w.lastBody))
	}
	var created api.Task
	if err := w.decodeLastBody(&created); err != nil {
		return nil, err
	}
	if created.Id == nil {
		return nil, fmt.Errorf("POST /tasks: пустой id в ответе")
	}
	t := &bddTask{ID: *created.Id, IntegrationID: integ.ID, Owner: integ.Owner, IdempotencyKey: idempotencyKey}
	w.tasks[taskAlias] = t
	return t, nil
}

// ensureRunningTask возвращает задачу taskAlias в статусе running с
// подключённой "машиной" (WS), заводя интеграцию/задачу/task_accepted при
// первом обращении — общая точка входа для сценариев §5-§8/§10, которые
// начинаются с «агент выполняет мою задачу»/«задача выполняется на машине»
// и т.п.
func (w *World) ensureRunningTask(ctx context.Context, taskAlias string) (*bddTask, error) {
	if bt, ok := w.tasks[taskAlias]; ok {
		return bt, nil
	}
	integrationAlias := taskAlias + "/интеграция"
	bt, err := w.createTask(ctx, taskAlias, integrationAlias, "сделай что-нибудь полезное")
	if err != nil {
		return nil, err
	}
	if err := w.machineAcceptsTask(ctx, integrationAlias, bt); err != nil {
		return nil, err
	}
	return bt, nil
}

// machineAcceptsTask открывает (если нужно) WS-соединение машины и
// отправляет task_accepted — задача queued→running (тот же кадр, что шлёт
// настоящий агент при начале работы, protocol.md §4).
func (w *World) machineAcceptsTask(ctx context.Context, integrationAlias string, bt *bddTask) error {
	taskIDStr := bt.ID.String()
	env := bus.Envelope{
		MessageID:       bus.NewMessageID(),
		TaskID:          &taskIDStr,
		IntegrationID:   bt.IntegrationID.String(),
		Type:            bus.MessageTypeTaskAccepted,
		Seq:             1,
		Ts:              time.Now().UTC().Format(time.RFC3339),
		ProtocolVersion: bus.ProtocolVersion,
		Payload:         json.RawMessage("{}"),
	}
	if err := w.sendMachineFrame(ctx, integrationAlias, env); err != nil {
		return err
	}
	return w.waitTaskStatus(ctx, bt.ID, task.StatusRunning, 5*time.Second)
}

// integrationAliasForTask восстанавливает alias интеграции, под которым
// была заведена задача taskAlias (по соглашению ensureRunningTask —
// "<taskAlias>/интеграция"); используется степами, которым нужно
// адресоваться к "машине" этой задачи (отправить agent_question и т.п.),
// не переоткрывая интеграцию заново.
func integrationAliasForTask(taskAlias string) string {
	return taskAlias + "/интеграция"
}

// waitTaskStatus опрашивает tasks.status до совпадения с want или до
// истечения deadline — переходы FSM, вызванные через WS-кадр машины,
// обрабатываются read loop'ом сервера асинхронно (см. GetMachineWs), не
// синхронно с моментом отправки кадра степом.
func (w *World) waitTaskStatus(ctx context.Context, taskID uuid.UUID, want task.Status, deadline time.Duration) error {
	end := time.Now().Add(deadline)
	var last string
	for {
		var status string
		err := w.pool.QueryRow(ctx, `SELECT status FROM tasks WHERE id = $1`, pgtype.UUID{Bytes: taskID, Valid: true}).Scan(&status)
		if err != nil {
			return fmt.Errorf("SELECT tasks.status: %w", err)
		}
		last = status
		if status == string(want) {
			return nil
		}
		if time.Now().After(end) {
			return fmt.Errorf("tasks.status не стал %q за %v (последнее наблюдение: %q)", want, deadline, last)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// --- Клиентский (web) WS ---------------------------------------------------

// connectClient открывает WS-соединение браузера (/ws, тикет 7.2) для
// пользователя ownerAlias, аутентифицируется первым кадром (см. GetWs) и
// запускает readLoop, складывающий разобранные уведомления в канал —
// waitNotification (steps_notifications_test.go) читает из него.
func (w *World) connectClient(ctx context.Context, ownerAlias string) (*clientSession, error) {
	if cs, ok := w.clientConns[ownerAlias]; ok {
		return cs, nil
	}
	owner, err := w.ensureUser(ctx, ownerAlias)
	if err != nil {
		return nil, err
	}
	wsURL := strings.Replace(w.httpServer.URL, "http://", "ws://", 1) + "/ws"
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("websocket.Dial(/ws): %w", err)
	}
	authFrame := struct {
		Type        string `json:"type"`
		AccessToken string `json:"access_token"`
	}{Type: "auth", AccessToken: owner.AccessToken}
	raw, err := json.Marshal(authFrame)
	if err != nil {
		_ = conn.CloseNow()
		return nil, fmt.Errorf("marshal auth-кадра: %w", err)
	}
	if err := conn.Write(ctx, websocket.MessageText, raw); err != nil {
		_ = conn.CloseNow()
		return nil, fmt.Errorf("write auth-кадра: %w", err)
	}

	cs := &clientSession{conn: conn, ch: make(chan clientNotification, 16)}
	go func() {
		for {
			_, data, rerr := conn.Read(context.Background())
			if rerr != nil {
				return
			}
			var n clientNotification
			if jerr := json.Unmarshal(data, &n); jerr != nil {
				continue
			}
			cs.mu.Lock()
			cs.msgs = append(cs.msgs, n)
			cs.mu.Unlock()
			select {
			case cs.ch <- n:
			default:
			}
		}
	}()
	w.clientConns[ownerAlias] = cs
	return cs, nil
}

// waitNotification блокируется, пока в клиентской сессии не появится
// уведомление вида kind, либо не истечёт deadline.
func (cs *clientSession) waitNotification(kind string, deadline time.Duration) (clientNotification, error) {
	cs.mu.Lock()
	for _, n := range cs.msgs {
		if n.Kind == kind {
			cs.mu.Unlock()
			return n, nil
		}
	}
	cs.mu.Unlock()

	end := time.After(deadline)
	for {
		select {
		case n := <-cs.ch:
			if n.Kind == kind {
				return n, nil
			}
		case <-end:
			return clientNotification{}, fmt.Errorf("не дождались уведомления kind=%q за %v", kind, deadline)
		}
	}
}

// expectStatus сверяет статус последнего HTTP-ответа (w.lastStatus) с want,
// возвращая содержательную ошибку (с телом ответа) при расхождении —
// общий генерик-степ вида «Тогда аккаунт создаётся» (201) и т.п.
func (w *World) expectStatus(want int) error {
	if w.lastStatus != want {
		return fmt.Errorf("статус последнего ответа = %d, ожидался %d; тело = %s", w.lastStatus, want, string(w.lastBody))
	}
	return nil
}

// --- §1 «Регистрация и доступ» -------------------------------------------

// seedRegistrationToken заводит активный токен регистрации НАПРЯМУЮ через
// db.Queries.CreateRegistrationToken (тот же запрос, что использует
// bootstrap.Run) и запоминает его в w.regToken для последующего
// registerViaAPI.
func (w *World) seedRegistrationToken(ctx context.Context, token string) error {
	if _, err := w.queries.CreateRegistrationToken(ctx, token); err != nil {
		return fmt.Errorf("CreateRegistrationToken: %w", err)
	}
	w.regToken = token
	return nil
}

// registerViaAPI шлёт РЕАЛЬНЫЙ POST /auth/register (тот эндпоинт и есть
// предмет §1, поэтому степы «регистрируюсь»/«пытаюсь зарегистрироваться»
// обязаны проходить через него, не заводить пользователя в обход).
func (w *World) registerViaAPI(ctx context.Context, username, password, registrationToken string) error {
	return w.doRequest(ctx, http.MethodPost, "/auth/register", "", api.RegisterRequest{
		Username:          username,
		Password:          password,
		RegistrationToken: registrationToken,
	}, nil)
}

// bindExistingUser регистрирует под alias пользователя, УЖЕ существующего в
// БД (например, заведённого bootstrap.Run в обход World.createUser),
// выпуская ему access-токен тем же способом, что и createUser.
func (w *World) bindExistingUser(alias string, row db.User, password string) (*bddUser, error) {
	userID := uuid.UUID(row.ID.Bytes)
	accessToken, err := auth.IssueAccessToken(userID.String(), []byte(bddJWTSigningKey), time.Now())
	if err != nil {
		return nil, fmt.Errorf("IssueAccessToken(%s): %w", row.Username, err)
	}
	u := &bddUser{ID: userID, Username: row.Username, Password: password, AccessToken: accessToken, IsAdmin: row.IsAdmin}
	w.users[alias] = u
	return u, nil
}

// assertVisibleOnly проверяет, что снимки GET /tasks/GET /integrations,
// снятые токеном actorAlias (см. lastVisibleTasks/lastVisibleIntegrations),
// содержат РОВНО задачу/интеграцию actorAlias и ничего больше (Gherkin §1
// «Изоляция данных между пользователями», «Тогда она видит только свои
// данные»).
func (w *World) assertVisibleOnly(_ context.Context, actorAlias string) error {
	wantTask, ok := w.tasks[actorAlias+"/задача"]
	if !ok {
		return fmt.Errorf("задача %s не заведена в World", actorAlias+"/задача")
	}
	wantIntegration, ok := w.integrations[actorAlias+"/интеграция"]
	if !ok {
		return fmt.Errorf("интеграция %s не заведена в World", actorAlias+"/интеграция")
	}
	if len(w.lastVisibleTasks) != 1 || w.lastVisibleTasks[0].Id == nil || *w.lastVisibleTasks[0].Id != wantTask.ID {
		return fmt.Errorf("GET /tasks для %s вернул %+v, ожидалась ровно своя задача %s", actorAlias, w.lastVisibleTasks, wantTask.ID)
	}
	if len(w.lastVisibleIntegrations) != 1 || w.lastVisibleIntegrations[0].Id == nil || *w.lastVisibleIntegrations[0].Id != wantIntegration.ID {
		return fmt.Errorf("GET /integrations для %s вернул %+v, ожидалась ровно своя интеграция %s", actorAlias, w.lastVisibleIntegrations, wantIntegration.ID)
	}
	return nil
}

// ruTaskStatus — русские названия статусов FSM задачи, как они звучат в
// Gherkin-сценариях (docs/User_stories_Gherkin.md), сопоставленные
// значениям task.Status (orchestrator/internal/task/fsm.go). Общая таблица
// для степов §4/§5/§7/§9/§10 — все они формулируют статус текстом "Тогда
// задача переходит в статус \"...\"".
var ruTaskStatus = map[string]task.Status{
	"в очереди":   task.StatusQueued,
	"выполняется": task.StatusRunning,
	"ожидает ответа пользователя": task.StatusWaitingUser,
	"ожидает подтверждения":       task.StatusAwaitingConfirm,
	"завершена":                   task.StatusCompleted,
	"отменена":                    task.StatusCancelled,
	"зависла":                     task.StatusStale,
}

// resolveRuTaskStatus сопоставляет русскую фразу из Gherkin-шага значению
// task.Status, либо возвращает содержательную ошибку (опечатка в
// .feature-файле не должна тихо матчить "любой статус").
func resolveRuTaskStatus(ru string) (task.Status, error) {
	st, ok := ruTaskStatus[ru]
	if !ok {
		return "", fmt.Errorf("неизвестный русский статус задачи в шаге: %q", ru)
	}
	return st, nil
}

// resolveUserAlias сопоставляет имя из шага Gherkin уже заведённому
// пользовательскому алиасу. Нужен из-за грамматики самого исходного текста
// docs/User_stories_Gherkin.md §1: пользователь заводится в именительном
// падеже («Дано существуют пользователи "Аня" и "Борис"»), а упоминается в
// дательном в шаге отрицания («И не видит ничего, принадлежащего
// "Борису"») — текст сценария копируется в .feature-файл БЕЗ изменений (см.
// orchestrator/features/README.md), поэтому падеж согласовывает степ, не
// исходный документ. Сопоставление — по имени заведённого пользователя,
// являющемуся префиксом слова из шага (эвристика достаточна для русских
// имён собственных в этом документе: "Борис" — префикс "Борису").
func (w *World) resolveUserAlias(raw string) (string, error) {
	if _, ok := w.users[raw]; ok {
		return raw, nil
	}
	for alias := range w.users {
		if strings.HasPrefix(raw, alias) {
			return alias, nil
		}
	}
	return "", fmt.Errorf("пользователь %q (или его именительная форма) не заведён в World", raw)
}

// assertVisibleOnlyExcludes — дополнение assertVisibleOnly: явная проверка,
// что среди последних снимков НЕТ задачи/интеграции чужого пользователя
// otherAlias (Gherkin §1 «И не видит ничего, принадлежащего "Борису"»).
func (w *World) assertVisibleOnlyExcludes(otherAliasRaw string) error {
	otherAlias, err := w.resolveUserAlias(otherAliasRaw)
	if err != nil {
		return err
	}
	otherTask, ok := w.tasks[otherAlias+"/задача"]
	if !ok {
		return fmt.Errorf("задача %s не заведена в World", otherAlias+"/задача")
	}
	otherIntegration, ok := w.integrations[otherAlias+"/интеграция"]
	if !ok {
		return fmt.Errorf("интеграция %s не заведена в World", otherAlias+"/интеграция")
	}
	for _, t := range w.lastVisibleTasks {
		if t.Id != nil && *t.Id == otherTask.ID {
			return fmt.Errorf("GET /tasks увидел чужую задачу %s (владелец %s)", otherTask.ID, otherAlias)
		}
	}
	for _, in := range w.lastVisibleIntegrations {
		if in.Id != nil && *in.Id == otherIntegration.ID {
			return fmt.Errorf("GET /integrations увидел чужую интеграцию %s (владелец %s)", otherIntegration.ID, otherAlias)
		}
	}
	return nil
}
