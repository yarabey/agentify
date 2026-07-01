package api

// integrations.go — CRUD интеграций и выдача UUID-секрета (тикет 2.2,
// FR B1, B2, B5, Gherkin §2 «Управление интеграциями»).
//
// Назначение (бизнес): пользователь описывает свои машины как интеграции,
// чтобы направлять в них задачи. Создание (POST /integrations) требует
// имени, опционально принимает ip_hint (доп. проверка IP при подключении
// машины, FR B3 — используется тикетом 2.3, здесь только сохраняется) и
// выдаёт владельцу случайный UUID-секрет — машина предъявит его при
// WS-handshake (тикет 2.3), чтобы система опознала её как эту интеграцию
// (Gherkin §2 «Создание интеграции выдаёт UUID»). UUID показывается владельцу
// сразу при создании И при каждом последующем GET /integrations/{id} (нужен
// повторно для настройки машины — FR B2), но НИКОГДА в списке
// GET /integrations (там — только Integration без секрета). Список и доступ
// по id жёстко owner-scoped (FR A4, I3): чужая интеграция не существует для
// тебя, единый 404, без утечки самого факта существования id. Изменение
// (PATCH, тикет 2.5) — частичное обновление name/ip_hint; активные задачи
// интеграции не рвутся структурно (см. godoc PatchIntegrationsId ниже).
//
// Как устроено (тех): UUID-секрет в БД не хранится в открытом виде —
// integrations.uuid_hmac хранит HMAC-SHA256 от него (для будущего поиска по
// предъявленному машиной значению, тикет 2.3, без расшифровки), а
// integrations.uuid_enc — AEAD-шифротекст (для повторного показа владельцу).
// Оба считаются через internal/crypto (зерно общего крипто-модуля тикета
// 11.1) под подключами, выведенными из мастер-ключа сервера
// (Server.encryptionKey, ORCH_APP_ENCRYPTION_KEY) через crypto.DeriveKey:
// мастер-ключ никогда не используется напрямую ни в Encrypt, ни в HMACSHA256
// — разные purpose дают независимые подключи под каждый примитив (см. поля
// Server в server.go). Сами SQL-запросы (CreateIntegration,
// ListIntegrationsByUser, GetIntegrationByIDAndUser, UpdateIntegration) — в
// orchestrator/queries/integrations.sql, сгенерированы sqlc.
import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/yarabey/agentify/internal/crypto"
	"github.com/yarabey/agentify/orchestrator/internal/db"
)

// PostIntegrations реализует POST /integrations — создание интеграции с
// выдачей UUID-секрета (FR B1, B2, Gherkin §2 «Создание интеграции выдаёт
// UUID»).
//
// Бизнес: имя обязательно (непустое после TrimSpace, как и в
// validateRegister), ip_hint опционален и не валидируется — это просто
// необязательная доп. проверка IP, используемая позже (тикет 2.3). Генерируем
// случайный (криптостойкий, uuid.NewRandom) UUID, считаем его HMAC и
// AEAD-шифротекст, вставляем со status='offline' (машина ещё не
// подключалась, FR B4) и возвращаем IntegrationWithSecret с расшифрованным
// (только что сгенерированным, расшифровывать не требуется) UUID — он
// больше нигде не хранится в открытом виде и нужен владельцу для настройки
// машины.
func (s *Server) PostIntegrations(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	userID, ok := UserIDFromContext(ctx)
	if !ok {
		// Защищённый маршрут (auth-middleware, тикет 1.4) — отсутствие user_id
		// в контексте означало бы дыру в middleware, не штатный пользовательский
		// случай. Тот же 401, что и admin.go при том же edge-case.
		writeUnauthorized(w)
		return
	}

	var req IntegrationCreate
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_body", "тело запроса не является валидным JSON")
		return
	}
	if strings.TrimSpace(req.Name) == "" {
		writeError(w, http.StatusBadRequest, "validation_error", "name обязателен")
		return
	}

	secret, err := uuid.NewRandom()
	if err != nil {
		s.logError("uuid.NewRandom", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}
	secretBytes := secret[:]

	uuidHMAC := hex.EncodeToString(crypto.HMACSHA256(s.integrationUUIDHMACKey, secretBytes))
	uuidEnc, err := crypto.Encrypt(s.integrationUUIDAEADKey, secretBytes)
	if err != nil {
		s.logError("crypto.Encrypt", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	row, err := s.queries.CreateIntegration(ctx, db.CreateIntegrationParams{
		UserID:   pgtype.UUID{Bytes: userID, Valid: true},
		Name:     req.Name,
		IpHint:   req.IpHint,
		UuidHmac: uuidHMAC,
		UuidEnc:  uuidEnc,
	})
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation {
			// Коллизия uuid_hmac (UNIQUE на integrations.uuid_hmac, миграция
			// 00002) статистически ничтожна — 128-битный криптостойкий UUID.
			// Не пользовательская ошибка вроде «имя занято»: логируем и
			// отвечаем 500, повторная генерация не требуется в MVP (не FR).
			s.logError("CreateIntegration (uuid_hmac collision)", err)
			writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
			return
		}
		s.logError("CreateIntegration", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	writeJSON(w, http.StatusCreated, toIntegrationWithSecret(row, secret))
}

// GetIntegrations реализует GET /integrations — список интеграций владельца
// (FR A4, I3).
//
// Бизнес: только свои интеграции (Gherkin §2, owner isolation) и БЕЗ секрета
// — Integration не содержит поля uuid, в отличие от IntegrationWithSecret.
func (s *Server) GetIntegrations(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	userID, ok := UserIDFromContext(ctx)
	if !ok {
		writeUnauthorized(w)
		return
	}

	rows, err := s.queries.ListIntegrationsByUser(ctx, pgtype.UUID{Bytes: userID, Valid: true})
	if err != nil {
		s.logError("ListIntegrationsByUser", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	result := make([]Integration, 0, len(rows))
	for _, row := range rows {
		result = append(result, toIntegration(row))
	}
	writeJSON(w, http.StatusOK, result)
}

// GetIntegrationsId реализует GET /integrations/{id} — показ интеграции
// владельцу вместе с расшифрованным UUID-секретом (FR B2).
//
// Бизнес: UUID нужен владельцу повторно при настройке машины, поэтому (в
// отличие от GET /integrations) расшифровываем uuid_enc и показываем его
// каждый раз. Запрос owner-scoped прямо в SQL (GetIntegrationByIDAndUser) —
// чужая или несуществующая интеграция неотличимы, единый 404 (не 403, не
// давать атакующему лишний сигнал — тот же принцип, что у ParseAccessToken).
func (s *Server) GetIntegrationsId(w http.ResponseWriter, r *http.Request, id IdPath) {
	ctx := r.Context()

	userID, ok := UserIDFromContext(ctx)
	if !ok {
		writeUnauthorized(w)
		return
	}

	row, err := s.queries.GetIntegrationByIDAndUser(ctx, db.GetIntegrationByIDAndUserParams{
		ID:     pgtype.UUID{Bytes: id, Valid: true},
		UserID: pgtype.UUID{Bytes: userID, Valid: true},
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeIntegrationNotFound(w)
			return
		}
		s.logError("GetIntegrationByIDAndUser", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	plaintext, err := crypto.Decrypt(s.integrationUUIDAEADKey, row.UuidEnc)
	if err != nil {
		// uuid_enc был зашифрован этим же сервером под тем же мастер-ключом —
		// сбой расшифровки тут означает порчу данных/смену ключа, не
		// пользовательскую ошибку, поэтому 500 с логом.
		s.logError("crypto.Decrypt", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}
	secret, err := uuid.FromBytes(plaintext)
	if err != nil {
		s.logError("uuid.FromBytes", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	writeJSON(w, http.StatusOK, toIntegrationWithSecret(row, secret))
}

// PatchIntegrationsId реализует PATCH /integrations/{id} — частичное
// редактирование name/ip_hint (FR B5, Gherkin §2 «Редактирование не рвёт
// активные задачи», тикет 2.5).
//
// «Не рвёт активные задачи» здесь выполняется структурно, а не отдельной
// проверкой: UpdateIntegration (orchestrator/queries/integrations.sql)
// пишет ТОЛЬКО в строку integrations (name/ip_hint/updated_at) и никогда не
// затрагивает tasks/task_events, а сам id интеграции PATCH не меняет —
// FK tasks.integration_id (ON DELETE RESTRICT, migrations/00003) тут ни при
// чём. Это подтверждено приёмочным тестом
// TestIntegration_Integrations_PatchDuringRunningTaskDoesNotBreakTask
// (integrations_integration_test.go): PATCH во время running-задачи не
// меняет tasks.status, не добавляет task_events, и FSM задачи продолжает
// штатно переходить дальше уже после PATCH.
//
// Бизнес: PATCH-семантика частичного обновления — nil-указатель в теле
// запроса значит «не менять», ненулевой (включая указатель на пустую
// строку для ip_hint) — «установить это значение». Для name пустая строка
// после TrimSpace считается невалидной (400) — это поле обязательно у
// интеграции в принципе (FR B1), PATCH не должен иметь возможность его
// обнулить. Owner-scoped поиск/обновление — как и в GetIntegrationsId,
// чужая/несуществующая интеграция → единый 404.
func (s *Server) PatchIntegrationsId(w http.ResponseWriter, r *http.Request, id IdPath) {
	ctx := r.Context()

	userID, ok := UserIDFromContext(ctx)
	if !ok {
		writeUnauthorized(w)
		return
	}

	var req IntegrationUpdate
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_body", "тело запроса не является валидным JSON")
		return
	}

	current, err := s.queries.GetIntegrationByIDAndUser(ctx, db.GetIntegrationByIDAndUserParams{
		ID:     pgtype.UUID{Bytes: id, Valid: true},
		UserID: pgtype.UUID{Bytes: userID, Valid: true},
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeIntegrationNotFound(w)
			return
		}
		s.logError("GetIntegrationByIDAndUser", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	name := current.Name
	if req.Name != nil {
		if strings.TrimSpace(*req.Name) == "" {
			writeError(w, http.StatusBadRequest, "validation_error", "name не может быть пустым")
			return
		}
		name = *req.Name
	}
	ipHint := current.IpHint
	if req.IpHint != nil {
		ipHint = req.IpHint
	}

	updated, err := s.queries.UpdateIntegration(ctx, db.UpdateIntegrationParams{
		ID:     pgtype.UUID{Bytes: id, Valid: true},
		UserID: pgtype.UUID{Bytes: userID, Valid: true},
		Name:   name,
		IpHint: ipHint,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Между GetIntegrationByIDAndUser и UpdateIntegration интеграцию
			// успели удалить (тикет 2.6, вне скоупа здесь) — тот же 404.
			writeIntegrationNotFound(w)
			return
		}
		s.logError("UpdateIntegration", err)
		writeError(w, http.StatusInternalServerError, "internal", "внутренняя ошибка")
		return
	}

	writeJSON(w, http.StatusOK, toIntegration(updated))
}

// writeIntegrationNotFound отвечает единым 404 (схема NotFound = Error
// контракта) для owner-scoped операций над интеграцией: используется и при
// «не существует», и при «существует, но чужая» — намеренно неразличимо.
func writeIntegrationNotFound(w http.ResponseWriter) {
	writeError(w, http.StatusNotFound, "not_found", "интеграция не найдена")
}

// writeTaskNotFound отвечает единым 404 для owner-scoped операций над
// задачей (тикет 6.1, FR A4, I3) — тот же приём, что и writeIntegrationNotFound
// выше: используется и при «не существует», и при «существует, но чужая»,
// намеренно неразличимо (см. PostTasksIdAnswer, tasks.go).
func writeTaskNotFound(w http.ResponseWriter) {
	writeError(w, http.StatusNotFound, "not_found", "задача не найдена")
}

// toIntegration конвертирует строку БД в контрактный Integration — БЕЗ
// секрета (используется списком и ответом PATCH).
func toIntegration(row db.Integration) Integration {
	id := uuid.UUID(row.ID.Bytes)
	name := row.Name
	status := IntegrationStatus(row.Status)
	createdAt := row.CreatedAt.Time

	result := Integration{
		Id:        &id,
		Name:      &name,
		IpHint:    row.IpHint,
		Status:    &status,
		CreatedAt: &createdAt,
	}
	if row.LastSeenAt.Valid {
		lastSeenAt := row.LastSeenAt.Time
		result.LastSeenAt = &lastSeenAt
	}
	return result
}

// toIntegrationWithSecret конвертирует строку БД и уже расшифрованный/
// сгенерированный UUID-секрет в контрактный IntegrationWithSecret —
// используется ответами POST и GET /integrations/{id}, единственными
// операциями, которые показывают секрет владельцу (FR B2).
func toIntegrationWithSecret(row db.Integration, secret uuid.UUID) IntegrationWithSecret {
	id := uuid.UUID(row.ID.Bytes)
	name := row.Name
	status := IntegrationWithSecretStatus(row.Status)
	createdAt := row.CreatedAt.Time

	result := IntegrationWithSecret{
		Id:        &id,
		Name:      &name,
		IpHint:    row.IpHint,
		Status:    &status,
		CreatedAt: &createdAt,
		Uuid:      &secret,
	}
	if row.LastSeenAt.Valid {
		lastSeenAt := row.LastSeenAt.Time
		result.LastSeenAt = &lastSeenAt
	}
	return result
}
