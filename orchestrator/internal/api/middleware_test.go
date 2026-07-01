// Unit-тесты auth-middleware (тикет 1.4, FR A3, D2, Gherkin §1 «Доступ к API
// по токену»).
//
// Тесты гоняются через настоящую цепочку oapi-codegen (ServerInterfaceWrapper
// + ChiServerOptions.Middlewares), а не через прямой вызов authMiddleware с
// руками собранным контекстом: так маркер BearerAuthScopes в контексте
// запроса кладёт сгенерированный код (server.gen.go), а не тест — тест не
// дублирует генерируемую логику и не ссылается на BearerAuthScopes напрямую.
// probeServer подменяет ОДНУ защищённую операцию (GET /admin/registration-token)
// тестовым обработчиком (probeHandler), чтобы можно было заглянуть в
// контекст после прохождения middleware (UserIDFromContext) — выбор именно
// этой операции для probe исторический (тикет 1.4, когда она ещё отвечала
// 501 через Unimplemented); с тикета 1.6 она реализована по-настоящему
// (admin.go), но как маршрут для проверки самого middleware подходит
// одинаково хорошо в обоих случаях — probeServer.GetAdminRegistrationToken
// всё равно подменяет вызов целиком.
//
// Покрываем приёмочные сценарии тикета 1.4:
//   - валидный токен → пропускает и кладёт user_id в контекст;
//   - протухший токен → 401;
//   - битый/мусорный токен → 401;
//   - отсутствующий заголовок Authorization → 401;
//   - неправильная схема (не "Bearer") → 401;
//   - выборочность: публичный маршрут (GET /healthz) — без проверки вовсе;
//     защищённый маршрут (через probe) — всё равно требует токен, отвечая
//     401 без него и пропуская (статус-маркер probe) с валидным; отдельная
//     проверка «защищённый, но ещё не реализованный → 401/501» теперь живёт
//     на маршруте DELETE /integrations/{id} (см. TestRouter_ProtectedRouteRequiresToken).
package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/yarabey/agentify/internal/auth"
)

// issueTestAccessToken выпускает access-JWT для userID, подписанный
// testJWTSigningKey, действующий от момента now (см. auth.IssueAccessToken) —
// общий помощник для тестов, которым нужен валидный или истёкший токен.
func issueTestAccessToken(t *testing.T, userID uuid.UUID, now time.Time) string {
	t.Helper()
	token, err := auth.IssueAccessToken(userID.String(), []byte(testJWTSigningKey), now)
	if err != nil {
		t.Fatalf("IssueAccessToken: %v", err)
	}
	return token
}

// probeHandler — терминальный http.Handler для тестов middleware: запоминает,
// был ли вызван, и что вернул UserIDFromContext в момент вызова.
type probeHandler struct {
	called bool
	userID uuid.UUID
	hasID  bool
}

func (p *probeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.called = true
	p.userID, p.hasID = UserIDFromContext(r.Context())
	w.WriteHeader(http.StatusTeapot) // отличимый от 401/501 статус-маркер «дошли до next»
}

// probeServer подменяет GetAdminRegistrationToken (защищённая контрактом
// операция, см. godoc файла) на probe, оставляя остальные операции *Server
// (включая саму GetAdminRegistrationToken-заглушку в проде) нетронутыми.
type probeServer struct {
	*Server
	probe *probeHandler
}

func (p *probeServer) GetAdminRegistrationToken(w http.ResponseWriter, r *http.Request) {
	p.probe.ServeHTTP(w, r)
}

// newProbeRouter собирает роутер с той же сборкой, что и NewRouter
// (server.go) — auth-middleware подключён через ChiServerOptions.Middlewares
// — но с GetAdminRegistrationToken, подменённым на probe.
func newProbeRouter(s *Server, probe *probeHandler) chi.Router {
	ps := &probeServer{Server: s, probe: probe}
	r := chi.NewRouter()
	handler := HandlerWithOptions(ps, ChiServerOptions{
		BaseRouter:  r,
		Middlewares: []MiddlewareFunc{s.authMiddleware},
	})
	return handler.(chi.Router)
}

// doProbeRequest шлёт GET /admin/registration-token с заданным значением
// заголовка Authorization (пустая строка — заголовок не выставляется вовсе)
// через роутер с подменённым обработчиком probe.
func doProbeRequest(t *testing.T, probe *probeHandler, authorizationHeader string) *httptest.ResponseRecorder {
	t.Helper()
	router := newProbeRouter(newTestServer(fakeQuerier{}), probe)
	req := httptest.NewRequest(http.MethodGet, "/admin/registration-token", nil)
	if authorizationHeader != "" {
		req.Header.Set("Authorization", authorizationHeader)
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

// TestAuthMiddleware_ValidToken_PassesAndSetsUserID — приёмочный сценарий
// «валидный токен → пропускает и кладёт user_id в контекст».
func TestAuthMiddleware_ValidToken_PassesAndSetsUserID(t *testing.T) {
	wantID := uuid.New()
	token := issueTestAccessToken(t, wantID, time.Now())

	probe := &probeHandler{}
	rec := doProbeRequest(t, probe, "Bearer "+token)

	if !probe.called {
		t.Fatal("next-обработчик не вызван — middleware не пропустило валидный токен")
	}
	if rec.Code != http.StatusTeapot {
		t.Fatalf("статус = %d (%s), ожидался %d (ответ next-обработчика)", rec.Code, rec.Body.String(), http.StatusTeapot)
	}
	if !probe.hasID {
		t.Fatal("UserIDFromContext: user_id отсутствует в контексте после успешной проверки")
	}
	if probe.userID != wantID {
		t.Fatalf("UserIDFromContext = %s, ожидался %s", probe.userID, wantID)
	}
}

// authMiddlewareRejectionCases — общие случаи отказа: протухший токен,
// битый/мусорный токен, неправильная схема (не Bearer), схема без значения
// токена (приёмочные сценарии тикета 1.4).
func authMiddlewareRejectionCases(t *testing.T) map[string]string {
	t.Helper()
	userID := uuid.New()
	return map[string]string{
		"протухший токен":            "Bearer " + issueTestAccessToken(t, userID, time.Now().Add(-2*auth.AccessTokenTTL)),
		"битый/мусорный токен":       "Bearer not-a-jwt-at-all",
		"неправильная схема (Basic)": "Basic " + issueTestAccessToken(t, userID, time.Now()),
		"схема без значения токена":  "Bearer",
		"пустой токен после схемы":   "Bearer ",
	}
}

// TestAuthMiddleware_RejectsInvalidToken — протухший/битый/неверная схема →
// 401 в стандартном формате ошибки проекта (writeError), next не вызывается.
func TestAuthMiddleware_RejectsInvalidToken(t *testing.T) {
	for name, header := range authMiddlewareRejectionCases(t) {
		t.Run(name, func(t *testing.T) {
			probe := &probeHandler{}
			rec := doProbeRequest(t, probe, header)

			if probe.called {
				t.Fatalf("%s: next-обработчик вызван — middleware должно было отказать", name)
			}
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("%s: статус = %d (%s), ожидался 401", name, rec.Code, rec.Body.String())
			}
			assertErrorBody(t, rec)
		})
	}
}

// TestAuthMiddleware_MissingAuthorizationHeader — отсутствующий заголовок
// Authorization на защищённой операции → 401.
func TestAuthMiddleware_MissingAuthorizationHeader(t *testing.T) {
	probe := &probeHandler{}
	rec := doProbeRequest(t, probe, "") // заголовка нет вовсе

	if probe.called {
		t.Fatal("next-обработчик вызван без заголовка Authorization")
	}
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("статус = %d (%s), ожидался 401", rec.Code, rec.Body.String())
	}
	assertErrorBody(t, rec)
}

// assertErrorBody проверяет, что тело 401-ответа — это схема Error
// контракта (code+message), тот же формат, что у остальных 401-ответов
// проекта (writeInvalidCredentials, writeInvalidRefreshToken).
func assertErrorBody(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()
	var body Error
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("тело ответа не Error{code,message}: %v (%s)", err, rec.Body.String())
	}
	if body.Code == "" || body.Message == "" {
		t.Fatalf("Error.Code/Message пусты: %+v", body)
	}
}

// TestRouter_ProtectedRouteRequiresToken — сквозной тест через NewRouter:
// защищённый контрактом маршрут (нет `security: []` в openapi.yaml) отдаёт
// 401 без токена и доходит до обработчика (тут — заглушка Unimplemented,
// 501) с валидным.
//
// Маршрут под тестом — GET /tasks (история задач, FR H1, Gherkin §10; ещё
// не реализован — GetTasks не переопределён ни в одном обработчике пакета,
// падает в заглушку Unimplemented). DELETE /integrations/{id} использовался
// здесь раньше для той же проверки, но с тикета 2.6 реализован
// (integrations.go, DeleteIntegrationsId) и отвечает 204/409/404, а не 501 —
// этот тест проверяет общий механизм middleware+Unimplemented на ЛЮБОМ ещё
// не готовом защищённом маршруте, а не конкретно на /integrations/{id}.
func TestRouter_ProtectedRouteRequiresToken(t *testing.T) {
	router := NewRouter(newTestServer(fakeQuerier{}))

	noToken := httptest.NewRequest(http.MethodGet, "/tasks", nil)
	noTokenRec := httptest.NewRecorder()
	router.ServeHTTP(noTokenRec, noToken)
	if noTokenRec.Code != http.StatusUnauthorized {
		t.Fatalf("без токена: статус = %d (%s), ожидался 401", noTokenRec.Code, noTokenRec.Body.String())
	}

	withToken := httptest.NewRequest(http.MethodGet, "/tasks", nil)
	withToken.Header.Set("Authorization", "Bearer "+issueTestAccessToken(t, uuid.New(), time.Now()))
	withTokenRec := httptest.NewRecorder()
	router.ServeHTTP(withTokenRec, withToken)
	if withTokenRec.Code != http.StatusNotImplemented {
		t.Fatalf("с валидным токеном: статус = %d (%s), ожидался 501 (обработчик ещё не реализован, но middleware пропустило)", withTokenRec.Code, withTokenRec.Body.String())
	}
}

// TestRouter_PublicRouteSkipsAuth — GET /healthz (security: [] в
// openapi.yaml) отвечает 200 вовсе без заголовка Authorization.
func TestRouter_PublicRouteSkipsAuth(t *testing.T) {
	router := NewRouter(newTestServer(fakeQuerier{}))

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("статус = %d (%s), ожидался 200", rec.Code, rec.Body.String())
	}
}
