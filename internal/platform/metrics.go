package platform

import (
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Package-level наблюдаемость (тикет 11.4, ТЗ «эксплуатация»): каждый из трёх
// сервисов (orchestrator/bot/agent) экспонирует единый минимальный набор
// HTTP-метрик Prometheus-формата на GET /metrics — количество запросов и их
// латентность по эндпоинту. Специфичные для конкретного сервиса метрики
// (WS-соединения, переходы FSM задачи — оркестратор) регистрируются самим
// сервисом поверх Registry(), возвращаемого этим же Metrics (см.
// orchestrator/internal/metrics).
//
// Выбор библиотеки: github.com/prometheus/client_golang — стандартный клиент
// Prometheus для Go, де-факто отраслевой стандарт; альтернатив (OpenTelemetry
// Metrics SDK и т.п.) в MVP-объёме не рассматривали — формат ответа /metrics
// прямо задан приёмкой тикета («валидный Prometheus-формат»), a
// client_golang/promhttp — самый прямой путь его получить без лишней
// инфраструктуры (нет коллектора/экспортёра OTel в проекте).

// Metrics — набор Prometheus-метрик одного сервиса и их реестр.
//
// Назначение (бизнес): даёт эксплуатации (человеку, следящему за прод-VPS)
// единый источник числовых показателей нагрузки/здоровья сервиса — сколько
// запросов, с какими статусами, насколько медленно — без необходимости
// парсить логи (тикет 11.4, ТЗ «эксплуатация»).
//
// Как устроено (тех): держит собственный *prometheus.Registry (НЕ глобальный
// prometheus.DefaultRegisterer) — так параллельные тесты (несколько
// *platform.Service в одном процессе) не конфликтуют за регистрацию одних и
// тех же метрик. Registry предрегистрирован стандартными Go/process-
// коллекторами (память, горутины, CPU — то, что ожидает увидеть любой
// Prometheus-дашборд по любому Go-сервису) и собственными HTTP-метриками
// сервиса; Registry() отдаёт тот же реестр наружу для регистрации
// специфичных для сервиса метрик (например, orchestrator/internal/metrics).
type Metrics struct {
	registry *prometheus.Registry

	httpRequestsTotal   *prometheus.CounterVec
	httpRequestDuration *prometheus.HistogramVec
}

// NewMetrics собирает Metrics сервиса service: новый изолированный registry
// со стандартными Go/process-коллекторами и HTTP-метриками этого сервиса
// (agentify_http_requests_total, agentify_http_request_duration_seconds).
// Регистрация не может завершиться неуспехом (свежий реестр, статические
// имена метрик без коллизий) — паника при ошибке MustRegister означала бы
// баг в этом самом файле, а не во внешнем вводе.
func NewMetrics(service string) *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	constLabels := prometheus.Labels{"service": service}

	m := &Metrics{
		registry: reg,
		httpRequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "agentify_http_requests_total",
			Help:        "Количество обработанных HTTP-запросов по методу, пути (route pattern) и статус-коду.",
			ConstLabels: constLabels,
		}, []string{"method", "path", "status"}),
		httpRequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "agentify_http_request_duration_seconds",
			Help:        "Латентность обработки HTTP-запроса в секундах, по методу и пути (route pattern).",
			ConstLabels: constLabels,
			Buckets:     prometheus.DefBuckets,
		}, []string{"method", "path"}),
	}
	reg.MustRegister(m.httpRequestsTotal, m.httpRequestDuration)
	return m
}

// Registry возвращает Prometheus-реестр сервиса, чтобы вызывающий код мог
// зарегистрировать в нём собственные, специфичные для сервиса метрики
// (например, счётчик переходов FSM задачи или gauge активных WS-соединений
// машин в оркестраторе, см. orchestrator/internal/metrics) — они попадут в
// тот же общий эндпоинт GET /metrics.
func (m *Metrics) Registry() *prometheus.Registry {
	return m.registry
}

// handler возвращает http.Handler, отдающий метрики реестра в
// Prometheus-текстовом формате (используется на GET /metrics, см.
// wrapRouter/Service.Run).
func (m *Metrics) handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// middleware — chi-мидлварь, инструментирующая КАЖДЫЙ HTTP-запрос: считает
// agentify_http_requests_total и agentify_http_request_duration_seconds по
// (метод, route pattern, статус). route pattern (не r.URL.Path) берётся из
// chi.RouteContext ПОСЛЕ вызова next — chi заполняет его по мере спуска по
// дереву маршрутов, поэтому к моменту возврата из next.ServeHTTP он уже
// содержит полный шаблон (например "/tasks/{id}/answer", а не
// "/tasks/3fa8.../answer") — так путь с UUID/ID не создаёт неограниченную
// кардинальность лейбла path у нагруженного эндпоинта. Если шаблон почему-то
// пуст (путь не совпал ни с одним маршрутом — 404 у неизвестного пути),
// используется "unmatched", чтобы не заводить лейбл на каждый произвольный
// URL, которым мог постучаться сканер/бот.
func (m *Metrics) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

		next.ServeHTTP(rec, r)

		path := "unmatched"
		if rctx := chi.RouteContext(r.Context()); rctx != nil {
			if p := rctx.RoutePattern(); p != "" {
				path = p
			}
		}

		duration := time.Since(start).Seconds()
		m.httpRequestsTotal.WithLabelValues(r.Method, path, strconv.Itoa(rec.status)).Inc()
		m.httpRequestDuration.WithLabelValues(r.Method, path).Observe(duration)
	})
}

// wrapRouter оборачивает router (текущий Service.router, включая /healthz и
// всё, что смонтировал конкретный сервис/тикет) в самый внешний chi-роутер,
// который: (1) инструментирует ЛЮБОЙ обслуженный запрос через middleware —
// в т.ч. запросы, обслуженные router'ом, подставленным через
// Service.SetHandler ПОСЛЕ создания Metrics (сгенерированный из openapi
// роутер оркестратора и т.п.); (2) безусловно отдаёт GET /metrics —
// независимо от того, определён ли такой путь в самом router (не определён
// ни у одного из трёх сервисов). Вызывается один раз в Service.Run,
// непосредственно перед стартом http.Server — router к этому моменту уже
// финальный (SetHandler вызывается ДО Run, см. её годок).
func (m *Metrics) wrapRouter(router http.Handler) http.Handler {
	r := chi.NewRouter()
	r.Use(m.middleware)
	r.Handle("/metrics", m.handler())
	r.Mount("/", router)
	return r
}

// statusRecorder оборачивает http.ResponseWriter, запоминая переданный
// WriteHeader статус-код для последующей метки метрики (см. middleware).
// Обработчики, не вызывающие WriteHeader явно (net/http сам считает это
// 200 OK при первой записи в тело) — статус остаётся дефолтным 200,
// выставленным в middleware при создании recorder, ровно как ведёт себя
// net/http.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

// WriteHeader — реализация http.ResponseWriter: запоминает статус и
// прокидывает вызов дальше.
func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

// Unwrap отдаёт исходный http.ResponseWriter — нужен http.ResponseController/
// http.Hijacker-совместимому коду (WS-апгрейд, тикеты 2.4/7.2) добираться до
// реальных возможностей нижележащего ResponseWriter в обход обёртки: пакет
// net/http (начиная с Go 1.20) распознаёт этот метод через интерфейс
// { Unwrap() http.ResponseWriter } при поиске http.Hijacker/http.Flusher и
// т.п. Без него WS-хендшейк (websocket.Accept → Hijack) сквозь эту обёртку
// сломался бы.
func (r *statusRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}
