// Package metrics — бизнес-метрики оркестратора поверх общего
// platform.Metrics (тикет 11.4, ТЗ «эксплуатация», deps: 0.6).
//
// Назначение (бизнес): помимо базовых HTTP-метрик (internal/platform.Metrics
// — количество запросов и латентность по всем трём сервисам), человеку,
// следящему за прод-эксплуатацией оркестратора, важны две вещи, которые
// напрямую отражают состояние домена, а не только транспорта: (1) сколько
// машин сейчас реально на связи (WS-соединения — то же, от чего зависит
// доставка команд, бридж, тикет 3.4) и (2) как движутся задачи по FSM
// (переходы статуса — задержавшиеся waiting_user/stale видны по накоплению
// без прогресса дальше). Оба показателя нельзя вывести только из HTTP-
// метрик (переходы FSM и WS handshake происходят вне обычного
// request/response REST-цикла — WS-соединение живёт значительно дольше
// одного HTTP-запроса, а Transition вызывается и из фоновых воркеров, не
// только из HTTP-хендлеров).
//
// Как устроено (тех): метрики объявлены пакетными переменными (идиома
// клиента Prometheus — сравни с promauto.With(reg).NewCounterVec в
// офхендбуке client_golang) НЕ автоматически зарегистрированными в
// DefaultRegisterer — Register(reg) должен быть вызван РОВНО ОДИН раз, в
// orchestrator/main.go, с реестром конкретного *platform.Service
// (svc.Metrics().Registry()), иначе (a) метрики не попадут в ответ
// GET /metrics вообще, либо (b) повторный вызов Register с тем же реестром
// вызовет панику дублирующей регистрации — тот же контракт, что и у
// prometheus.Registry.MustRegister. Package-level переменные (а не поле
// какой-то структуры, прокидываемое через конструкторы Transitioner/
// registerMachineConn/ClientConnHub) — сознательное отступление от
// принятого в проекте DI многих других зависимостей (см. WithLogger/
// WithMasterKey и т.п.): у обоих счётчиков ровно ОДИН экземпляр на процесс
// (не завязаны на конкретный *Transitioner/*Server, которых тоже по одному),
// а протаскивать их explicit-параметром через глубокие цепочки вызовов
// (Transition → transition, registerMachineConn/unregisterMachineConn,
// ClientConnHub.register/unregister) добавило бы сигнатуры без реальной
// пользы — тот же компромисс, что сам клиент Prometheus рекомендует для
// метрик в его официальных примерах.
package metrics

import "github.com/prometheus/client_golang/prometheus"

// TaskTransitionsTotal считает переходы FSM задачи (task.Transitioner,
// orchestrator/internal/task/transition.go) по (from, to, trigger) —
// например {from="queued",to="running",trigger="agent_task_started"}.
// Инкрементируется ПОСЛЕ успешного commit транзакции перехода (см.
// Transitioner.transition) — неуспешные/откатившиеся попытки не считаются,
// иначе счётчик отражал бы не реальное движение задач, а количество попыток.
var TaskTransitionsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "agentify_orchestrator_task_transitions_total",
	Help: "Количество успешных переходов статуса задачи (FSM) по (from, to, trigger).",
}, []string{"from", "to", "trigger"})

// MachineWSConnectionsActive — gauge количества АКТИВНЫХ прямо сейчас
// WS-соединений машин (integration_id, тикет 2.4/3.4). Не более одного
// активного соединения на интеграцию (ADR 0002) — это НЕ количество
// интеграций вообще, а именно то, сколько машин сейчас реально на связи;
// напрямую отражает, доставляются ли команды (см. годок пакета bridge про
// зависимость доставки от активного соединения). Инкрементируется в
// registerMachineConn, декрементируется в unregisterMachineConn
// (orchestrator/internal/api/machine_ws.go) — СИММЕТРИЧНО, gauge не должен
// «уплывать» при рестартах процесса (при рестарте реестр machineConns и сам
// процесс пересоздаются заново, gauge стартует с нуля вместе с ними).
var MachineWSConnectionsActive = prometheus.NewGauge(prometheus.GaugeOpts{
	Name: "agentify_orchestrator_machine_ws_connections_active",
	Help: "Количество активных прямо сейчас WS-соединений машин (интеграций) к оркестратору.",
})

// ClientWSConnectionsActive — gauge количества активных WS-соединений
// браузеров (тикет 7.2, ClientConnHub, orchestrator/internal/api/client_ws.go).
// В отличие от MachineWSConnectionsActive один user_id может держать НЕСКОЛЬКО
// одновременных соединений (несколько вкладок) — это сумма ВСЕХ них, не
// количество уникальных пользователей.
var ClientWSConnectionsActive = prometheus.NewGauge(prometheus.GaugeOpts{
	Name: "agentify_orchestrator_client_ws_connections_active",
	Help: "Количество активных прямо сейчас WS-соединений web-клиентов (вкладок браузера) к оркестратору.",
})

// Register регистрирует все метрики этого пакета в реестре reg. Вызывается
// РОВНО ОДИН раз из orchestrator/main.go с svc.Metrics().Registry() — до
// начала обслуживания HTTP/WS-трафика (см. run в main.go), чтобы ни одно
// увеличение счётчика не произошло раньше регистрации (само по себе не
// паникует — WithLabelValues на незарегистрированной метрике безопасен,
// значение просто не попадёт в GET /metrics, пока Register не вызван).
func Register(reg *prometheus.Registry) {
	reg.MustRegister(TaskTransitionsTotal, MachineWSConnectionsActive, ClientWSConnectionsActive)
}
