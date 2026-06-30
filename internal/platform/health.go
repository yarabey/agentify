package platform

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
)

// healthResponse — тело ответа GET /healthz. Поле status всегда "ok" при живом
// процессе; service позволяет проверяющему (docker compose / Caddy / smoke-тест
// тикета 0.3) понять, какой именно сервис ответил.
type healthResponse struct {
	Status  string `json:"status"`
	Service string `json:"service"`
}

// NewHealthRouter возвращает chi-роутер с единственным эндпоинтом
// GET /healthz, отдающим 200 и тело {"status":"ok","service":"<service>"}.
//
// Эндпоинт нужен инфраструктуре для проверки живости сервиса (приёмка тикета
// 0.3 «/healthz всех сервисов отвечает»); он намеренно не делает глубоких
// проверок (БД, шина) — это liveness, а не readiness, и в MVP этого достаточно.
// Выделен в отдельную функцию, чтобы покрываться httptest без поднятия сокета.
func NewHealthRouter(service string, logger *slog.Logger) chi.Router {
	r := chi.NewRouter()
	r.Get("/healthz", healthHandler(service, logger))
	return r
}

// healthHandler формирует обработчик GET /healthz для конкретного сервиса.
func healthHandler(service string, logger *slog.Logger) http.HandlerFunc {
	body, err := json.Marshal(healthResponse{Status: "ok", Service: service})
	if err != nil {
		// Сериализация статической структуры не может упасть в норме; на всякий
		// случай деградируем до фиксированного ответа, не роняя процесс.
		body = []byte(`{"status":"ok"}`)
	}

	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if _, werr := w.Write(body); werr != nil && logger != nil {
			logger.Debug("не удалось записать ответ /healthz", slog.Any("error", werr))
		}
	}
}
