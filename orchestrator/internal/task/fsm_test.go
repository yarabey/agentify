package task

import "testing"

// TestNextStatus_ValidTransitions — прямая приёмка тикета 5.2: "все валидные
// переходы ок". Итерируется по САМОЙ таблице transitions (тест в одном
// пакете с fsm.go, доступ к неэкспортированному полю), так что при
// добавлении нового ребра в transitions тест автоматически покрывает его —
// расхождение таблицы и теста физически невозможно.
func TestNextStatus_ValidTransitions(t *testing.T) {
	if len(transitions) != 18 {
		t.Fatalf("ожидалось 18 переходов (дословный перенос docs/\"Жизненный цикл задачи.md\"), получено %d", len(transitions))
	}
	for key, want := range transitions {
		got, err := NextStatus(key.from, key.trigger)
		if err != nil {
			t.Errorf("NextStatus(%s, %s) вернул неожиданную ошибку: %v", key.from, key.trigger, err)
			continue
		}
		if got != want {
			t.Errorf("NextStatus(%s, %s) = %s, хотим %s", key.from, key.trigger, got, want)
		}
	}
}

// TestNextStatus_InvalidTransitions — прямая приёмка тикета 5.2: "невалидные
// отвергнуты". Покрывает: терминальные статусы (никаких исходящих переходов),
// намеренно отсутствующее ребро Created→Cancelled, несколько случаев
// "правильный статус, неподходящий триггер" и неизвестный Status.
func TestNextStatus_InvalidTransitions(t *testing.T) {
	cases := []struct {
		name    string
		from    Status
		trigger Trigger
	}{
		// Терминальные статусы — из диаграммы нет ни одного исходящего ребра.
		{"completed + cancel_requested", StatusCompleted, TriggerCancelRequested},
		{"completed + task_accepted", StatusCompleted, TriggerTaskAccepted},
		{"failed + cancel_requested", StatusFailed, TriggerCancelRequested},
		{"failed + user_confirmed", StatusFailed, TriggerUserConfirmed},
		{"cancelled + enqueued", StatusCancelled, TriggerEnqueued},
		{"cancelled + machine_recovered", StatusCancelled, TriggerMachineRecovered},

		// Created → Cancelled намеренно отсутствует в диаграмме: отменить
		// можно только уже поставленную в очередь задачу, не Created.
		{"created + cancel_requested (ребра нет)", StatusCreated, TriggerCancelRequested},

		// Правильный статус, неподходящий триггер.
		{"queued + agent_question", StatusQueued, TriggerAgentQuestion},
		{"running + task_accepted (повторно)", StatusRunning, TriggerTaskAccepted},
		{"awaiting_confirm + timeout", StatusAwaitingConfirm, TriggerTimeout},

		// Неизвестный статус.
		{"unknown status", Status("unknown"), TriggerEnqueued},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NextStatus(tc.from, tc.trigger)
			if err == nil {
				t.Fatalf("NextStatus(%s, %s) = %s, ожидалась ошибка недопустимого перехода", tc.from, tc.trigger, got)
			}
			if got != "" {
				t.Fatalf("NextStatus(%s, %s) при ошибке должен вернуть пустой Status, получено %s", tc.from, tc.trigger, got)
			}
		})
	}
}
