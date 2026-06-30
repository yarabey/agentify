package bus

import "testing"

// TestTopicNamesNonEmptyAndUnique страхует раскладку ADR 0001 от случайной
// порчи: имена топиков не пусты и попарно различны (иначе два направления
// схлопнутся в один топик).
func TestTopicNamesNonEmptyAndUnique(t *testing.T) {
	topics := map[string]string{
		"TopicMachineCommands":       TopicMachineCommands,
		"TopicMachineEvents":         TopicMachineEvents,
		"TopicNotificationsTelegram": TopicNotificationsTelegram,
	}
	seen := make(map[string]string, len(topics))
	for name, value := range topics {
		if value == "" {
			t.Errorf("%s: имя топика не должно быть пустым", name)
		}
		if prev, dup := seen[value]; dup {
			t.Errorf("%s и %s имеют одинаковое значение %q — топики должны быть уникальны", prev, name, value)
		}
		seen[value] = name
	}
}

// TestPartitionKeysNonEmptyAndUnique проверяет, что ключи партиций не пусты и
// различны — каждый соответствует своему полю конверта (protocol.md §2).
func TestPartitionKeysNonEmptyAndUnique(t *testing.T) {
	keys := map[string]string{
		"PartitionKeyIntegrationID": PartitionKeyIntegrationID,
		"PartitionKeyTaskID":        PartitionKeyTaskID,
		"PartitionKeyUserID":        PartitionKeyUserID,
	}
	seen := make(map[string]string, len(keys))
	for name, value := range keys {
		if value == "" {
			t.Errorf("%s: ключ партиции не должен быть пустым", name)
		}
		if prev, dup := seen[value]; dup {
			t.Errorf("%s и %s имеют одинаковое значение %q — ключи должны быть уникальны", prev, name, value)
		}
		seen[value] = name
	}
}
