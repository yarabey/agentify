package bus

// Провижининг топиков Redpanda через franz-go kadm (тикет 3.2). Используется в
// integration-тестах для создания топиков с числом партиций из ADR 0001 (6/6/3).
// В проде топики провижинятся инфраструктурой (compose/скрипты, тикет 0.3) с
// полными атрибутами (ретеншн/cleanup) — здесь только создание с правильным
// числом партиций, без дублирования ретеншн-политики в коде.

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Число партиций топиков для MVP (ADR 0001, таблица «Топики и ключи партиций»).
// Не «магические» значения: единственный источник — ADR 0001.
const (
	// PartitionsMachineCommands — партиций у TopicMachineCommands (ADR 0001).
	PartitionsMachineCommands = 6
	// PartitionsMachineEvents — партиций у TopicMachineEvents (ADR 0001).
	PartitionsMachineEvents = 6
	// PartitionsNotificationsTelegram — партиций у TopicNotificationsTelegram (ADR 0001).
	PartitionsNotificationsTelegram = 3
)

// defaultReplication — фактор репликации для провижининга в тестах: одиночный
// брокер Redpanda (testcontainer) => 1. В проде задаётся инфраструктурой.
const defaultReplication = 1

// EnsureTopic создаёт топик с заданным числом партиций, если его ещё нет.
// Идемпотентна: уже существующий топик (kerr.TopicAlreadyExists) — не ошибка.
// Возвращает ошибку при сбое связи с брокером или ином отказе создания.
func EnsureTopic(ctx context.Context, seeds []string, topic string, partitions int32) error {
	client, err := kgo.NewClient(kgo.SeedBrokers(seeds...))
	if err != nil {
		return fmt.Errorf("bus: kadm-клиент для провижининга: %w", err)
	}
	defer client.Close()

	admin := kadm.NewClient(client)
	resp, err := admin.CreateTopic(ctx, partitions, defaultReplication, nil, topic)
	if err != nil {
		return fmt.Errorf("bus: создание топика %q: %w", topic, err)
	}
	if resp.Err != nil && !errors.Is(resp.Err, kerr.TopicAlreadyExists) {
		return fmt.Errorf("bus: создание топика %q: %w", topic, resp.Err)
	}
	return nil
}

// EnsureMVPTopics создаёт все три топика MVP с числом партиций из ADR 0001
// (machine.commands=6, machine.events=6, notifications.telegram=3). Идемпотентна.
// Предназначена для integration-тестов и локального провижининга.
func EnsureMVPTopics(ctx context.Context, seeds []string) error {
	topics := []struct {
		name       string
		partitions int32
	}{
		{TopicMachineCommands, PartitionsMachineCommands},
		{TopicMachineEvents, PartitionsMachineEvents},
		{TopicNotificationsTelegram, PartitionsNotificationsTelegram},
	}
	for _, t := range topics {
		if err := EnsureTopic(ctx, seeds, t.name, t.partitions); err != nil {
			return err
		}
	}
	return nil
}
