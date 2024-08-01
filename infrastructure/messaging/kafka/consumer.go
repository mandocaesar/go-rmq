package kafka

import "github.com/confluentinc/confluent-kafka-go/kafka"

type ConsumerHandler func(err error)

type Consumer struct {
	Instance         kafka.Consumer
	BootstrapServers string
}

func NewConsumer(bootstrapServer, groupID string) *Consumer {
	_instance, _ := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServer,
		"group.id":          groupID,
		"auto.offset.reset": "earliest",
	})

	return _instance
}
