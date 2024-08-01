package kafka

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type ConsumerHandler func(err error)

type Consumer struct {
	Instance         *kafka.Consumer
	BootstrapServers string
}

type KafkaConsumerHandler func(event interface{}, err error)

func NewConsumer(bootstrapServer, groupID string) *Consumer {
	_instance, _ := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServer,
		"group.id":          groupID,
		"auto.offset.reset": "earliest",
	})

	return &Consumer{Instance: _instance, BootstrapServers: bootstrapServer}
}

func (c *Consumer) Consume(handler KafkaConsumerHandler, topic string) (err error) {
	c.Instance.Subscribe(topic, nil)
	if err != nil {
		println("Failed to subscribe to topic: %s\n", err)
		return err
	}

	// Setup a channel to handle OS signals for graceful shutdown
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt)

	// Start consuming messages
	run := true
	for run == true {
		select {
		case sig := <-sigchan:
			fmt.Printf("Received signal %v: terminating\n", sig)
			run = false
		default:
			// Poll for Kafka messages
			ev := c.Instance.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				// Process the consumed message
				fmt.Printf("Received message from topic %s: %s\n", *e.TopicPartition.Topic, string(e.Value))
			case kafka.Error:
				// Handle Kafka errors
				fmt.Printf("Error: %v\n", e)
			}
		}
	}
	return nil
}
