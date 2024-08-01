package kafka

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Producer struct {
	Instance         kafka.Producer
	BootstrapServers string
	TotalMsgcnt      int
}

func NewProducer(bootstrapServers string, totalMessageCount int) *Producer {
	_instance, _ := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": bootstrapServers})

	return &Producer{
		Instance:         *_instance,
		BootstrapServers: bootstrapServers,
		TotalMsgcnt:      totalMessageCount,
	}
}

func (p *Producer) Listen() {
	go func() {
		for e := range p.Instance.Events() {
			switch ev := e.(type) {
			case kafka.Error:
				// Generic client instance-level errors, such as
				// broker connection failures, authentication issues, etc.
				//
				// These errors should generally be considered informational
				// as the underlying client will automatically try to
				// recover from any errors encountered, the application
				// does not need to take action on them.
				fmt.Printf("Error: %v\n", ev)
			default:
				fmt.Printf("Ignored event: %s\n", ev)
			}
		}
	}()
}

func (p *Producer) Publish(ctx context.Context, event Message, topic string) (err error) {
	//create buffer for temporary storage of event bytes
	var buf bytes.Buffer
	//initiate gob encoder
	eventBytes := gob.NewEncoder(&buf)
	//encode event to buffer
	if err = eventBytes.Encode(event); err != nil {
		return err
	}

	//publish event to kafka
	kafkaMessage := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          buf.Bytes(),
		Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
	}

	deliveryChan := make(chan kafka.Event)
	err = p.Instance.Produce(kafkaMessage, deliveryChan)
	if err != nil {
		return fmt.Errorf("failed to produce message: %w", err)
	}

	// Wait for delivery report or error
	e := <-deliveryChan
	m := e.(*kafka.Message)

	// Check for delivery errors
	if m.TopicPartition.Error != nil {
		return fmt.Errorf("delivery failed: %s", m.TopicPartition.Error)
	}

	// Close the delivery channel
	close(deliveryChan)

	return nil
}
