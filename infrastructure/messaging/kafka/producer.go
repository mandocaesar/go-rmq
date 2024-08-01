package kafka

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Producer struct {
	Instance         kafka.Producer
	BootstrapServers string
	TotalMsgcnt      int
}

func NewProducser(bootstrapServers string, totalMessageCount int) *Producer {
	return &Producer{
		Instance:         kafka.Producer{},
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

func (p *Producer) Publish(ctx context.Context, event interface{}, topic string) (err error) {
	//create buffer for temporary storage of event bytes
	var buf bytes.Buffer
	//initiate gob encoder
	eventBytes := gob.NewEncoder(&buf)
	//encode event to buffer
	if err = eventBytes.Encode(event); err != nil {
		return err
	}

	//publish event to kafka
	err = p.Instance.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          buf.Bytes(),
		Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
	}, nil)

	//catch any errors
	if err != nil {
		if err.(kafka.Error).Code() == kafka.ErrQueueFull {
			// Producer queue is full, wait 1s for messages
			// to be delivered then try again.
			time.Sleep(time.Second)
		}
		fmt.Printf("Failed to produce message: %v\n", err)
	}

	return nil
}
