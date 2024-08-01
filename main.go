package main

import (
	"context"
	"go-rmq/infrastructure/messaging/kafka"
)

func main() {

	producer := kafka.NewProducer("localhost:9092", 3)
	ctx := context.Background()

	message := kafka.Message{Key: "1", Value: "test"}
	err := producer.Publish(ctx, message, "my-topic")
	if err != nil {
		println(err.Error())
	}
}
