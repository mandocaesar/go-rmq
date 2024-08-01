package main

import "go-rmq/infrastructure/messaging/kafka"

func main() {

	consumer := kafka.NewConsumer("localhost:9092", "test-group")

	consumer.Consume(nil, "my-topic")
}
