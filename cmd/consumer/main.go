package main

import "go-rmq/infrastructure/messaging/kafka"

func Handler(event interface{}, err error) {

	if err != nil {
		println(err)
		return
	}

	println("%v", event)
}

func main() {

	consumer := kafka.NewConsumer("localhost:9092", "test-group", 100)

	consumer.Consume(Handler, "my-topic")
}
