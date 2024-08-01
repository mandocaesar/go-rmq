package config

import "github.com/rabbitmq/amqp091-go"

type RabbitMQConfig struct {
	// uri for rabbitmq
	AmqpURI string

	// Is queue and exchange will survives broker restart or not, if durable is true, then it will survive, no otherwise
	Durable bool

	// Exclusive queues are deleted when their declaring connection is closed or gone (e.g. due to underlying TCP connection loss).
	// They therefore are only suitable for client-specific transient state.
	Exclusive bool

	// An auto-delete queue will be deleted when its last consumer is cancelled (e.g. using the basic.cancel in AMQP 0-9-1)
	// or gone (closed channel or connection,
	// or lost TCP connection with the server).
	AutoDeleted bool

	// Is exchange for internal routing only or for external
	// external exchange pass message to another exchange on another cluster
	Internal bool

	// do we wait for confirmation of connection or not
	NoWait bool

	// additional arguments
	Arguments amqp091.Table
}
