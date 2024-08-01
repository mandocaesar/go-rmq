package core

import (
	"fmt"
	"go-rmq/infrastructure/messaging/common/config"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Producer struct {
	channel      *amqp.Channel
	exchangeName string
	config       config.RabbitMQConfig

	PublishConfirmation chan amqp.Confirmation
}

// Publish publish message to the broker
// this is an asynchronous operation
// routing key can be the same as queue name for direct and fanout, or
// formatted binding keys, eg voila.#.messaging for topics exchange routing
func (p *Producer) Publish(body []byte, routingKey string) error {
	err := p.channel.Publish(
		p.exchangeName,
		routingKey,
		false,
		false,
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "application/json", // TODO: serialization format
			ContentEncoding: "utf-8",            // TODO: encoding
			Body:            body,
			DeliveryMode:    amqp.Persistent,
		},
	)
	if err != nil {
		return fmt.Errorf("error on publishing: %v", err)
	}

	return nil
}

// NotifyPublishConfirmation set rabbitmq to send publish confirmation
// through amqp.Confirmation channel
// if error occurs, it means that the rabbitmq config is set so that it can't send confirmation
func (p *Producer) NotifyPublishConfirmation() (chan amqp.Confirmation, error) {
	if err := p.channel.Confirm(false); err != nil {
		return nil, fmt.Errorf("can not set send confirmation: %v", err)
	}

	return p.channel.NotifyPublish(make(chan amqp.Confirmation)), nil
}

// CreateProducer connects and set up new exchange for publishing message
func CreateProducer(
	exchangeName string,
	exchangeType string,
	config config.RabbitMQConfig,
) (*Producer, error) {
	connection, err := amqp.Dial(config.AmqpURI)
	if err != nil {
		return nil, fmt.Errorf("error on connection %v", err)
	}

	channel, err := connection.Channel()
	if err != nil {
		return nil, fmt.Errorf("error on creating amqp channel %v", err)
	}

	err = channel.ExchangeDeclare(
		exchangeName,
		exchangeType,
		config.Durable,
		config.AutoDeleted,
		config.Internal,
		config.NoWait,
		config.Arguments,
	)
	if err != nil {
		return nil, fmt.Errorf("error on declaring exchange %s, err: %v", exchangeName, err)
	}

	producer := &Producer{
		channel:      channel,
		exchangeName: exchangeName,
		config:       config,
	}

	return producer, nil
}

type PublishResponse struct {
	Value string // can be OK or FAILED, if Value is FAILED, then error exists, otherwise error is nil
	Err   error  // error produced
}
