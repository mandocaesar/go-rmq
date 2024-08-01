package core

import (
	"fmt"
	"go-rmq/infrastructure/common/randomid"
	"go-rmq/infrastructure/messaging/common/config"

	amqp "github.com/rabbitmq/amqp091-go"
)

type ConsumerHandler func(delivery amqp.Delivery, err error)

type Consumer struct {
	channel      *amqp.Channel
	exchangeName string
	exchangeType string
	queueName    string
	config       config.RabbitMQConfig

	// used for tagging this consumer for safely shutdown the consumer without losing messages
	ctag string
}

// Listen for incoming messages from the queue
// handler is a callback function to receives every message
func (c *Consumer) Listen(handler ConsumerHandler) error {
	delivery, err := c.channel.Consume(
		c.queueName,
		"", // will produce unique identity for consumer
		false,
		c.config.Exclusive,
		false,
		c.config.NoWait,
		nil,
	)
	if err != nil {
		return fmt.Errorf("error on start consuming %v", err)
	}

	go func() {
		for d := range delivery {
			handler(d, err)
		}
	}()

	return nil
}

func (c *Consumer) Shutdown() error {
	err := c.channel.Cancel(c.ctag, false)
	if err != nil {
		return fmt.Errorf("error on cancelling channel on consumer: %s, error: %v", c.ctag, err)
	}

	err = c.channel.Close()
	if err != nil {
		return fmt.Errorf("error on closing connection on consumer: %s, error: %v", c.ctag, err)
	}

	return nil
}

func CreateConsumer(
	exchangeName,
	exchangeType,
	queueName string,
	config config.RabbitMQConfig,
) (*Consumer, error) {
	// use queuename as routing key for direct and fanout exchange
	return CreateConsumerWithRouting(exchangeName, exchangeType, queueName, queueName, config)
}

func CreateConsumerWithRouting(
	exchangeName,
	exchangeType,
	queueName string,
	routingKey string,
	config config.RabbitMQConfig,
) (*Consumer, error) {
	connection, err := amqp.Dial(config.AmqpURI)
	if err != nil {
		return nil, fmt.Errorf("error on connecting to amqp: %v", err)
	}

	channel, err := connection.Channel()
	if err != nil {
		return nil, fmt.Errorf("error on creating channel %v", err)
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
		return nil, fmt.Errorf("error on declaring exchange %s on consumer, %v", exchangeName, err)
	}

	_, err = channel.QueueDeclare(
		queueName,
		config.Durable,
		config.AutoDeleted,
		config.Exclusive,
		config.NoWait,
		config.Arguments,
	)
	if err != nil {
		return nil, fmt.Errorf("error declaring queue %s, %v", queueName, err)
	}

	err = channel.QueueBind(
		queueName,
		routingKey,
		exchangeName,
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("error on binding queue %s to exchange %s, error: %v", queueName, exchangeName, err)
	}

	return &Consumer{
		channel:      channel,
		exchangeName: exchangeName,
		exchangeType: exchangeType,
		queueName:    queueName,
		config:       config,
		ctag:         randomid.GenerateDefault(),
	}, nil
}
