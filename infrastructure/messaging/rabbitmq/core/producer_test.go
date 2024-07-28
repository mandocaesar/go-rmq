package core

import (
	"go-rmq/infrastructure/messaging/common/config"
	"testing"

	"github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
)

func TestShouldBeAbleToPublishToDirectQueue(t *testing.T) {
	config := config.RabbitMQConfig{
		AmqpURI:     "amqp://guest:guest@localhost:5672/",
		Durable:     true,
		Exclusive:   false,
		AutoDeleted: false,
		Internal:    false,
		NoWait:      false,
		Arguments:   nil,
	}
	_, err := CreateProducer(
		"direct_exchange",
		amqp091.ExchangeDirect,
		config,
	)
	assert.Equal(t, nil, err)
}
