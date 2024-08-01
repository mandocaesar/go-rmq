<<<<<<< HEAD
package core

import (
	"context"
	"fmt"
	"go-rmq/infrastructure/common/config"
	"log"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// This is a wrapper for doing amqp dial connection of amqp.Dial and amqp.Channel, and contains reconnection method
// whenever connection is closed by accident
// source: https://github.com/isayme/go-amqp-reconnect/blob/fc811b0bcda2dca67a3ab641135421c14c41696e/rabbitmq/rabbitmq.go

// Connection amqp.Connection wrapper
type Connection struct {
	*amqp.Connection
}

// Dial wrap amqp.Dial, dial and get a reconnect connection
func Dial(url string) (*Connection, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("error on connection %v", err)
	}

	connection := &Connection{
		Connection: conn,
	}

	go func() {
		for {
			reason, ok := <-connection.Connection.NotifyClose(make(chan *amqp.Error))
			// exit this goroutine if closed by developer
			if !ok {
				log.Println(context.Background(), "connection closed intentionally")
				break
			}

			log.Println("connection closed, reason: %v", reason)

			// reconnect if not closed by developer
			for {
				log.Println("wait %vs for reconnect", config.RABBITMQ_RECONNECTION_DELAY_SECONDS)

				time.Sleep(time.Duration(config.RABBITMQ_RECONNECTION_DELAY_SECONDS) * time.Second)

				log.Println("reconnecting...")

				conn, err := amqp.Dial(url)
				if err == nil {
					connection.Connection = conn
					log.Println("reconnection success..")
					break
				}

				log.Println(context.Background(), map[string]interface{}{"url": url}, err, "reconnect failed")
			}
		}
	}()

	return connection, nil
}

// Channel wrap amqp.Connection.Channel, get a auto reconnect channel
func (c *Connection) Channel() (*Channel, error) {
	ch, err := c.Connection.Channel()
	if err != nil {
		return nil, err
	}

	channel := &Channel{
		Channel: ch,
	}

	go func() {
		for {
			reason, ok := <-channel.Channel.NotifyClose(make(chan *amqp.Error))
			// exit this goroutine if closed by developer
			if !ok || channel.IsClosed() {
				log.Println("channel closed intentionally")
				channel.Close() // close again, ensure closed flag set when connection closed
				break
			}

			log.Println("channel closed, reason: %v", reason)

			// reconnect if not closed by developer
			for {
				log.Println("wait %vs for reconnect", config.RABBITMQ_RECONNECTION_DELAY_SECONDS)

				time.Sleep(time.Duration(config.RABBITMQ_RECONNECTION_DELAY_SECONDS) * time.Second)

				log.Println("reconnecting...")

				ch, err := c.Connection.Channel()
				if err == nil {
					log.Println("channel recreate success")
					channel.Channel = ch
					break
				}

				log.Println("channel recreate failed, err: %v", err)
			}
		}

	}()

	return channel, nil
}

// Channel amqp.Channel wrapper
type Channel struct {
	*amqp.Channel
	closed int32
}

// Consume wrap amqp.Channel.Consume, the returned delivery will end only when channel closed by developer
func (ch *Channel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	deliveries := make(chan amqp.Delivery)

	go func() {
		for {
			d, err := ch.Channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
			if err != nil {
				log.Println(context.Background(), map[string]interface{}{"queue": queue,
					"consumer": consumer, "autoAck": autoAck, "exclusive": exclusive, "noLocal": noLocal, "noWait": noWait, "args": args}, err, "reconsuming failed")
				time.Sleep(time.Duration(config.RABBITMQ_RECONNECTION_DELAY_SECONDS) * time.Second)
				continue
			}
			log.Println("consume success..")

			for msg := range d {
				deliveries <- msg
			}

			// sleep before IsClose call. closed flag may not set before sleep.
			time.Sleep(time.Duration(config.RABBITMQ_RECONNECTION_DELAY_SECONDS) * time.Second)

			if ch.IsClosed() {
				break
			}
		}
	}()

	return deliveries, nil
}

// IsClosed indicate closed by developer
func (ch *Channel) IsClosed() bool {
	return (atomic.LoadInt32(&ch.closed) == 1)
}

// Close ensure closed flag set
func (ch *Channel) Close() error {
	if ch.IsClosed() {
		return amqp.ErrClosed
	}

	atomic.StoreInt32(&ch.closed, 1)

	return ch.Channel.Close()
}
=======
package core

import (
	"context"
	"fmt"
	"go-rmq/infrastructure/common/config"
	"log"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// This is a wrapper for doing amqp dial connection of amqp.Dial and amqp.Channel, and contains reconnection method
// whenever connection is closed by accident
// source: https://github.com/isayme/go-amqp-reconnect/blob/fc811b0bcda2dca67a3ab641135421c14c41696e/rabbitmq/rabbitmq.go

// Connection amqp.Connection wrapper
type Connection struct {
	*amqp.Connection
}

// Dial wrap amqp.Dial, dial and get a reconnect connection
func Dial(url string) (*Connection, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("error on connection %v", err)
	}

	connection := &Connection{
		Connection: conn,
	}

	go func() {
		for {
			reason, ok := <-connection.Connection.NotifyClose(make(chan *amqp.Error))
			// exit this goroutine if closed by developer
			if !ok {
				log.Print(context.Background(), "connection closed intentionally")
				break
			}

			log.Print("connection closed, reason: %v", reason)

			// reconnect if not closed by developer
			for {
				log.Print("wait %vs for reconnect", config.RABBITMQ_RECONNECTION_DELAY_SECONDS)

				time.Sleep(time.Duration(config.RABBITMQ_RECONNECTION_DELAY_SECONDS) * time.Second)

				log.Print("reconnecting...")

				conn, err := amqp.Dial(url)
				if err == nil {
					connection.Connection = conn
					log.Print("reconnection success..")
					break
				}

				log.Print(context.Background(), map[string]interface{}{"url": url}, err, "reconnect failed")
			}
		}
	}()

	return connection, nil
}

// Channel wrap amqp.Connection.Channel, get a auto reconnect channel
func (c *Connection) Channel() (*Channel, error) {
	ch, err := c.Connection.Channel()
	if err != nil {
		return nil, err
	}

	channel := &Channel{
		Channel: ch,
	}

	go func() {
		for {
			reason, ok := <-channel.Channel.NotifyClose(make(chan *amqp.Error))
			// exit this goroutine if closed by developer
			if !ok || channel.IsClosed() {
				log.Print("channel closed intentionally")
				channel.Close() // close again, ensure closed flag set when connection closed
				break
			}

			log.Print("channel closed, reason: %v", reason)

			// reconnect if not closed by developer
			for {
				log.Print("wait %vs for reconnect", config.RABBITMQ_RECONNECTION_DELAY_SECONDS)

				time.Sleep(time.Duration(config.RABBITMQ_RECONNECTION_DELAY_SECONDS) * time.Second)

				log.Print("reconnecting...")

				ch, err := c.Connection.Channel()
				if err == nil {
					log.Print("channel recreate success")
					channel.Channel = ch
					break
				}

				log.Print("channel recreate failed, err: %v", err)
			}
		}

	}()

	return channel, nil
}

// Channel amqp.Channel wrapper
type Channel struct {
	*amqp.Channel
	closed int32
}

// Consume wrap amqp.Channel.Consume, the returned delivery will end only when channel closed by developer
func (ch *Channel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	deliveries := make(chan amqp.Delivery)

	go func() {
		for {
			d, err := ch.Channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
			if err != nil {
				log.Print(context.Background(), map[string]interface{}{"queue": queue,
					"consumer": consumer, "autoAck": autoAck, "exclusive": exclusive, "noLocal": noLocal, "noWait": noWait, "args": args}, err, "reconsuming failed")
				time.Sleep(time.Duration(config.RABBITMQ_RECONNECTION_DELAY_SECONDS) * time.Second)
				continue
			}
			log.Print("consume success..")

			for msg := range d {
				deliveries <- msg
			}

			// sleep before IsClose call. closed flag may not set before sleep.
			time.Sleep(time.Duration(config.RABBITMQ_RECONNECTION_DELAY_SECONDS) * time.Second)

			if ch.IsClosed() {
				break
			}
		}
	}()

	return deliveries, nil
}

// IsClosed indicate closed by developer
func (ch *Channel) IsClosed() bool {
	return (atomic.LoadInt32(&ch.closed) == 1)
}

// Close ensure closed flag set
func (ch *Channel) Close() error {
	if ch.IsClosed() {
		return amqp.ErrClosed
	}

	atomic.StoreInt32(&ch.closed, 1)

	return ch.Channel.Close()
}
>>>>>>> c87852bcc90639980b0dea39072af6e7a7e2033f
