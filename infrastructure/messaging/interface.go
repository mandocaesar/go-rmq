package infrastructure

import (
	"context"
)

type Producer interface {
	Publish(ctx context.Context, event interface{}, routingKey string) error
}

type Consumer interface {
	Listen() error
}

type Handler[T interface{}] interface {
	Handle(ctx context.Context, event T) error
}
