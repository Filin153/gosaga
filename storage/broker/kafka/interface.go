package kafka

import (
	"context"

	"github.com/Filin153/gosaga/domain"

	"github.com/IBM/sarama"
)

type Writer interface {
	Write(ctx context.Context, msg *domain.SagaMsg, rollback *domain.SagaMsg, idempotencyKey string) error
}

type Reader interface {
	// Run запускает чтение сообщений с использованием переданного контекста.
	Run(ctx context.Context)
	Shutdown() error
	Read() <-chan *sarama.ConsumerMessage
}
