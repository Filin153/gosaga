package kafka

import (
	"github.com/Filin153/gosaga/domain"

	"github.com/IBM/sarama"
)

type Writer interface {
	Write(msg *domain.SagaMsg, rollback *domain.SagaMsg, idempotencyKey string) error
}

type Reader interface {
	Run()
	Shutdown() error
	Read() <-chan *sarama.ConsumerMessage
}
