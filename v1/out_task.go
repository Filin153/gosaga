package gosaga

import (
	"context"

	"github.com/Filin153/gosaga/domain"
	"github.com/Filin153/gosaga/storage/broker/kafka"
	"github.com/Filin153/gosaga/storage/database"
)

type OutWorker struct {
	kafkaWriter kafka.Writer
}

func NewOutWorker(kafkaWriter kafka.Writer) *OutWorker {
	return &OutWorker{
		kafkaWriter: kafkaWriter,
	}
}

func (w *OutWorker) Worker(ctx context.Context, task *domain.SagaTask, sess database.Session) error {
	msgData, err := Unmarshal(task)
	if err != nil {
		return err
	}

	var rollbackMsg *domain.SagaMsg
	if task.RollbackData != nil {
		if r, err := domain.DecodeSagaMsg(*task.RollbackData); err == nil {
			rollbackMsg = r
		}
	}

	// KafkaWriter не хранит контекст, поэтому контекст передаётся снаружи при создании Writer.
	err = w.kafkaWriter.Write(ctx, msgData, rollbackMsg, task.IdempotencyKey)
	if err != nil {
		return err
	}

	return nil
}

func (w *OutWorker) DlqWorker(ctx context.Context, task *domain.SagaTask, sess database.Session) error {
	return w.Worker(ctx, task, sess)
}
