package gosaga

import (
	"context"
	"encoding/json"
	"github.com/Filin153/gosaga/domain"
	"github.com/Filin153/gosaga/storage/broker/kafka"
	"github.com/Filin153/gosaga/storage/database"
)

type OutWorker struct {
	ctx         context.Context
	kafkaWriter kafka.Writer
}

func NewOutWorker(ctx context.Context, kafkaWriter kafka.Writer) *OutWorker {
	return &OutWorker{
		ctx:         ctx,
		kafkaWriter: kafkaWriter,
	}
}

func New(ctx context.Context) (*OutWorker, error) {
	return &OutWorker{
		ctx: ctx,
	}, nil
}

func (w *OutWorker) Worker(task *domain.SagaTask, sess database.Session) error {
	msgData, err := Unmarshal(task)
	if err != nil {
		return err
	}

	var rollbackMsg *domain.SagaMsg
	if task.RollbackData != nil {
		var r domain.SagaMsg
		if err := json.Unmarshal(*task.RollbackData, &r); err == nil {
			rollbackMsg = &r
		}
	}

	err = w.kafkaWriter.Write(msgData, rollbackMsg, task.IdempotencyKey)
	if err != nil {
		return err
	}

	return nil
}

func (w *OutWorker) DlqWorker(task *domain.SagaTask, sess database.Session) error {
	return w.Worker(task, sess)
}

func (w *OutWorker) Rollback(task *domain.SagaTask, sess database.Session) error {
	return nil
}
