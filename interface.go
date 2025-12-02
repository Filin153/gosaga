package gosaga

import (
	"context"
	"github.com/Filin153/gosaga/domain"
	"github.com/Filin153/gosaga/storage/database"
)

type gosagaWorkerInterface interface {
	New(ctx context.Context) (gosagaWorkerInterface, error)
	Worker(task *domain.SagaTask, sess database.Session) error
	DlqWorker(task *domain.SagaTask, sess database.Session) error
}
