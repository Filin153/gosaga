package gosaga

import (
	"context"

	"github.com/Filin153/gosaga/domain"
	"github.com/Filin153/gosaga/storage/database"
)

// WorkerInterface описывает воркера, который обрабатывает задачи саги.
// Экспортируется, чтобы можно было использовать в моках и внешнем коде.
type WorkerInterface interface {
	New(ctx context.Context) (WorkerInterface, error)
	Worker(ctx context.Context, task *domain.SagaTask, sess database.Session) error
	DlqWorker(ctx context.Context, task *domain.SagaTask, sess database.Session) error
}
