package database

import (
	"context"

	"github.com/Filin153/gosaga/domain"
)

type TaskRepository interface {
	WithSession(sess Session) TaskRepository
	Create(ctx context.Context, task *domain.SagaTask) (int64, error)
	GetByID(ctx context.Context, id int64) (*domain.SagaTask, error)
	GetByIdempotencyKey(ctx context.Context, key string) (*domain.SagaTask, error)
	GetByStatus(ctx context.Context, status domain.TaskStatus, limit int) ([]domain.SagaTask, error)
	Update(ctx context.Context, task *domain.SagaTask) error
	UpdateByID(ctx context.Context, id int64, update domain.SagaTaskUpdate) error
	Delete(ctx context.Context, id int64) error
}

type DLQRepository interface {
	WithSession(sess Session) DLQRepository
	Create(ctx context.Context, task *domain.DLQTask) (int64, error)
	GetByID(ctx context.Context, id int64) (*domain.DLQTask, error)
	GetByTaskID(ctx context.Context, taskID int64) (*domain.DLQTask, error)
	GetByStatus(ctx context.Context, status domain.TaskStatus, limit int) ([]domain.DLQEntry, error)
	GetErrorsWithAttempts(ctx context.Context) ([]domain.DLQEntry, error)
	Update(ctx context.Context, task *domain.DLQTask) error
	UpdateByID(ctx context.Context, id int64, update domain.DLQTaskUpdate) error
	Delete(ctx context.Context, id int64) error
}
