package database

import "github.com/Filin153/gosaga/domain"

type TaskRepository interface {
	WithSession(sess Session) TaskRepository
	Create(task *domain.SagaTask) (int64, error)
	GetByID(id int64) (*domain.SagaTask, error)
	GetByIdempotencyKey(key string) (*domain.SagaTask, error)
	GetByStatus(status domain.TaskStatus) ([]domain.SagaTask, error)
	Update(task *domain.SagaTask) error
	UpdateByID(id int64, update domain.SagaTaskUpdate) error
	Delete(id int64) error
}

type DLQRepository interface {
	WithSession(sess Session) DLQRepository
	Create(task *domain.DLQTask) (int64, error)
	GetByID(id int64) (*domain.DLQTask, error)
	GetByTaskID(taskID int64) (*domain.DLQTask, error)
	GetByStatus(status domain.TaskStatus) ([]domain.DLQEntry, error)
	GetErrorsWithAttempts() ([]domain.DLQEntry, error)
	Update(task *domain.DLQTask) error
	UpdateByID(id int64, update domain.DLQTaskUpdate) error
	Delete(id int64) error
}
