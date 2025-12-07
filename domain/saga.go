package domain

import (
	"encoding/json"
	"time"
)

type TaskStatus string

const (
	TaskStatusWait              TaskStatus = "wait"
	TaskStatusReserved          TaskStatus = "reserved"
	TaskStatusWork              TaskStatus = "work"
	TaskStatusReady             TaskStatus = "ready"
	TaskStatusError             TaskStatus = "error"
	TaskStatusRollback          TaskStatus = "rollback"
	TaskStatusErrorRollbackNone TaskStatus = "error_rollback_none"
)

type SagaMsg struct {
	Key   string
	Value any
	Topic string
}

type SagaTask struct {
	ID             int64
	IdempotencyKey string
	Data           json.RawMessage
	RollbackData   *json.RawMessage
	Status         TaskStatus
	Info           *string
	UpdatedAt      time.Time
}

type DLQTask struct {
	ID             int64
	TaskID         int64
	TimeForNextTry int
	TimeMul        int
	MaxAttempts    int
	HaveAttempts   int
	UpdatedAt      time.Time
}

type DLQEntry struct {
	Task SagaTask
	DLQ  DLQTask
}

// SagaTaskUpdate allows typed partial updates for saga tasks.
type SagaTaskUpdate struct {
	IdempotencyKey *string          `db:"idempotency_key"`
	Data           *json.RawMessage `db:"data"`
	RollbackData   *json.RawMessage `db:"rollback_data"`
	Status         *TaskStatus      `db:"status"`
	Info           *string          `db:"info"`
}

// DLQTaskUpdate allows typed partial updates for DLQ records.
type DLQTaskUpdate struct {
	TaskID         *int64 `db:"task_id"`
	TimeForNextTry *int   `db:"time_for_next_try"`
	TimeMul        *int   `db:"time_mul"`
	MaxAttempts    *int   `db:"max_attempts"`
	HaveAttempts   *int   `db:"have_attempts"`
}
