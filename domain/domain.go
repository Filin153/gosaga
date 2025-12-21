package domain

import (
	"bytes"
	"encoding/gob"
	"errors"
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
	Value []byte
	Topic string
}

type SagaTask struct {
	ID             int64
	IdempotencyKey string
	Data           []byte
	RollbackData   *[]byte
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
	IdempotencyKey *string     `db:"idempotency_key"`
	Data           *[]byte     `db:"data"`
	RollbackData   *[]byte     `db:"rollback_data"`
	Status         *TaskStatus `db:"status"`
	Info           *string     `db:"info"`
}

// DLQTaskUpdate allows typed partial updates for DLQ records.
type DLQTaskUpdate struct {
	TaskID         *int64 `db:"task_id"`
	TimeForNextTry *int   `db:"time_for_next_try"`
	TimeMul        *int   `db:"time_mul"`
	MaxAttempts    *int   `db:"max_attempts"`
	HaveAttempts   *int   `db:"have_attempts"`
}

// EncodeSagaMsg serializes SagaMsg using Go's builtin gob.
func EncodeSagaMsg(msg *SagaMsg) ([]byte, error) {
	if msg == nil {
		return nil, errors.New("nil saga msg")
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(msg); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DecodeSagaMsg deserializes SagaMsg encoded by EncodeSagaMsg.
func DecodeSagaMsg(data []byte) (*SagaMsg, error) {
	if len(data) == 0 {
		return nil, errors.New("empty saga msg data")
	}
	var msg SagaMsg
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&msg); err != nil {
		return nil, err
	}
	return &msg, nil
}
