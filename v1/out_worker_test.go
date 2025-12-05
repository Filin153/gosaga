package gosaga

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/Filin153/gosaga/domain"
	"github.com/stretchr/testify/require"
)

type captureWriter struct {
	mu          sync.Mutex
	callCount   int
	msg         *domain.SagaMsg
	rollback    *domain.SagaMsg
	idempotency string
	err         error
}

func (c *captureWriter) Write(ctx context.Context, msg *domain.SagaMsg, rollback *domain.SagaMsg, idempotencyKey string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.callCount++
	c.msg = msg
	c.rollback = rollback
	c.idempotency = idempotencyKey
	return c.err
}

func TestOutWorkerSuccess(t *testing.T) {
	writer := &captureWriter{}
	worker := NewOutWorker(writer)

	data, _ := json.Marshal(domain.SagaMsg{Key: "k", Value: map[string]int{"a": 1}, Topic: "t"})
	rbData, _ := json.Marshal(domain.SagaMsg{Key: "rk", Value: map[string]int{"b": 2}, Topic: "rt"})
	rawRB := json.RawMessage(rbData)
	task := &domain.SagaTask{
		IdempotencyKey: "id",
		Data:           data,
		RollbackData:   &rawRB,
	}

	err := worker.Worker(context.Background(), task, nil)
	require.NoError(t, err)

	writer.mu.Lock()
	require.Equal(t, 1, writer.callCount)
	require.Equal(t, "id", writer.idempotency)
	require.Equal(t, "k", writer.msg.Key)
	require.Equal(t, "t", writer.msg.Topic)
	require.NotNil(t, writer.rollback)
	require.Equal(t, "rk", writer.rollback.Key)
	writer.mu.Unlock()
}

func TestOutWorkerIgnoresInvalidRollback(t *testing.T) {
	writer := &captureWriter{}
	worker := NewOutWorker(writer)

	data, _ := json.Marshal(domain.SagaMsg{Key: "k", Topic: "t"})
	rawRB := json.RawMessage(`{invalid json}`)
	task := &domain.SagaTask{IdempotencyKey: "id", Data: data, RollbackData: &rawRB}

	err := worker.Worker(context.Background(), task, nil)
	require.NoError(t, err)

	writer.mu.Lock()
	require.Nil(t, writer.rollback)
	writer.mu.Unlock()
}

func TestOutWorkerWriterError(t *testing.T) {
	writer := &captureWriter{err: errors.New("writer boom")}
	worker := NewOutWorker(writer)

	data, _ := json.Marshal(domain.SagaMsg{Key: "k", Topic: "t"})
	task := &domain.SagaTask{IdempotencyKey: "id", Data: data}

	err := worker.Worker(context.Background(), task, nil)
	require.EqualError(t, err, "writer boom")
}
