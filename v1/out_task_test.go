package gosaga

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Filin153/gosaga/domain"
	kmocks "github.com/Filin153/gosaga/mocks/kafka"
	"github.com/stretchr/testify/mock"
)

func TestOutWorker_Worker_Success(t *testing.T) {
	writer := kmocks.NewMockWriter(t)
	worker := &OutWorker{kafkaWriter: writer}

	rollback := domain.SagaMsg{Key: "rb", Value: "r", Topic: "t"}
	rollbackRaw, _ := json.Marshal(rollback)

	task := &domain.SagaTask{
		IdempotencyKey: "idem",
		Data:           mustJSON(t, domain.SagaMsg{Key: "k", Value: "v", Topic: "t"}),
		RollbackData:   (*json.RawMessage)(&rollbackRaw),
	}

	writer.
		EXPECT().
		Write(context.Background(), &domain.SagaMsg{Key: "k", Value: "v", Topic: "t"}, &rollback, "idem").
		Return(nil)

	if err := worker.Worker(context.Background(), task, nil); err != nil {
		t.Fatalf("Worker() error = %v", err)
	}
}

func TestOutWorker_Worker_UnmarshalError(t *testing.T) {
	writer := kmocks.NewMockWriter(t)
	worker := &OutWorker{kafkaWriter: writer}

	task := &domain.SagaTask{
		Data: []byte(`{invalid`),
	}

	if err := worker.Worker(context.Background(), task, nil); err == nil {
		t.Fatalf("Worker() error = nil, want non-nil")
	}
}

func TestOutWorker_Worker_WriteError(t *testing.T) {
	writer := kmocks.NewMockWriter(t)
	worker := &OutWorker{kafkaWriter: writer}

	task := &domain.SagaTask{
		IdempotencyKey: "idem",
		Data:           mustJSON(t, domain.SagaMsg{Key: "k", Value: "v", Topic: "t"}),
	}

	writer.
		EXPECT().
		Write(context.Background(), &domain.SagaMsg{Key: "k", Value: "v", Topic: "t"}, (*domain.SagaMsg)(nil), "idem").
		Return(assertErr("write error"))

	if err := worker.Worker(context.Background(), task, nil); err == nil {
		t.Fatalf("Worker() error = nil, want non-nil")
	}
}

func TestOutWorker_DlqWorker(t *testing.T) {
	writer := kmocks.NewMockWriter(t)
	worker := &OutWorker{kafkaWriter: writer}
	task := &domain.SagaTask{
		IdempotencyKey: "idem",
		Data:           mustJSON(t, domain.SagaMsg{Key: "k", Value: "v", Topic: "t"}),
	}

	writer.
		EXPECT().
		Write(context.Background(), mock.MatchedBy(func(m *domain.SagaMsg) bool {
			return m.Key == "k" && m.Topic == "t"
		}), (*domain.SagaMsg)(nil), "idem").
		Return(nil)

	if err := worker.DlqWorker(context.Background(), task, nil); err != nil {
		t.Fatalf("DlqWorker() error = %v", err)
	}
}

// assertErr is used as a sentinel error in writer expectations.
type assertErr string

func (e assertErr) Error() string { return string(e) }
