package gosaga

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Filin153/gosaga/domain"
	dbmocks "github.com/Filin153/gosaga/mocks/database"
	"github.com/stretchr/testify/mock"
)

func TestSagaWrite_Success(t *testing.T) {
	ctx := context.Background()
	repo := dbmocks.NewMockTaskRepository(t)

	s := &Saga[struct{}]{
		outTaskRepo: repo,
	}

	msg := &domain.SagaMsg{
		Key:   "key",
		Value: map[string]string{"foo": "bar"},
		Topic: "topic",
	}

	repo.EXPECT().
		Create(ctx, mock.MatchedBy(func(task *domain.SagaTask) bool {
			if task.IdempotencyKey == "" {
				return false
			}
			var got domain.SagaMsg
			if err := json.Unmarshal(task.Data, &got); err != nil {
				return false
			}
			return got.Key == msg.Key && got.Topic == msg.Topic
		})).
		Return(int64(1), nil)

	rollbackCalled := false
	err := s.Write(ctx, msg, nil, func() { rollbackCalled = true })
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if rollbackCalled {
		t.Fatalf("Write() called rollbackFunc on success")
	}
}

func TestSagaWrite_CreateErrorTriggersRollback(t *testing.T) {
	ctx := context.Background()
	repo := dbmocks.NewMockTaskRepository(t)

	s := &Saga[struct{}]{
		outTaskRepo: repo,
	}

	msg := &domain.SagaMsg{
		Key:   "key",
		Value: "value",
		Topic: "topic",
	}

	repo.EXPECT().
		Create(ctx, mock.AnythingOfType("*domain.SagaTask")).
		Return(int64(0), errors.New("create error"))

	var mu sync.Mutex
	rollbackCalled := false
	err := s.Write(ctx, msg, nil, func() {
		mu.Lock()
		defer mu.Unlock()
		rollbackCalled = true
	})
	if err == nil {
		t.Fatalf("Write() error = nil, want non-nil")
	}

	mu.Lock()
	defer mu.Unlock()
	if !rollbackCalled {
		t.Fatalf("Write() did not call rollbackFunc when Create failed")
	}
}

func TestSagaAsyncWrite_Success(t *testing.T) {
	ctx := context.Background()
	repo := dbmocks.NewMockTaskRepository(t)

	s := &Saga[struct{}]{
		outTaskRepo: repo,
	}

	msg := &domain.SagaMsg{
		Key:   "key",
		Value: "value",
		Topic: "topic",
	}

	done := make(chan struct{})
	repo.EXPECT().
		Create(ctx, mock.AnythingOfType("*domain.SagaTask")).
		Run(func(_ context.Context, _ *domain.SagaTask) {
			close(done)
		}).
		Return(int64(1), nil)

	rollbackCalled := false
	err := s.AsyncWrite(ctx, msg, nil, func() { rollbackCalled = true })
	if err != nil {
		t.Fatalf("AsyncWrite() error = %v", err)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("AsyncWrite() did not call Create in time")
	}

	if rollbackCalled {
		t.Fatalf("AsyncWrite() called rollbackFunc on success")
	}
}

func TestSagaAsyncWrite_CreateErrorTriggersRollback(t *testing.T) {
	ctx := context.Background()
	repo := dbmocks.NewMockTaskRepository(t)

	s := &Saga[struct{}]{
		outTaskRepo: repo,
	}

	msg := &domain.SagaMsg{
		Key:   "key",
		Value: "value",
		Topic: "topic",
	}

	repo.EXPECT().
		Create(ctx, mock.AnythingOfType("*domain.SagaTask")).
		Return(int64(0), errors.New("create error"))

	ch := make(chan struct{})
	err := s.AsyncWrite(ctx, msg, nil, func() { close(ch) })
	if err != nil {
		t.Fatalf("AsyncWrite() error = %v", err)
	}

	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("AsyncWrite() did not call rollbackFunc after Create error")
	}
}
