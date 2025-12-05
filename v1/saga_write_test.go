package gosaga

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Filin153/gosaga/domain"
	"github.com/stretchr/testify/require"
)

func TestWriteSuccess(t *testing.T) {
	repo := &stubTaskRepo{}
	s := &Saga{outTaskRepo: repo}
	rollbackCalled := atomic.Int32{}

	msg := &domain.SagaMsg{Key: "k", Value: map[string]any{"a": 1}, Topic: "topic"}
	err := s.Write(context.Background(), msg, nil, func() { rollbackCalled.Add(1) })
	require.NoError(t, err)
	require.Zero(t, rollbackCalled.Load())

	repo.mu.Lock()
	require.Len(t, repo.created, 1)
	require.NotNil(t, repo.created[0].Data)
	require.NotEmpty(t, repo.created[0].IdempotencyKey)
	require.Nil(t, repo.created[0].RollbackData)
	repo.mu.Unlock()
}

func TestWriteCreateErrorTriggersRollback(t *testing.T) {
	repo := &stubTaskRepo{createErr: errors.New("create fail")}
	s := &Saga{outTaskRepo: repo}
	rollbackCalled := atomic.Int32{}

	err := s.Write(context.Background(), &domain.SagaMsg{}, nil, func() { rollbackCalled.Add(1) })
	require.EqualError(t, err, "create fail")
	require.Equal(t, int32(1), rollbackCalled.Load())
}

func TestWriteMarshalError(t *testing.T) {
	repo := &stubTaskRepo{}
	s := &Saga{outTaskRepo: repo}
	rollbackCalled := atomic.Int32{}

	err := s.Write(context.Background(), &domain.SagaMsg{Value: make(chan int)}, nil, func() { rollbackCalled.Add(1) })
	require.Error(t, err)
	require.Equal(t, int32(1), rollbackCalled.Load())
}

func TestWriteRandError(t *testing.T) {
	oldRand := randReader
	randReader = func(b []byte) (int, error) { return 0, errors.New("rand fail") }
	defer func() { randReader = oldRand }()

	repo := &stubTaskRepo{}
	s := &Saga{outTaskRepo: repo}
	rollbackCalled := atomic.Int32{}

	err := s.Write(context.Background(), &domain.SagaMsg{}, nil, func() { rollbackCalled.Add(1) })
	require.EqualError(t, err, "rand fail")
	require.Equal(t, int32(1), rollbackCalled.Load())
}

func TestAsyncWriteSuccess(t *testing.T) {
	repo := &stubTaskRepo{}
	wg := sync.WaitGroup{}
	wg.Add(1)
	repo.wg = &wg

	s := &Saga{outTaskRepo: repo}
	rollbackCalled := atomic.Int32{}

	err := s.AsyncWrite(context.Background(), &domain.SagaMsg{Key: "k", Value: map[string]int{"a": 1}, Topic: "topic"}, nil, func() { rollbackCalled.Add(1) })
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("async create did not finish")
	}

	require.Zero(t, rollbackCalled.Load())
	repo.mu.Lock()
	require.Len(t, repo.created, 1)
	repo.mu.Unlock()
}

func TestAsyncWriteCreateErrorTriggersRollback(t *testing.T) {
	wg := sync.WaitGroup{}
	wg.Add(1)
	repo := &stubTaskRepo{createErr: errors.New("create boom"), wg: &wg}
	s := &Saga{outTaskRepo: repo}
	rollbackCalled := atomic.Int32{}

	err := s.AsyncWrite(context.Background(), &domain.SagaMsg{}, nil, func() { rollbackCalled.Add(1) })
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("async create did not finish")
	}

	require.Eventually(t, func() bool { return rollbackCalled.Load() == 1 }, time.Second, 10*time.Millisecond)
}

func TestAsyncWriteEarlyError(t *testing.T) {
	oldRand := randReader
	randReader = func(b []byte) (int, error) { return 0, errors.New("rand fail") }
	defer func() { randReader = oldRand }()

	s := &Saga{outTaskRepo: &stubTaskRepo{}}
	rollbackCalled := atomic.Int32{}

	err := s.AsyncWrite(context.Background(), &domain.SagaMsg{}, nil, func() { rollbackCalled.Add(1) })
	require.EqualError(t, err, "rand fail")
	require.Eventually(t, func() bool { return rollbackCalled.Load() == 1 }, time.Second, 10*time.Millisecond)
}

func TestAsyncWriteRace5000(t *testing.T) {
	const count = 5000
	wg := sync.WaitGroup{}
	wg.Add(count)

	repo := &stubTaskRepo{wg: &wg}
	s := &Saga{outTaskRepo: repo}

	for i := 0; i < count; i++ {
		err := s.AsyncWrite(context.Background(), &domain.SagaMsg{Key: "k", Value: map[string]int{"a": i}, Topic: "topic"}, &domain.SagaMsg{Key: "rb"}, func() {})
		require.NoError(t, err)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for async writes")
	}

	repo.mu.Lock()
	require.Len(t, repo.created, count)
	repo.mu.Unlock()
}
