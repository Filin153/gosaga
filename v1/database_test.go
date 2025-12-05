package gosaga

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Filin153/gosaga/domain"
	"github.com/stretchr/testify/require"
)

func TestDataBaseTaskReaderDeliversAndStops(t *testing.T) {
	oldSleep := sleep
	sleep = func(time.Duration) {}
	defer func() { sleep = oldSleep }()

	tx := &fakeTx{}
	pool := &fakePool{tx: tx}
	repo := &stubTaskRepo{
		getByStatusResp: []domain.SagaTask{{ID: 1}, {ID: 2}},
	}
	s := &Saga{pool: pool}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := s.dataBaseTaskReader(ctx, repo)
	var ids []int64
	for task := range ch {
		ids = append(ids, task.ID)
		if len(ids) == 2 {
			cancel()
		}
	}

	require.ElementsMatch(t, []int64{1, 2}, ids)
	require.True(t, tx.committed)
}

func TestDataBaseTaskReaderRetryOnError(t *testing.T) {
	oldSleep := sleep
	var sleeps atomic.Int32
	sleep = func(time.Duration) { sleeps.Add(1) }
	defer func() { sleep = oldSleep }()

	tx := &fakeTx{}
	pool := &fakePool{tx: tx}
	repo := &stubTaskRepo{
		getByStatusErrs: []error{errors.New("first fail")},
		getByStatusResp: []domain.SagaTask{{ID: 7}},
	}
	s := &Saga{pool: pool}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := s.dataBaseTaskReader(ctx, repo)
	select {
	case task := <-ch:
		require.Equal(t, int64(7), task.ID)
		cancel()
	case <-time.After(2 * time.Second):
		t.Fatalf("no task received")
	}
	require.GreaterOrEqual(t, sleeps.Load(), int32(1))
}

func TestDataBaseDLQTaskReader(t *testing.T) {
	oldSleep := sleep
	sleep = func(time.Duration) {}
	defer func() { sleep = oldSleep }()

	tx := &fakeTx{}
	pool := &fakePool{tx: tx}
	repo := &stubDLQRepo{
		getByStatusResp: []domain.DLQEntry{
			{Task: domain.SagaTask{ID: 11}},
			{Task: domain.SagaTask{ID: 12}},
		},
	}
	s := &Saga{pool: pool}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := s.dataBaseDLQTaskReader(ctx, repo)
	var ids []int64
	for task := range ch {
		ids = append(ids, task.ID)
		if len(ids) == 2 {
			cancel()
		}
	}

	require.ElementsMatch(t, []int64{11, 12}, ids)
}
