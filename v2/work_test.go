package gosaga

import (
	"context"
	"errors"
	"testing"

	"github.com/Filin153/gosaga/domain"
	"github.com/Filin153/gosaga/storage/database"
	"github.com/stretchr/testify/require"
)

func TestOutWorkSuccess(t *testing.T) {
	taskRepo := &stubTaskRepo{}
	dlqRepo := &stubDLQRepo{}
	tx := &fakeTx{}
	s := &Saga{
		pool:           &fakePool{tx: tx},
		outTaskRepo:    taskRepo,
		dlqOutTaskRepo: dlqRepo,
	}

	task := &domain.SagaTask{ID: 1}
	err := s.outWork(context.Background(), task, func(ctx context.Context, task *domain.SagaTask, sess database.Session) error {
		return nil
	})
	require.NoError(t, err)
	require.True(t, tx.committed)

	taskRepo.mu.Lock()
	require.Len(t, taskRepo.updates, 2)
	require.True(t, taskRepo.updates[0].hasStatus)
	require.Equal(t, domain.TaskStatusWork, taskRepo.updates[0].status)
	require.Equal(t, "Start", taskRepo.updates[0].info)
	require.Equal(t, domain.TaskStatusReady, taskRepo.updates[1].status)
	require.Equal(t, "OK", taskRepo.updates[1].info)
	taskRepo.mu.Unlock()

	require.Empty(t, dlqRepo.created)
}

func TestOutWorkWorkerErrorCreatesDLQ(t *testing.T) {
	taskRepo := &stubTaskRepo{}
	dlqRepo := &stubDLQRepo{}
	tx := &fakeTx{}
	s := &Saga{
		pool:           &fakePool{tx: tx},
		outTaskRepo:    taskRepo,
		dlqOutTaskRepo: dlqRepo,
	}

	task := &domain.SagaTask{ID: 2}
	err := s.outWork(context.Background(), task, func(ctx context.Context, task *domain.SagaTask, sess database.Session) error {
		return errors.New("boom")
	})
	require.NoError(t, err)
	require.True(t, tx.committed)

	taskRepo.mu.Lock()
	require.Len(t, taskRepo.updates, 2)
	require.Equal(t, domain.TaskStatusWork, taskRepo.updates[0].status)
	require.Equal(t, domain.TaskStatusError, taskRepo.updates[1].status)
	require.Equal(t, "boom", taskRepo.updates[1].info)
	taskRepo.mu.Unlock()

	require.Len(t, dlqRepo.created, 1)
	require.Equal(t, task.ID, dlqRepo.created[0].TaskID)
}

func TestOutWorkSkipExistingDLQ(t *testing.T) {
	taskRepo := &stubTaskRepo{}
	dlqRepo := &stubDLQRepo{}
	dlqRepo.setExisting(3, &domain.DLQTask{TaskID: 3})
	tx := &fakeTx{}
	s := &Saga{
		pool:           &fakePool{tx: tx},
		outTaskRepo:    taskRepo,
		dlqOutTaskRepo: dlqRepo,
	}

	task := &domain.SagaTask{ID: 3}
	err := s.outWork(context.Background(), task, func(ctx context.Context, task *domain.SagaTask, sess database.Session) error {
		return errors.New("boom")
	})
	require.NoError(t, err)
	require.True(t, tx.committed)
	require.Empty(t, dlqRepo.created)
}

func TestOutWorkUpdateError(t *testing.T) {
	taskRepo := &stubTaskRepo{updateErrs: []error{errors.New("update fail")}}
	tx := &fakeTx{}
	s := &Saga{
		pool:           &fakePool{tx: tx},
		outTaskRepo:    taskRepo,
		dlqOutTaskRepo: &stubDLQRepo{},
	}

	task := &domain.SagaTask{ID: 4}
	err := s.outWork(context.Background(), task, func(ctx context.Context, task *domain.SagaTask, sess database.Session) error { return nil })
	require.EqualError(t, err, "update fail")
	require.False(t, tx.committed)
	require.True(t, tx.rolledBack)
}

func TestOutWorkBeginTxError(t *testing.T) {
	taskRepo := &stubTaskRepo{}
	pool := &fakePool{err: errors.New("begin fail")}
	s := &Saga{
		pool:           pool,
		outTaskRepo:    taskRepo,
		dlqOutTaskRepo: &stubDLQRepo{},
	}
	task := &domain.SagaTask{ID: 5}
	err := s.outWork(context.Background(), task, func(ctx context.Context, task *domain.SagaTask, sess database.Session) error { return nil })
	require.EqualError(t, err, "begin fail")
}

func TestInWorkErrorDoesNotCreateDLQ(t *testing.T) {
	taskRepo := &stubTaskRepo{}
	dlqRepo := &stubDLQRepo{}
	tx := &fakeTx{}
	s := &Saga{
		pool:           &fakePool{tx: tx},
		inTaskRepo:     taskRepo,
		dlqOutTaskRepo: dlqRepo,
	}

	task := &domain.SagaTask{ID: 10}
	err := s.inWork(context.Background(), task, func(ctx context.Context, task *domain.SagaTask, sess database.Session) error {
		return errors.New("oops")
	})
	require.NoError(t, err)
	require.True(t, tx.committed)
	require.Empty(t, dlqRepo.created)

	taskRepo.mu.Lock()
	require.Equal(t, domain.TaskStatusWork, taskRepo.updates[0].status)
	require.Equal(t, domain.TaskStatusError, taskRepo.updates[1].status)
	taskRepo.mu.Unlock()
}
