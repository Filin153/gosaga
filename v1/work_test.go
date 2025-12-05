package gosaga

import (
	"context"
	"errors"
	"testing"

	"github.com/Filin153/gosaga/domain"
	dbmocks "github.com/Filin153/gosaga/mocks/database"
	"github.com/Filin153/gosaga/storage/database"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/mock"
	"sync"
)

type stubBatchResults struct{}

func (stubBatchResults) Close() error                     { return nil }
func (stubBatchResults) Exec() (pgconn.CommandTag, error) { return pgconn.CommandTag{}, nil }
func (stubBatchResults) Query() (pgx.Rows, error)         { return nil, nil }
func (stubBatchResults) QueryRow() pgx.Row                { return nil }

type stubTx struct {
	mu            sync.Mutex
	commitCount   int
	rollbackCount int
	execArgs      []any
	rows          pgx.Rows
	row           pgx.Row
}

func (s *stubTx) Begin(ctx context.Context) (pgx.Tx, error) { return s, nil }
func (s *stubTx) Commit(ctx context.Context) error {
	s.mu.Lock()
	s.commitCount++
	s.mu.Unlock()
	return nil
}
func (s *stubTx) Rollback(ctx context.Context) error {
	s.mu.Lock()
	s.rollbackCount++
	s.mu.Unlock()
	return nil
}
func (s *stubTx) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	return 0, nil
}
func (s *stubTx) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	return stubBatchResults{}
}
func (s *stubTx) LargeObjects() pgx.LargeObjects { return pgx.LargeObjects{} }
func (s *stubTx) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
	return nil, nil
}
func (s *stubTx) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	s.mu.Lock()
	s.execArgs = append(s.execArgs, append([]any{sql}, arguments...)...)
	s.mu.Unlock()
	return pgconn.CommandTag{}, nil
}
func (s *stubTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return s.rows, nil
}
func (s *stubTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row { return s.row }
func (s *stubTx) Conn() *pgx.Conn                                               { return nil }
func (s *stubTx) commits() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.commitCount
}
func (s *stubTx) rollbacks() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rollbackCount
}

type stubPool struct {
	tx       *stubTx
	beginErr error
}

func (p *stubPool) BeginTx(ctx context.Context, _ pgx.TxOptions) (pgx.Tx, error) {
	if p.beginErr != nil {
		return nil, p.beginErr
	}
	return p.tx, nil
}

func TestInWork_Success(t *testing.T) {
	ctx := context.Background()
	tx := &stubTx{}
	pool := &stubPool{tx: tx}

	taskRepo := dbmocks.NewMockTaskRepository(t)
	dlqRepo := dbmocks.NewMockDLQRepository(t)

	s := &Saga{
		pool:          pool,
		inTaskRepo:    taskRepo,
		dlqInTaskRepo: dlqRepo,
	}

	taskRepo.EXPECT().WithSession(tx).Return(taskRepo)
	dlqRepo.EXPECT().WithSession(tx).Return(dlqRepo)

	task := &domain.SagaTask{ID: 1}

	taskRepo.EXPECT().
		UpdateByID(ctx, int64(1), mock.MatchedBy(func(u domain.SagaTaskUpdate) bool {
			return u.Status != nil && *u.Status == domain.TaskStatusWork && u.Info != nil && *u.Info == "Start"
		})).
		Return(nil)

	taskRepo.EXPECT().
		UpdateByID(ctx, int64(1), mock.MatchedBy(func(u domain.SagaTaskUpdate) bool {
			return u.Status != nil && *u.Status == domain.TaskStatusReady && u.Info != nil && *u.Info == "OK"
		})).
		Return(nil)

	err := s.inWork(ctx, task, func(ctx context.Context, task *domain.SagaTask, sess database.Session) error {
		if sess != tx {
			t.Fatalf("expected tx passed to worker")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("inWork() error = %v", err)
	}
	if tx.commits() != 1 {
		t.Fatalf("expected commit once, got %d", tx.commits())
	}
}

func TestInWork_ErrorCreatesDLQ(t *testing.T) {
	ctx := context.Background()
	tx := &stubTx{}
	pool := &stubPool{tx: tx}

	taskRepo := dbmocks.NewMockTaskRepository(t)
	dlqRepo := dbmocks.NewMockDLQRepository(t)

	s := &Saga{
		pool:          pool,
		inTaskRepo:    taskRepo,
		dlqInTaskRepo: dlqRepo,
	}

	task := &domain.SagaTask{ID: 5}

	taskRepo.EXPECT().WithSession(tx).Return(taskRepo)
	dlqRepo.EXPECT().WithSession(tx).Return(dlqRepo)

	taskRepo.EXPECT().
		UpdateByID(ctx, int64(5), mock.MatchedBy(func(u domain.SagaTaskUpdate) bool {
			return u.Status != nil && *u.Status == domain.TaskStatusWork
		})).
		Return(nil).Once()
	taskRepo.EXPECT().
		UpdateByID(ctx, int64(5), mock.MatchedBy(func(u domain.SagaTaskUpdate) bool {
			return u.Status != nil && *u.Status == domain.TaskStatusError
		})).
		Return(nil).Once()

	dlqRepo.EXPECT().GetByTaskID(ctx, int64(5)).Return(nil, nil)
	dlqRepo.EXPECT().Create(ctx, mock.MatchedBy(func(d *domain.DLQTask) bool {
		return d.TaskID == 5
	})).Return(int64(10), nil)

	errSentinel := errors.New("boom")
	err := s.inWork(ctx, task, func(ctx context.Context, task *domain.SagaTask, sess database.Session) error {
		return errSentinel
	})
	if err != nil {
		t.Fatalf("inWork() error = %v, want nil (current implementation overwrites worker error)", err)
	}
	if tx.commits() != 1 {
		t.Fatalf("expected commit once on error path, got %d", tx.commits())
	}
}

func TestOutWork_Success(t *testing.T) {
	ctx := context.Background()
	tx := &stubTx{}
	pool := &stubPool{tx: tx}

	taskRepo := dbmocks.NewMockTaskRepository(t)
	dlqRepo := dbmocks.NewMockDLQRepository(t)

	s := &Saga{
		pool:           pool,
		outTaskRepo:    taskRepo,
		dlqOutTaskRepo: dlqRepo,
	}

	task := &domain.SagaTask{ID: 2}

	taskRepo.EXPECT().WithSession(tx).Return(taskRepo)
	dlqRepo.EXPECT().WithSession(tx).Return(dlqRepo)

	taskRepo.EXPECT().UpdateByID(ctx, int64(2), mock.Anything).Return(nil)
	taskRepo.EXPECT().
		UpdateByID(ctx, int64(2), mock.MatchedBy(func(u domain.SagaTaskUpdate) bool {
			return u.Status != nil && *u.Status == domain.TaskStatusReady
		})).
		Return(nil)

	err := s.outWork(ctx, task, func(ctx context.Context, task *domain.SagaTask, sess database.Session) error {
		return nil
	})
	if err != nil {
		t.Fatalf("outWork() error = %v", err)
	}
	if tx.commits() != 1 {
		t.Fatalf("expected commit once, got %d", tx.commits())
	}
}

func TestOutWork_ErrorCreatesDLQ(t *testing.T) {
	ctx := context.Background()
	tx := &stubTx{}
	pool := &stubPool{tx: tx}

	taskRepo := dbmocks.NewMockTaskRepository(t)
	dlqRepo := dbmocks.NewMockDLQRepository(t)

	s := &Saga{
		pool:           pool,
		outTaskRepo:    taskRepo,
		dlqOutTaskRepo: dlqRepo,
	}

	task := &domain.SagaTask{ID: 7}

	taskRepo.EXPECT().WithSession(tx).Return(taskRepo)
	dlqRepo.EXPECT().WithSession(tx).Return(dlqRepo)

	taskRepo.EXPECT().
		UpdateByID(ctx, int64(7), mock.MatchedBy(func(u domain.SagaTaskUpdate) bool {
			return u.Status != nil && *u.Status == domain.TaskStatusWork
		})).
		Return(nil).Once()
	taskRepo.EXPECT().
		UpdateByID(ctx, int64(7), mock.MatchedBy(func(u domain.SagaTaskUpdate) bool {
			return u.Status != nil && *u.Status == domain.TaskStatusError
		})).
		Return(nil).Once()

	dlqRepo.EXPECT().GetByTaskID(ctx, int64(7)).Return(nil, nil)
	dlqRepo.EXPECT().Create(ctx, mock.Anything).Return(int64(1), nil)

	err := s.outWork(ctx, task, func(ctx context.Context, task *domain.SagaTask, sess database.Session) error {
		return errors.New("err")
	})
	if err != nil {
		t.Fatalf("outWork() error = %v, want nil (current implementation overwrites worker error)", err)
	}
	if tx.commits() != 1 {
		t.Fatalf("expected commit once on error path, got %d", tx.commits())
	}
}

func TestDataBaseTaskReader_SendsTasks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	tx := &stubTx{}
	pool := &stubPool{tx: tx}
	repo := dbmocks.NewMockTaskRepository(t)

	s := &Saga{pool: pool}

	repo.EXPECT().WithSession(tx).Return(repo).Maybe()
	repo.EXPECT().GetByStatus(ctx, domain.TaskStatusWait).Return([]domain.SagaTask{{ID: 1}}, nil).Once()
	repo.EXPECT().GetByStatus(ctx, domain.TaskStatusWait).Return(nil, nil).Maybe()

	ch := s.dataBaseTaskReader(ctx, repo)

	received := 0
	for task := range ch {
		received++
		if task.ID != 1 {
			t.Fatalf("expected task id 1, got %d", task.ID)
		}
		cancel() // stop reader goroutine after first message
	}

	if received < 1 {
		t.Fatalf("received %d tasks, want >=1", received)
	}
}

func TestDataBaseDLQTaskReader_SendsTasks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	tx := &stubTx{}
	pool := &stubPool{tx: tx}
	repo := dbmocks.NewMockDLQRepository(t)

	s := &Saga{pool: pool}

	repo.EXPECT().WithSession(tx).Return(repo).Maybe()
	repo.EXPECT().
		GetByStatus(ctx, domain.TaskStatusWait).
		Return([]domain.DLQEntry{{Task: domain.SagaTask{ID: 3}}}, nil).Once()
	repo.EXPECT().
		GetByStatus(ctx, domain.TaskStatusWait).
		Return([]domain.DLQEntry{}, nil).Maybe()

	ch := s.dataBaseDLQTaskReader(ctx, repo)

	received := 0
	for task := range ch {
		received++
		if task.ID != 3 {
			t.Fatalf("expected task id 3, got %d", task.ID)
		}
		cancel()
	}

	if received < 1 {
		t.Fatalf("received %d tasks, want >=1", received)
	}
}
