package gosaga

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Filin153/gosaga/domain"
	"github.com/Filin153/gosaga/storage/database"
	"github.com/jackc/pgx/v5"
)

// safeRepo is a thread-safe in-memory implementation of TaskRepository used for load tests.
type safeRepo struct {
	mu          sync.Mutex
	tasks       []domain.SagaTask
	createCount int
	oneShot     bool // when true, GetByStatus returns tasks once and then empty
}

func (r *safeRepo) WithSession(_ database.Session) database.TaskRepository { return r }

func (r *safeRepo) Create(_ context.Context, task *domain.SagaTask) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.createCount++
	if task.ID == 0 {
		task.ID = int64(r.createCount)
	}
	task.UpdatedAt = time.Now()
	return task.ID, nil
}

func (r *safeRepo) GetByID(_ context.Context, id int64) (*domain.SagaTask, error) { return nil, nil }
func (r *safeRepo) GetByIdempotencyKey(_ context.Context, key string) (*domain.SagaTask, error) {
	return nil, nil
}

func (r *safeRepo) GetByStatus(_ context.Context, _ domain.TaskStatus) ([]domain.SagaTask, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.oneShot {
		if r.tasks == nil {
			return nil, nil
		}
		tasks := r.tasks
		r.tasks = nil
		return tasks, nil
	}
	return r.tasks, nil
}

func (r *safeRepo) Update(_ context.Context, _ *domain.SagaTask) error                   { return nil }
func (r *safeRepo) UpdateByID(_ context.Context, _ int64, _ domain.SagaTaskUpdate) error { return nil }
func (r *safeRepo) Delete(_ context.Context, _ int64) error                              { return nil }
func (r *safeRepo) createCalls() int                                                     { r.mu.Lock(); defer r.mu.Unlock(); return r.createCount }

// stub implementations to satisfy TaskRepository for load tests; unused here.
var _ database.TaskRepository = (*safeRepo)(nil)

// stub pool/tx for reader load.
type loadTx struct {
	pgx.Tx
}

type loadPool struct {
	tx pgx.Tx
}

func (p *loadPool) BeginTx(_ context.Context, _ pgx.TxOptions) (pgx.Tx, error) {
	return p.tx, nil
}

var _ txBeginner = (*loadPool)(nil)

func TestSagaWrite_Load5000(t *testing.T) {
	ctx := context.Background()
	repo := &safeRepo{}
	s := &Saga{outTaskRepo: repo}

	const n = 5000
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			msg := &domain.SagaMsg{Key: fmt.Sprintf("k-%d", i), Value: i, Topic: "topic"}
			if err := s.Write(ctx, msg, nil, func() {}); err != nil {
				t.Errorf("Write error: %v", err)
			}
		}()
	}
	wg.Wait()

	if repo.createCalls() != n {
		t.Fatalf("Create called %d times, want %d", repo.createCalls(), n)
	}
}

func TestDataBaseTaskReader_Load5000(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const n = 5000
	tasks := make([]domain.SagaTask, n)
	for i := 0; i < n; i++ {
		tasks[i] = domain.SagaTask{ID: int64(i + 1)}
	}

	repo := &safeRepo{tasks: tasks, oneShot: true}
	tx := &stubTx{}
	pool := &loadPool{tx: tx}
	s := &Saga{pool: pool}

	ch := s.dataBaseTaskReader(ctx, repo)

	received := 0
	for range ch {
		received++
		if received == n {
			cancel()
			break
		}
	}

	if received != n {
		t.Fatalf("received %d tasks, want %d", received, n)
	}
}
