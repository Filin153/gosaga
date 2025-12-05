package gosaga

import (
	"context"
	"sync"

	"github.com/Filin153/gosaga/domain"
	"github.com/Filin153/gosaga/storage/database"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type fakeTx struct {
	commitErr   error
	rollbackErr error
	committed   bool
	rolledBack  bool
	userToken   any
}

func (f *fakeTx) Begin(ctx context.Context) (pgx.Tx, error) { return f, nil }
func (f *fakeTx) Commit(context.Context) error {
	f.committed = true
	return f.commitErr
}
func (f *fakeTx) Rollback(context.Context) error {
	f.rolledBack = true
	return f.rollbackErr
}
func (f *fakeTx) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, nil
}
func (f *fakeTx) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults { return nil }
func (f *fakeTx) LargeObjects() pgx.LargeObjects {
	var lo pgx.LargeObjects
	return lo
}
func (f *fakeTx) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	return &pgconn.StatementDescription{}, nil
}
func (f *fakeTx) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}
func (f *fakeTx) Query(context.Context, string, ...any) (pgx.Rows, error) { return nil, nil }
func (f *fakeTx) QueryRow(context.Context, string, ...any) pgx.Row        { return fakeRow{} }
func (f *fakeTx) Conn() *pgx.Conn                                         { return nil }
func (f *fakeTx) UserToken() any                                          { return f.userToken }
func (f *fakeTx) SetUserToken(token any)                                  { f.userToken = token }

type fakeRow struct{}

func (fakeRow) Scan(dest ...any) error { return nil }

type fakePool struct {
	tx      pgx.Tx
	err     error
	callsMu sync.Mutex
	calls   int
}

func (f *fakePool) BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
	f.callsMu.Lock()
	f.calls++
	f.callsMu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	return f.tx, nil
}

type updateCall struct {
	id        int64
	status    domain.TaskStatus
	info      string
	hasStatus bool
	hasInfo   bool
}

type stubTaskRepo struct {
	mu               sync.Mutex
	updates          []updateCall
	updateErrs       []error
	createErr        error
	getByStatusErrs  []error
	getByStatusResp  []domain.SagaTask
	created          []*domain.SagaTask
	getByIDResp      *domain.SagaTask
	getByKeyResp     *domain.SagaTask
	withSessionCalls int
	wg               *sync.WaitGroup
}

func (s *stubTaskRepo) nextUpdateErr() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.updateErrs) == 0 {
		return nil
	}
	err := s.updateErrs[0]
	s.updateErrs = s.updateErrs[1:]
	return err
}

func (s *stubTaskRepo) WithSession(database.Session) database.TaskRepository {
	s.mu.Lock()
	s.withSessionCalls++
	s.mu.Unlock()
	return s
}

func (s *stubTaskRepo) Create(ctx context.Context, task *domain.SagaTask) (int64, error) {
	if s.wg != nil {
		defer s.wg.Done()
	}
	if s.createErr != nil {
		return 0, s.createErr
	}
	s.mu.Lock()
	s.created = append(s.created, task)
	id := int64(len(s.created))
	task.ID = id
	s.mu.Unlock()
	return id, nil
}

func (s *stubTaskRepo) GetByID(context.Context, int64) (*domain.SagaTask, error) {
	return s.getByIDResp, nil
}

func (s *stubTaskRepo) GetByIdempotencyKey(context.Context, string) (*domain.SagaTask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getByKeyResp, nil
}

func (s *stubTaskRepo) GetByStatus(context.Context, domain.TaskStatus) ([]domain.SagaTask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.getByStatusErrs) > 0 {
		err := s.getByStatusErrs[0]
		s.getByStatusErrs = s.getByStatusErrs[1:]
		return nil, err
	}
	return s.getByStatusResp, nil
}

func (s *stubTaskRepo) Update(context.Context, *domain.SagaTask) error { return nil }

func (s *stubTaskRepo) UpdateByID(ctx context.Context, id int64, update domain.SagaTaskUpdate) error {
	if err := s.nextUpdateErr(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	call := updateCall{id: id}
	if update.Status != nil {
		call.hasStatus = true
		call.status = *update.Status
	}
	if update.Info != nil {
		call.hasInfo = true
		call.info = *update.Info
	}
	s.updates = append(s.updates, call)
	return nil
}

func (s *stubTaskRepo) Delete(context.Context, int64) error { return nil }

type stubDLQRepo struct {
	mu               sync.Mutex
	existing         map[int64]*domain.DLQTask
	created          []*domain.DLQTask
	createErr        error
	getByStatusResp  []domain.DLQEntry
	getByStatusErrs  []error
	withSessionCalls int
}

func (s *stubDLQRepo) WithSession(database.Session) database.DLQRepository {
	s.mu.Lock()
	s.withSessionCalls++
	s.mu.Unlock()
	return s
}

func (s *stubDLQRepo) Create(ctx context.Context, task *domain.DLQTask) (int64, error) {
	if s.createErr != nil {
		return 0, s.createErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.created = append(s.created, task)
	task.ID = int64(len(s.created))
	return task.ID, nil
}

func (s *stubDLQRepo) GetByID(context.Context, int64) (*domain.DLQTask, error) { return nil, nil }

func (s *stubDLQRepo) GetByTaskID(_ context.Context, taskID int64) (*domain.DLQTask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.existing == nil {
		return nil, nil
	}
	return s.existing[taskID], nil
}

func (s *stubDLQRepo) GetByStatus(context.Context, domain.TaskStatus) ([]domain.DLQEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.getByStatusErrs) > 0 {
		err := s.getByStatusErrs[0]
		s.getByStatusErrs = s.getByStatusErrs[1:]
		return nil, err
	}
	return s.getByStatusResp, nil
}

func (s *stubDLQRepo) GetErrorsWithAttempts(context.Context) ([]domain.DLQEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getByStatusResp, nil
}

func (s *stubDLQRepo) Update(context.Context, *domain.DLQTask) error                 { return nil }
func (s *stubDLQRepo) UpdateByID(context.Context, int64, domain.DLQTaskUpdate) error { return nil }
func (s *stubDLQRepo) Delete(context.Context, int64) error                           { return nil }
func (s *stubDLQRepo) setExisting(taskID int64, dlq *domain.DLQTask) { // helper
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.existing == nil {
		s.existing = make(map[int64]*domain.DLQTask)
	}
	s.existing[taskID] = dlq
}

type stubWorker struct {
	err      error
	dlqErr   error
	calls    int32
	dlqCalls int32
}

func (w *stubWorker) New(ctx context.Context) (WorkerInterface, error) { return w, nil }
func (w *stubWorker) Worker(ctx context.Context, task *domain.SagaTask, sess database.Session) error {
	return w.err
}
func (w *stubWorker) DlqWorker(ctx context.Context, task *domain.SagaTask, sess database.Session) error {
	return w.dlqErr
}
