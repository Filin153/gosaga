package pg

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/Filin153/gosaga/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type fakeSession struct {
	execArgs  []any
	execErr   error
	queryRows pgx.Rows
	queryErr  error
	row       pgx.Row
}

func (f *fakeSession) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	f.execArgs = append([]any{sql}, arguments...)
	return pgconn.CommandTag{}, f.execErr
}

func (f *fakeSession) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if f.queryErr != nil {
		return nil, f.queryErr
	}
	return f.queryRows, nil
}

func (f *fakeSession) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return f.row
}

type rowStub struct {
	values []any
	err    error
}

func (r *rowStub) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != len(r.values) {
		return fmt.Errorf("scan len mismatch: dest %d values %d", len(dest), len(r.values))
	}
	for i, d := range dest {
		if err := assignValue(d, r.values[i]); err != nil {
			return err
		}
	}
	return nil
}

type rowsStub struct {
	rows [][]any
	idx  int
	err  error
}

func (r *rowsStub) Close()                                       {}
func (r *rowsStub) Err() error                                   { return r.err }
func (r *rowsStub) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *rowsStub) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *rowsStub) Next() bool {
	if r.err != nil {
		return false
	}
	if r.idx >= len(r.rows) {
		return false
	}
	r.idx++
	return true
}

func (r *rowsStub) Scan(dest ...any) error {
	if r.idx == 0 || r.idx > len(r.rows) {
		return fmt.Errorf("scan called without Next")
	}
	row := r.rows[r.idx-1]
	if len(dest) != len(row) {
		return fmt.Errorf("scan len mismatch: dest %d values %d", len(dest), len(row))
	}
	for i, d := range dest {
		if err := assignValue(d, row[i]); err != nil {
			return err
		}
	}
	return nil
}

func (r *rowsStub) Values() ([]any, error) { return nil, nil }
func (r *rowsStub) RawValues() [][]byte    { return nil }
func (r *rowsStub) Conn() *pgx.Conn        { return nil }

func assignValue(dest any, src any) error {
	dv := reflect.ValueOf(dest)
	if dv.Kind() != reflect.Pointer || dv.IsNil() {
		return fmt.Errorf("dest must be pointer")
	}
	ev := dv.Elem()
	sv := reflect.ValueOf(src)

	if sv.Type().AssignableTo(ev.Type()) {
		ev.Set(sv)
		return nil
	}
	if sv.Type().ConvertibleTo(ev.Type()) {
		ev.Set(sv.Convert(ev.Type()))
		return nil
	}
	return fmt.Errorf("cannot assign %T to %T", src, dest)
}

func TestTaskRepository_Create(t *testing.T) {
	now := time.Now()
	sess := &fakeSession{
		row: &rowStub{
			values: []any{int64(10), now},
		},
	}
	repo := &taskPgRepository{db: sess, table: "tbl"}

	task := &domain.SagaTask{IdempotencyKey: "k", Data: json.RawMessage(`"v"`)}
	id, err := repo.Create(context.Background(), task)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if id != 10 || task.ID != 10 || !task.UpdatedAt.Equal(now) {
		t.Fatalf("Create() unexpected task: %+v", task)
	}
}

func TestTaskRepository_GetByID(t *testing.T) {
	now := time.Now()
	rawRollback := json.RawMessage(`"rb"`)
	sess := &fakeSession{
		row: &rowStub{
			values: []any{
				int64(1),
				"key",
				json.RawMessage(`"data"`),
				sql.NullString{String: string(rawRollback), Valid: true},
				domain.TaskStatusReady,
				sql.NullString{String: "info", Valid: true},
				now,
			},
		},
	}
	repo := &taskPgRepository{db: sess, table: "tbl"}

	got, err := repo.GetByID(context.Background(), 1)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if got.Info == nil || *got.Info != "info" {
		t.Fatalf("GetByID() info = %v", got.Info)
	}
	if got.RollbackData == nil || string(*got.RollbackData) != `"rb"` {
		t.Fatalf("GetByID() rollback = %v", got.RollbackData)
	}
	if !got.UpdatedAt.Equal(now) {
		t.Fatalf("GetByID() updated_at mismatch")
	}
}

func TestTaskRepository_GetByIdempotencyKey(t *testing.T) {
	now := time.Now()
	sess := &fakeSession{
		row: &rowStub{
			values: []any{
				int64(2),
				"key2",
				json.RawMessage(`"data2"`),
				sql.NullString{Valid: false},
				domain.TaskStatusWait,
				sql.NullString{Valid: false},
				now,
			},
		},
	}
	repo := &taskPgRepository{db: sess, table: "tbl"}

	got, err := repo.GetByIdempotencyKey(context.Background(), "key2")
	if err != nil {
		t.Fatalf("GetByIdempotencyKey() error = %v", err)
	}
	if got.RollbackData != nil || got.Info != nil {
		t.Fatalf("GetByIdempotencyKey() expected nil fields")
	}
}

func TestTaskRepository_Update(t *testing.T) {
	sess := &fakeSession{}
	repo := &taskPgRepository{db: sess, table: "tbl"}

	task := &domain.SagaTask{ID: 5, IdempotencyKey: "k", Data: json.RawMessage(`"d"`), Status: domain.TaskStatusWork}
	if err := repo.Update(context.Background(), task); err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	if len(sess.execArgs) == 0 {
		t.Fatalf("Update() did not call Exec")
	}
}

func TestTaskRepository_Delete(t *testing.T) {
	sess := &fakeSession{}
	repo := &taskPgRepository{db: sess, table: "tbl"}

	if err := repo.Delete(context.Background(), 7); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if len(sess.execArgs) == 0 || sess.execArgs[1] != int64(7) {
		t.Fatalf("Delete() args = %v", sess.execArgs)
	}
}

func TestTaskRepository_GetByStatus(t *testing.T) {
	now := time.Now()
	rows := &rowsStub{
		rows: [][]any{
			{
				int64(1), "k1", json.RawMessage(`"d1"`), sql.NullString{Valid: false},
				domain.TaskStatusWait, sql.NullString{String: "info1", Valid: true}, now,
			},
			{
				int64(2), "k2", json.RawMessage(`"d2"`), sql.NullString{String: `"rb2"`, Valid: true},
				domain.TaskStatusWait, sql.NullString{Valid: false}, now,
			},
		},
	}
	sess := &fakeSession{queryRows: rows}
	repo := &taskPgRepository{db: sess, table: "tbl"}

	got, err := repo.GetByStatus(context.Background(), domain.TaskStatusWait)
	if err != nil {
		t.Fatalf("GetByStatus() error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("GetByStatus() len = %d, want 2", len(got))
	}
	if got[0].Info == nil || *got[0].Info != "info1" {
		t.Fatalf("GetByStatus() info mismatch")
	}
	if got[1].RollbackData == nil || string(*got[1].RollbackData) != `"rb2"` {
		t.Fatalf("GetByStatus() rollback mismatch")
	}
}

func TestDLQRepository_CreateAndGet(t *testing.T) {
	now := time.Now()
	sess := &fakeSession{
		row: &rowStub{values: []any{int64(3), now}},
	}
	repo := &dlqPgRepository{db: sess, dlqTable: "dlq", taskTable: "task"}

	dlq := &domain.DLQTask{TaskID: 11}
	id, err := repo.Create(context.Background(), dlq)
	if err != nil || id != 3 {
		t.Fatalf("Create() id=%d err=%v", id, err)
	}

	// Prepare row for GetByID
	sess.row = &rowStub{
		values: []any{
			int64(3), int64(11), 5, 2, 10, 1, now,
		},
	}
	got, err := repo.GetByID(context.Background(), 3)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if got.TaskID != 11 || got.TimeMul != 2 || got.HaveAttempts != 1 {
		t.Fatalf("GetByID() mismatch: %+v", got)
	}
}

func TestDLQRepository_GetByTaskID(t *testing.T) {
	now := time.Now()
	sess := &fakeSession{
		row: &rowStub{
			values: []any{int64(5), int64(9), 1, 1, 3, 0, now},
		},
	}
	repo := &dlqPgRepository{db: sess, dlqTable: "dlq", taskTable: "task"}

	got, err := repo.GetByTaskID(context.Background(), 9)
	if err != nil || got.TaskID != 9 {
		t.Fatalf("GetByTaskID() = %+v, err=%v", got, err)
	}
}

func TestDLQRepository_UpdateAndDelete(t *testing.T) {
	sess := &fakeSession{}
	repo := &dlqPgRepository{db: sess, dlqTable: "dlq", taskTable: "task"}

	task := &domain.DLQTask{ID: 4, TaskID: 8, TimeForNextTry: 5, TimeMul: 2, MaxAttempts: 5, HaveAttempts: 1}
	if err := repo.Update(context.Background(), task); err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	if len(sess.execArgs) == 0 {
		t.Fatalf("Update() did not call Exec")
	}

	if err := repo.Delete(context.Background(), 4); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if len(sess.execArgs) < 2 || sess.execArgs[len(sess.execArgs)-1] != int64(4) {
		t.Fatalf("Delete() args = %v", sess.execArgs)
	}
}

func TestDLQRepository_GetByStatus(t *testing.T) {
	now := time.Now()
	rows := &rowsStub{
		rows: [][]any{
			{
				int64(1), int64(2), 5, 1, 3, 1, now,
				int64(2), "ik", json.RawMessage(`"data"`), sql.NullString{String: `"rb"`, Valid: true},
				domain.TaskStatusWait, sql.NullString{String: "info", Valid: true}, now,
			},
		},
	}
	sess := &fakeSession{queryRows: rows}
	repo := &dlqPgRepository{db: sess, dlqTable: "dlq", taskTable: "task"}

	got, err := repo.GetByStatus(context.Background(), domain.TaskStatusWait)
	if err != nil {
		t.Fatalf("GetByStatus() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("GetByStatus() len = %d, want 1", len(got))
	}
	if got[0].Task.RollbackData == nil || string(*got[0].Task.RollbackData) != `"rb"` {
		t.Fatalf("GetByStatus() rollback mismatch")
	}
	if got[0].Task.Info == nil || *got[0].Task.Info != "info" {
		t.Fatalf("GetByStatus() info mismatch")
	}
}

func TestDLQRepository_GetErrorsWithAttempts(t *testing.T) {
	now := time.Now()
	rows := &rowsStub{
		rows: [][]any{
			{
				int64(1), int64(2), 5, 1, 1, 1, now,
				int64(2), "ik", json.RawMessage(`"data"`), sql.NullString{Valid: false},
				domain.TaskStatusError, sql.NullString{Valid: false}, now,
			},
		},
	}
	sess := &fakeSession{queryRows: rows}
	repo := &dlqPgRepository{db: sess, dlqTable: "dlq", taskTable: "task"}

	got, err := repo.GetErrorsWithAttempts(context.Background())
	if err != nil {
		t.Fatalf("GetErrorsWithAttempts() error = %v", err)
	}
	if len(got) != 1 || got[0].DLQ.TaskID != 2 {
		t.Fatalf("GetErrorsWithAttempts() unexpected result: %+v", got)
	}
}
