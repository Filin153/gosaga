package pg

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/Filin153/gosaga/domain"
	"github.com/Filin153/gosaga/storage/database"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type InTaskRepository struct {
	ctx context.Context
	db  session
}

func NewInTaskRepository(ctx context.Context, pool *pgxpool.Pool) *InTaskRepository {
	return &InTaskRepository{ctx: ctx, db: pool}
}

func (r *InTaskRepository) WithSession(sess session) database.TaskRepository {
	return &InTaskRepository{
		ctx: r.ctx,
		db:  sess,
	}
}

func (r *InTaskRepository) Create(task *domain.SagaTask) (int64, error) {
	query := `
		INSERT INTO "go_saga_in_task" ("idempotency_key", "data", "rollback_data")
		VALUES ($1, $2, $3)
		RETURNING "id", "updated_at";
	`

	err := r.db.QueryRow(r.ctx, query,
		task.IdempotencyKey,
		task.Data,
		task.RollbackData,
	).Scan(&task.ID, &task.UpdatedAt)
	if err != nil {
		return 0, err
	}

	return task.ID, nil
}

func (r *InTaskRepository) GetByID(id int64) (*domain.SagaTask, error) {
	query := `
		SELECT "id", "idempotency_key", "data", "rollback_data", "status", "info", "updated_at"
		FROM "go_saga_in_task"
		WHERE "id" = $1;
	`

	var (
		task     domain.SagaTask
		info     sql.NullString
		rollback sql.NullString
	)

	err := r.db.QueryRow(r.ctx, query, id).Scan(
		&task.ID,
		&task.IdempotencyKey,
		&task.Data,
		&rollback,
		&task.Status,
		&info,
		&task.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	if info.Valid {
		task.Info = &info.String
	}
	if rollback.Valid {
		raw := json.RawMessage(rollback.String)
		task.RollbackData = &raw
	}

	return &task, nil
}

func (r *InTaskRepository) GetByIdempotencyKey(key string) (*domain.SagaTask, error) {
	query := `
		SELECT "id", "idempotency_key", "data", "rollback_data", "status", "info", "updated_at"
		FROM "go_saga_in_task"
		WHERE "idempotency_key" = $1;
	`

	var (
		task     domain.SagaTask
		info     sql.NullString
		rollback sql.NullString
	)

	err := r.db.QueryRow(r.ctx, query, key).Scan(
		&task.ID,
		&task.IdempotencyKey,
		&task.Data,
		&rollback,
		&task.Status,
		&info,
		&task.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	if info.Valid {
		task.Info = &info.String
	}
	if rollback.Valid {
		raw := json.RawMessage(rollback.String)
		task.RollbackData = &raw
	}

	return &task, nil
}

func (r *InTaskRepository) Update(task *domain.SagaTask) error {
	return r.UpdateByID(task.ID, domain.SagaTaskUpdate{
		IdempotencyKey: &task.IdempotencyKey,
		Data:           &task.Data,
		Status:         &task.Status,
		Info:           task.Info,
	})
}

func (r *InTaskRepository) Delete(id int64) error {
	query := `DELETE FROM "go_saga_in_task" WHERE "id" = $1;`

	_, err := r.db.Exec(r.ctx, query, id)
	return err
}

func (r *InTaskRepository) UpdateByID(id int64, update domain.SagaTaskUpdate) error {
	query, args, err := database.GenerateUpdateQueryById("go_saga_in_task", id, update)
	if err != nil {
		return err
	}

	_, err = r.db.Exec(r.ctx, query, args...)
	return err
}

func (r *InTaskRepository) GetByStatus(status domain.TaskStatus) ([]domain.SagaTask, error) {
	query := `
		SELECT "id", "idempotency_key", "data", "rollback_data", "status", "info", "updated_at"
		FROM "go_saga_in_task"
		WHERE "status" = $1;
	`

	rows, err := r.db.Query(r.ctx, query, status)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (domain.SagaTask, error) {
		var (
			task     domain.SagaTask
			info     sql.NullString
			rollback sql.NullString
		)
		if err := row.Scan(
			&task.ID,
			&task.IdempotencyKey,
			&task.Data,
			&rollback,
			&task.Status,
			&info,
			&task.UpdatedAt,
		); err != nil {
			return domain.SagaTask{}, err
		}
		if info.Valid {
			task.Info = &info.String
		}
		if rollback.Valid {
			raw := json.RawMessage(rollback.String)
			task.RollbackData = &raw
		}
		return task, nil
	})

	return result, err
}
