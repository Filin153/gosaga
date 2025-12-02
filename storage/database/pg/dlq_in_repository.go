package pg

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"

	"github.com/Filin153/gosaga/domain"
	"github.com/Filin153/gosaga/storage/database"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DLQInTaskRepository struct {
	ctx context.Context
	db  session
}

func NewDLQInTaskRepository(ctx context.Context, pool *pgxpool.Pool) *DLQInTaskRepository {
	slog.Info("DLQInTaskRepository.New", "ctx_set", ctx != nil)
	return &DLQInTaskRepository{ctx: ctx, db: pool}
}

func (r *DLQInTaskRepository) WithSession(sess session) database.DLQRepository {
	slog.Info("DLQInTaskRepository.WithSession")
	return &DLQInTaskRepository{
		ctx: r.ctx,
		db:  sess,
	}
}

func (r *DLQInTaskRepository) Create(task *domain.DLQTask) (int64, error) {
	slog.Info("DLQInTaskRepository.Create: start", "task_id", task.TaskID)
	query := `
		INSERT INTO "go_saga_dlq_in_task" ("task_id")
		VALUES ($1)
		RETURNING "id", "updated_at";
	`

	err := r.db.QueryRow(r.ctx, query,
		task.TaskID,
	).Scan(&task.ID, &task.UpdatedAt)
	if err != nil {
		slog.Error("DLQInTaskRepository.Create: query error", "error", err.Error(), "task_id", task.TaskID)
		return 0, err
	}

	slog.Info("DLQInTaskRepository.Create: success", "id", task.ID, "task_id", task.TaskID)
	return task.ID, nil
}

func (r *DLQInTaskRepository) GetByID(id int64) (*domain.DLQTask, error) {
	slog.Info("DLQInTaskRepository.GetByID: start", "id", id)
	query := `
		SELECT "id", "task_id", "time_for_next_try", "time_mul", "max_attempts", "have_attempts", "updated_at"
		FROM "go_saga_dlq_in_task"
		WHERE "id" = $1;
	`

	var task domain.DLQTask

	err := r.db.QueryRow(r.ctx, query, id).Scan(
		&task.ID,
		&task.TaskID,
		&task.TimeForNextTry,
		&task.TimeMul,
		&task.MaxAttempts,
		&task.HaveAttempts,
		&task.UpdatedAt,
	)
	if err != nil {
		slog.Error("DLQInTaskRepository.GetByID: query error", "error", err.Error(), "id", id)
		return nil, err
	}

	slog.Info("DLQInTaskRepository.GetByID: success", "id", task.ID, "task_id", task.TaskID)
	return &task, nil
}

func (r *DLQInTaskRepository) GetByTaskID(taskID int64) (*domain.DLQTask, error) {
	slog.Info("DLQInTaskRepository.GetByTaskID: start", "task_id", taskID)
	query := `
		SELECT "id", "task_id", "time_for_next_try", "time_mul", "max_attempts", "have_attempts", "updated_at"
		FROM "go_saga_dlq_in_task"
		WHERE "task_id" = $1;
	`

	var task domain.DLQTask

	err := r.db.QueryRow(r.ctx, query, taskID).Scan(
		&task.ID,
		&task.TaskID,
		&task.TimeForNextTry,
		&task.TimeMul,
		&task.MaxAttempts,
		&task.HaveAttempts,
		&task.UpdatedAt,
	)
	if err != nil {
		slog.Error("DLQInTaskRepository.GetByTaskID: query error", "error", err.Error(), "task_id", taskID)
		return nil, err
	}

	slog.Info("DLQInTaskRepository.GetByTaskID: success", "id", task.ID, "task_id", task.TaskID)
	return &task, nil
}

func (r *DLQInTaskRepository) Update(task *domain.DLQTask) error {
	slog.Info("DLQInTaskRepository.Update: start", "id", task.ID, "task_id", task.TaskID)
	return r.UpdateByID(task.ID, domain.DLQTaskUpdate{
		TaskID:         &task.TaskID,
		TimeForNextTry: &task.TimeForNextTry,
		TimeMul:        &task.TimeMul,
		MaxAttempts:    &task.MaxAttempts,
		HaveAttempts:   &task.HaveAttempts,
	})
}

func (r *DLQInTaskRepository) Delete(id int64) error {
	slog.Info("DLQInTaskRepository.Delete: start", "id", id)
	query := `DELETE FROM "go_saga_dlq_in_task" WHERE "id" = $1;`

	_, err := r.db.Exec(r.ctx, query, id)
	if err != nil {
		slog.Error("DLQInTaskRepository.Delete: query error", "error", err.Error(), "id", id)
		return err
	}

	slog.Info("DLQInTaskRepository.Delete: success", "id", id)
	return err
}

func (r *DLQInTaskRepository) UpdateByID(id int64, update domain.DLQTaskUpdate) error {
	slog.Info("DLQInTaskRepository.UpdateByID: start", "id", id)
	query, args, err := database.GenerateUpdateQueryById("go_saga_dlq_in_task", id, update)
	if err != nil {
		slog.Error("DLQInTaskRepository.UpdateByID: build query error", "error", err.Error(), "id", id)
		return err
	}

	_, err = r.db.Exec(r.ctx, query, args...)
	if err != nil {
		slog.Error("DLQInTaskRepository.UpdateByID: exec error", "error", err.Error(), "id", id)
		return err
	}

	slog.Info("DLQInTaskRepository.UpdateByID: success", "id", id)
	return err
}

func (r *DLQInTaskRepository) GetByStatus(status domain.TaskStatus) ([]domain.DLQEntry, error) {
	slog.Info("DLQInTaskRepository.GetByStatus: start", "status", status)
	query := `
		WITH updated AS (
			UPDATE "go_saga_dlq_in_task" AS d
			SET "have_attempts" = d."have_attempts"
			FROM "go_saga_in_task" AS t
			WHERE d."task_id" = t."id"
				AND t."status" = $1
				AND d."updated_at" + (d."time_for_next_try" || ' seconds')::interval <= CURRENT_TIMESTAMP
				AND d."have_attempts" < d."max_attempts"
			RETURNING d."id", d."task_id", d."time_for_next_try", d."time_mul", d."max_attempts", d."have_attempts", d."updated_at",
					  t."id" AS task_id, t."idempotency_key", t."data", t."status", t."info", t."updated_at" AS task_updated_at
		)
		SELECT * FROM updated;
	`

	rows, err := r.db.Query(r.ctx, query, status)
	if err != nil {
		slog.Error("DLQInTaskRepository.GetByStatus: query error", "error", err.Error(), "status", status)
		return nil, err
	}
	defer rows.Close()

	entries, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (domain.DLQEntry, error) {
		var (
			entry domain.DLQEntry
			info  sql.NullString
		)
		if err := row.Scan(
			&entry.DLQ.ID,
			&entry.DLQ.TaskID,
			&entry.DLQ.TimeForNextTry,
			&entry.DLQ.TimeMul,
			&entry.DLQ.MaxAttempts,
			&entry.DLQ.HaveAttempts,
			&entry.DLQ.UpdatedAt,
			&entry.Task.ID,
			&entry.Task.IdempotencyKey,
			&entry.Task.Data,
			&entry.Task.Status,
			&info,
			&entry.Task.UpdatedAt,
		); err != nil {
			return domain.DLQEntry{}, err
		}
		if info.Valid {
			entry.Task.Info = &info.String
		}
		return entry, nil
	})

	if err != nil {
		slog.Error("DLQInTaskRepository.GetByStatus: scan error", "error", err.Error(), "status", status)
		return nil, err
	}

	slog.Info("DLQInTaskRepository.GetByStatus: success", "status", status, "count", len(entries))
	return entries, err
}

func (r *DLQInTaskRepository) GetErrorsWithAttempts() ([]domain.DLQEntry, error) {
	slog.Info("DLQInTaskRepository.GetErrorsWithAttempts: start")
	query := `
		SELECT d."id", d."task_id", d."time_for_next_try", d."time_mul", d."max_attempts", d."have_attempts", d."updated_at",
			   t."id", t."idempotency_key", t."data", t."rollback_data", t."status", t."info", t."updated_at"
		FROM "go_saga_dlq_in_task" AS d
		JOIN "go_saga_in_task" AS t ON d."task_id" = t."id"
		WHERE t."status" = 'error' AND d."have_attempts" >= d."max_attempts";
	`

	rows, err := r.db.Query(r.ctx, query)
	if err != nil {
		slog.Error("DLQInTaskRepository.GetErrorsWithAttempts: query error", "error", err.Error())
		return nil, err
	}
	defer rows.Close()

	entries, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (domain.DLQEntry, error) {
		var (
			entry    domain.DLQEntry
			info     sql.NullString
			rollback sql.NullString
		)
		if err := row.Scan(
			&entry.DLQ.ID,
			&entry.DLQ.TaskID,
			&entry.DLQ.TimeForNextTry,
			&entry.DLQ.TimeMul,
			&entry.DLQ.MaxAttempts,
			&entry.DLQ.HaveAttempts,
			&entry.DLQ.UpdatedAt,
			&entry.Task.ID,
			&entry.Task.IdempotencyKey,
			&entry.Task.Data,
			&rollback,
			&entry.Task.Status,
			&info,
			&entry.Task.UpdatedAt,
		); err != nil {
			return domain.DLQEntry{}, err
		}
		if info.Valid {
			entry.Task.Info = &info.String
		}
		if rollback.Valid {
			raw := json.RawMessage(rollback.String)
			entry.Task.RollbackData = &raw
		}
		return entry, nil
	})

	if err != nil {
		slog.Error("DLQInTaskRepository.GetErrorsWithAttempts: scan error", "error", err.Error())
		return nil, err
	}

	slog.Info("DLQInTaskRepository.GetErrorsWithAttempts: success", "count", len(entries))
	return entries, nil
}
