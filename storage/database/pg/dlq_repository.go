package pg

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/Filin153/gosaga/domain"
	"github.com/Filin153/gosaga/storage/database"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// dlqPgRepository реализует database.DLQRepository для DLQ-таблиц.
// Конкретные таблицы (dlq и базовая таблица задач) задаются в конструкторе.
type dlqPgRepository struct {
	db        session
	dlqTable  string
	taskTable string
}

// NewDLQInTaskRepository создаёт репозиторий для таблиц go_saga_dlq_in_task и go_saga_in_task.
func NewDLQInTaskRepository(ctx context.Context, pool *pgxpool.Pool) database.DLQRepository {
	slog.Debug("DLQInTaskRepository.New", "ctx_set", ctx != nil)
	return &dlqPgRepository{
		db:        pool,
		dlqTable:  "go_saga_dlq_in_task",
		taskTable: "go_saga_in_task",
	}
}

// NewDLQOutTaskRepository создаёт репозиторий для таблиц go_saga_dlq_out_task и go_saga_out_task.
func NewDLQOutTaskRepository(ctx context.Context, pool *pgxpool.Pool) database.DLQRepository {
	slog.Debug("DLQOutTaskRepository.New", "ctx_set", ctx != nil)
	return &dlqPgRepository{
		db:        pool,
		dlqTable:  "go_saga_dlq_out_task",
		taskTable: "go_saga_out_task",
	}
}

func (r *dlqPgRepository) WithSession(sess session) database.DLQRepository {
	slog.Debug("dlqPgRepository.WithSession", "dlq_table", r.dlqTable)
	return &dlqPgRepository{
		db:        sess,
		dlqTable:  r.dlqTable,
		taskTable: r.taskTable,
	}
}

func (r *dlqPgRepository) Create(ctx context.Context, task *domain.DLQTask) (int64, error) {
	slog.Debug("dlqPgRepository.Create: start", "dlq_table", r.dlqTable, "task_id", task.TaskID)
	query := fmt.Sprintf(`
		INSERT INTO "%s" ("task_id")
		VALUES ($1)
		RETURNING "id", "updated_at";
	`, r.dlqTable)

	err := r.db.QueryRow(ctx, query,
		task.TaskID,
	).Scan(&task.ID, &task.UpdatedAt)
	if err != nil {
		slog.Error("dlqPgRepository.Create: query error", "error", err.Error(), "dlq_table", r.dlqTable, "task_id", task.TaskID)
		return 0, err
	}

	slog.Debug("dlqPgRepository.Create: success", "dlq_table", r.dlqTable, "id", task.ID, "task_id", task.TaskID)
	return task.ID, nil
}

func (r *dlqPgRepository) GetByID(ctx context.Context, id int64) (*domain.DLQTask, error) {
	slog.Debug("dlqPgRepository.GetByID: start", "dlq_table", r.dlqTable, "id", id)
	query := fmt.Sprintf(`
		SELECT "id", "task_id", "time_for_next_try", "time_mul", "max_attempts", "have_attempts", "updated_at"
		FROM "%s"
		WHERE "id" = $1;
	`, r.dlqTable)

	var task domain.DLQTask

	err := r.db.QueryRow(ctx, query, id).Scan(
		&task.ID,
		&task.TaskID,
		&task.TimeForNextTry,
		&task.TimeMul,
		&task.MaxAttempts,
		&task.HaveAttempts,
		&task.UpdatedAt,
	)
	if err != nil {
		slog.Error("dlqPgRepository.GetByID: query error", "error", err.Error(), "dlq_table", r.dlqTable, "id", id)
		return nil, err
	}

	slog.Debug("dlqPgRepository.GetByID: success", "dlq_table", r.dlqTable, "id", task.ID, "task_id", task.TaskID)
	return &task, nil
}

func (r *dlqPgRepository) GetByTaskID(ctx context.Context, taskID int64) (*domain.DLQTask, error) {
	slog.Debug("dlqPgRepository.GetByTaskID: start", "dlq_table", r.dlqTable, "task_id", taskID)
	query := fmt.Sprintf(`
		SELECT "id", "task_id", "time_for_next_try", "time_mul", "max_attempts", "have_attempts", "updated_at"
		FROM "%s"
		WHERE "task_id" = $1;
	`, r.dlqTable)

	var task domain.DLQTask

	err := r.db.QueryRow(ctx, query, taskID).Scan(
		&task.ID,
		&task.TaskID,
		&task.TimeForNextTry,
		&task.TimeMul,
		&task.MaxAttempts,
		&task.HaveAttempts,
		&task.UpdatedAt,
	)
	if err != nil {
		slog.Error("dlqPgRepository.GetByTaskID: query error", "error", err.Error(), "dlq_table", r.dlqTable, "task_id", taskID)
		return nil, err
	}

	slog.Debug("dlqPgRepository.GetByTaskID: success", "dlq_table", r.dlqTable, "id", task.ID, "task_id", task.TaskID)
	return &task, nil
}

func (r *dlqPgRepository) Update(ctx context.Context, task *domain.DLQTask) error {
	slog.Debug("dlqPgRepository.Update: start", "dlq_table", r.dlqTable, "id", task.ID, "task_id", task.TaskID)
	return r.UpdateByID(ctx, task.ID, domain.DLQTaskUpdate{
		TaskID:         &task.TaskID,
		TimeForNextTry: &task.TimeForNextTry,
		TimeMul:        &task.TimeMul,
		MaxAttempts:    &task.MaxAttempts,
		HaveAttempts:   &task.HaveAttempts,
	})
}

func (r *dlqPgRepository) Delete(ctx context.Context, id int64) error {
	slog.Debug("dlqPgRepository.Delete: start", "dlq_table", r.dlqTable, "id", id)
	query := fmt.Sprintf(`DELETE FROM "%s" WHERE "id" = $1;`, r.dlqTable)

	_, err := r.db.Exec(ctx, query, id)
	if err != nil {
		slog.Error("dlqPgRepository.Delete: query error", "error", err.Error(), "dlq_table", r.dlqTable, "id", id)
		return err
	}

	slog.Debug("dlqPgRepository.Delete: success", "dlq_table", r.dlqTable, "id", id)
	return nil
}

func (r *dlqPgRepository) UpdateByID(ctx context.Context, id int64, update domain.DLQTaskUpdate) error {
	slog.Debug("dlqPgRepository.UpdateByID: start", "dlq_table", r.dlqTable, "id", id)
	query, args, err := database.GenerateUpdateQueryById(r.dlqTable, id, update)
	if err != nil {
		slog.Error("dlqPgRepository.UpdateByID: build query error", "error", err.Error(), "dlq_table", r.dlqTable, "id", id)
		return err
	}

	_, err = r.db.Exec(ctx, query, args...)
	if err != nil {
		slog.Error("dlqPgRepository.UpdateByID: exec error", "error", err.Error(), "dlq_table", r.dlqTable, "id", id)
		return err
	}

	slog.Debug("dlqPgRepository.UpdateByID: success", "dlq_table", r.dlqTable, "id", id)
	return nil
}

func (r *dlqPgRepository) GetByStatus(ctx context.Context, status domain.TaskStatus, limit int) ([]domain.DLQEntry, error) {
	slog.Debug("dlqPgRepository.GetByStatus: start", "dlq_table", r.dlqTable, "status", status)

	// Reserve error tasks to avoid duplicate processing; recycle stale reservations.
	if status == domain.TaskStatusError {
		const reservationTTL = 60 * time.Second
		query := fmt.Sprintf(`
			WITH candidates AS (
				SELECT d."id" AS dlq_id, d."task_id"
				FROM "%s" AS d
				JOIN "%s" AS t ON d."task_id" = t."id"
				WHERE (t."status" = $1 OR (t."status" = $2 AND t."updated_at" <= timezone('UTC', now()) - ($3 * interval '1 second')))
				  AND d."updated_at" + (d."time_for_next_try" * interval '1 second') <= timezone('UTC', now())
				  AND d."have_attempts" < d."max_attempts"
				ORDER BY d."updated_at"
				FOR UPDATE SKIP LOCKED
				LIMIT $4
			),
			updated_tasks AS (
				UPDATE "%s" AS t
				SET "status" = $2
				FROM candidates c
				WHERE t."id" = c."task_id"
				RETURNING t."id", t."idempotency_key", t."data", t."rollback_data", t."status", t."info", t."updated_at"
			),
			bumped_dlq AS (
				UPDATE "%s" AS d
				SET "have_attempts" = d."have_attempts"
				FROM candidates c
				WHERE d."id" = c."dlq_id"
				RETURNING d."id", d."task_id", d."time_for_next_try", d."time_mul", d."max_attempts", d."have_attempts", d."updated_at"
			)
			SELECT 
				t."id", t."idempotency_key", t."data", t."rollback_data", t."status", t."info", t."updated_at",
				d."id", d."task_id", d."time_for_next_try", d."time_mul", d."max_attempts", d."have_attempts", d."updated_at"
			FROM updated_tasks t
			JOIN bumped_dlq d ON t."id" = d."task_id";
		`, r.dlqTable, r.taskTable, r.taskTable, r.dlqTable)

		rows, err := r.db.Query(ctx, query, domain.TaskStatusError, domain.TaskStatusReserved, int(reservationTTL.Seconds()), limit)
		if err != nil {
			slog.Error("dlqPgRepository.GetByStatus: query error", "error", err.Error(), "dlq_table", r.dlqTable, "status", status)
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
				&entry.Task.ID,
				&entry.Task.IdempotencyKey,
				&entry.Task.Data,
				&rollback,
				&entry.Task.Status,
				&info,
				&entry.Task.UpdatedAt,
				&entry.DLQ.ID,
				&entry.DLQ.TaskID,
				&entry.DLQ.TimeForNextTry,
				&entry.DLQ.TimeMul,
				&entry.DLQ.MaxAttempts,
				&entry.DLQ.HaveAttempts,
				&entry.DLQ.UpdatedAt,
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
			slog.Error("dlqPgRepository.GetByStatus: scan error", "error", err.Error(), "dlq_table", r.dlqTable, "status", status)
			return nil, err
		}

		slog.Debug("dlqPgRepository.GetByStatus: success", "dlq_table", r.dlqTable, "status", status, "count", len(entries))
		return entries, nil
	}

	query := fmt.Sprintf(`
		WITH updated AS (
			UPDATE "%s" AS d
			SET "have_attempts" = d."have_attempts"
			FROM "%s" AS t
			WHERE d."task_id" = t."id"
				AND t."status" = $1
				AND d."updated_at" + (d."time_for_next_try" || ' seconds')::interval <= CURRENT_TIMESTAMP
				AND d."have_attempts" < d."max_attempts"
			RETURNING d."id", d."task_id", d."time_for_next_try", d."time_mul", d."max_attempts", d."have_attempts", d."updated_at",
					  t."id" AS task_id, t."idempotency_key", t."data", t."rollback_data", t."status", t."info", t."updated_at" AS task_updated_at
		)
		SELECT * FROM updated
		FOR UPDATE SKIP LOCKED
		LIMIT $2;
	`, r.dlqTable, r.taskTable)

	rows, err := r.db.Query(ctx, query, status, limit)
	if err != nil {
		slog.Error("dlqPgRepository.GetByStatus: query error", "error", err.Error(), "dlq_table", r.dlqTable, "status", status)
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
		slog.Error("dlqPgRepository.GetByStatus: scan error", "error", err.Error(), "dlq_table", r.dlqTable, "status", status)
		return nil, err
	}

	slog.Debug("dlqPgRepository.GetByStatus: success", "dlq_table", r.dlqTable, "status", status, "count", len(entries))
	return entries, nil
}

func (r *dlqPgRepository) GetErrorsWithAttempts(ctx context.Context) ([]domain.DLQEntry, error) {
	slog.Debug("dlqPgRepository.GetErrorsWithAttempts: start", "dlq_table", r.dlqTable)
	query := fmt.Sprintf(`
		SELECT d."id", d."task_id", d."time_for_next_try", d."time_mul", d."max_attempts", d."have_attempts", d."updated_at",
			   t."id", t."idempotency_key", t."data", t."rollback_data", t."status", t."info", t."updated_at"
		FROM "%s" AS d
		JOIN "%s" AS t ON d."task_id" = t."id"
		WHERE t."status" = 'error' AND d."have_attempts" >= d."max_attempts";
	`, r.dlqTable, r.taskTable)

	rows, err := r.db.Query(ctx, query)
	if err != nil {
		slog.Error("dlqPgRepository.GetErrorsWithAttempts: query error", "error", err.Error(), "dlq_table", r.dlqTable)
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
		slog.Error("dlqPgRepository.GetErrorsWithAttempts: scan error", "error", err.Error(), "dlq_table", r.dlqTable)
		return nil, err
	}

	slog.Debug("dlqPgRepository.GetErrorsWithAttempts: success", "dlq_table", r.dlqTable, "count", len(entries))
	return entries, nil
}
