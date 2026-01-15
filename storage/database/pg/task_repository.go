package pg

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Filin153/gosaga/domain"
	"github.com/Filin153/gosaga/storage/database"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// taskPgRepository реализует database.TaskRepository и работает с заданной таблицей.
// Таблица передаётся при создании репозитория, чтобы использовать одну структуру
// для in/out задач.
type taskPgRepository struct {
	db    session
	table string
}

// NewTaskRepository позволяет создать репозиторий для произвольной таблицы задач.
func newTaskRepository(ctx context.Context, pool *pgxpool.Pool, table string) database.TaskRepository {
	return &taskPgRepository{db: pool, table: table}
}

// NewInTaskRepository создаёт репозиторий для таблицы go_saga_in_task.
func NewInTaskRepository(ctx context.Context, pool *pgxpool.Pool) database.TaskRepository {
	return newTaskRepository(ctx, pool, "go_saga_in_task")
}

// NewOutTaskRepository возвращает репозиторий задач для таблицы go_saga_out_task.
// Использует общую структуру taskPgRepository, где таблица задаётся при создании.
func NewOutTaskRepository(ctx context.Context, pool *pgxpool.Pool) database.TaskRepository {
	return newTaskRepository(ctx, pool, "go_saga_out_task")
}

func (r *taskPgRepository) WithSession(sess session) database.TaskRepository {
	return &taskPgRepository{
		db:    sess,
		table: r.table,
	}
}

func (r *taskPgRepository) Create(ctx context.Context, task *domain.SagaTask) (int64, error) {
	query := fmt.Sprintf(`
		INSERT INTO "%s" ("idempotency_key", "data", "rollback_data")
		VALUES ($1, $2, $3)
		RETURNING "id", "updated_at";
	`, r.table)

	err := r.db.QueryRow(ctx, query,
		task.IdempotencyKey,
		task.Data,
		task.RollbackData,
	).Scan(&task.ID, &task.UpdatedAt)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			existing, getErr := r.GetByIdempotencyKey(ctx, task.IdempotencyKey)
			if getErr != nil {
				return 0, getErr
			}
			task.ID = existing.ID
			task.UpdatedAt = existing.UpdatedAt
			return task.ID, nil
		}
		return 0, err
	}

	return task.ID, nil
}

func (r *taskPgRepository) GetByID(ctx context.Context, id int64) (*domain.SagaTask, error) {
	query := fmt.Sprintf(`
		SELECT "id", "idempotency_key", "data", "rollback_data", "status", "info", "updated_at"
		FROM "%s"
		WHERE "id" = $1;
	`, r.table)

	var (
		task     domain.SagaTask
		info     sql.NullString
		rollback sql.NullString
	)

	err := r.db.QueryRow(ctx, query, id).Scan(
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

func (r *taskPgRepository) GetByIdempotencyKey(ctx context.Context, key string) (*domain.SagaTask, error) {
	query := fmt.Sprintf(`
		SELECT "id", "idempotency_key", "data", "rollback_data", "status", "info", "updated_at"
		FROM "%s"
		WHERE "idempotency_key" = $1;
	`, r.table)

	var (
		task     domain.SagaTask
		info     sql.NullString
		rollback sql.NullString
	)

	err := r.db.QueryRow(ctx, query, key).Scan(
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

func (r *taskPgRepository) Update(ctx context.Context, task *domain.SagaTask) error {
	return r.UpdateByID(ctx, task.ID, domain.SagaTaskUpdate{
		IdempotencyKey: &task.IdempotencyKey,
		Data:           &task.Data,
		Status:         &task.Status,
		Info:           task.Info,
	})
}

func (r *taskPgRepository) Delete(ctx context.Context, id int64) error {
	query := fmt.Sprintf(`DELETE FROM "%s" WHERE "id" = $1;`, r.table)

	_, err := r.db.Exec(ctx, query, id)
	return err
}

func (r *taskPgRepository) UpdateByID(ctx context.Context, id int64, update domain.SagaTaskUpdate) error {
	query, args, err := database.GenerateUpdateQueryById(r.table, id, update)
	if err != nil {
		return err
	}

	_, err = r.db.Exec(ctx, query, args...)
	return err
}

func (r *taskPgRepository) GetByStatus(ctx context.Context, status domain.TaskStatus, limit int) ([]domain.SagaTask, error) {
	// When pulling tasks in "wait" we reserve them first to avoid double processing.
	// Reserved tasks that have not moved forward within reservationTTL are retried.
	if status == domain.TaskStatusWait {
		const reservationTTL = 120 * time.Second
		query := fmt.Sprintf(`
			WITH candidates AS (
				SELECT "id"
				FROM "%s"
				WHERE ("status" = $1 OR ("status" = $2 AND "updated_at" <= timezone('UTC', now()) - ($3 * interval '1 second')))
				ORDER BY "updated_at"
				FOR UPDATE SKIP LOCKED
				LIMIT $4
			)
			UPDATE "%s" AS t
			SET "status" = $2
			FROM candidates
			WHERE t."id" = candidates."id"
			RETURNING t."id", t."idempotency_key", t."data", t."rollback_data", t."status", t."info", t."updated_at";
		`, r.table, r.table)

		rows, err := r.db.Query(ctx, query, domain.TaskStatusWait, domain.TaskStatusReserved, int(reservationTTL.Seconds()), limit)
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

	query := fmt.Sprintf(`
		SELECT "id", "idempotency_key", "data", "rollback_data", "status", "info", "updated_at"
		FROM "%s"
		WHERE "status" = $1
		FOR UPDATE SKIP LOCKED
		LIMIT $2;
	`, r.table)

	rows, err := r.db.Query(ctx, query, status, limit)
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
