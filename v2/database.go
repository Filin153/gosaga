package gosaga

import (
	"context"
	"log/slog"
	"time"

	"github.com/Filin153/gosaga/domain"
	"github.com/Filin153/gosaga/storage/database"

	"github.com/jackc/pgx/v5"
)

// dataBaseTaskReader polls tasks with status wait and streams them to channel until context is done.
func (s *Saga) dataBaseTaskReader(ctx context.Context, repo database.TaskRepository, taskMsg chan *domain.SagaTask) <-chan *domain.SagaTask {
	go func() {
		defer close(taskMsg)
		errWait := 1
		maxErrorWait := 30
		idleWait := 200 * time.Millisecond
		maxIdleWait := 5 * time.Second
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if len(taskMsg) != 0 {
					sleep(idleWait)
					if idleWait < maxIdleWait {
						idleWait *= 2
						if idleWait > maxIdleWait {
							idleWait = maxIdleWait
						}
					}
					continue
				}

				tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{
					IsoLevel: pgx.ReadUncommitted,
				})
				if err != nil {
					slog.Error("pool.BeginTx", "error", err.Error())
					sleep(time.Duration(errWait) * time.Second)
					errWait *= 2
					if errWait > maxErrorWait {
						errWait = maxErrorWait
					}
					continue
				}

				data, err := repo.WithSession(tx).GetByStatus(ctx, domain.TaskStatusWait, 10)
				if err != nil {
					slog.Error("dataBaseTaskReader: GetByStatus error", "error", err.Error())
					if err := tx.Rollback(ctx); err != nil {
						slog.Error("dataBaseTaskReader: GetByStatus error, rollback", "error", err.Error())
					}
					sleep(time.Duration(errWait) * time.Second)
					errWait *= 2
					if errWait > maxErrorWait {
						errWait = maxErrorWait
					}
					continue
				}

				if err := tx.Commit(ctx); err != nil {
					slog.Error("dataBaseTaskReader: tx.Commit error", "error", err.Error())
					if err := tx.Rollback(ctx); err != nil {
						slog.Error("dataBaseTaskReader: tx.Commit error, rollback", "error", err.Error())
					}
					sleep(time.Duration(errWait) * time.Second)
					errWait *= 2
					if errWait > maxErrorWait {
						errWait = maxErrorWait
					}
					continue
				}
				errWait = 1

				if len(data) == 0 {
					sleep(idleWait)
					if idleWait < maxIdleWait {
						idleWait *= 2
						if idleWait > maxIdleWait {
							idleWait = maxIdleWait
						}
					}
					continue
				}
				idleWait = 200 * time.Millisecond

				for _, item := range data {
					select {
					case taskMsg <- &item:
						continue
					case <-ctx.Done():
						slog.Info("dataBaseTaskReader: context canceled while sending tasks")
						if err := tx.Rollback(ctx); err != nil {
							slog.Error("dataBaseTaskReader: context canceled while sending tasks, rollback", "error", err.Error())
						}
						return
					}
				}

			}
		}
	}()

	return taskMsg
}

// dataBaseDLQTaskReader polls DLQ entries with status error and streams their tasks.
func (s *Saga) dataBaseDLQTaskReader(ctx context.Context, repo database.DLQRepository) <-chan *domain.SagaTask {
	taskMsg := make(chan *domain.SagaTask)
	go func() {
		defer close(taskMsg)
		errWait := 1
		maxErrorWait := 30
		idleWait := 200 * time.Millisecond
		maxIdleWait := 5 * time.Second
		for {
			select {
			case <-ctx.Done():
				return
			default:
				tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{
					IsoLevel: pgx.ReadUncommitted,
				})
				if err != nil {
					slog.Error("dataBaseDLQTaskReader: pool.BeginTx error", "error", err.Error())
					sleep(time.Duration(errWait) * time.Second)
					errWait *= 2
					if errWait > maxErrorWait {
						errWait = maxErrorWait
					}
					continue
				}

				data, err := repo.WithSession(tx).GetByStatus(ctx, domain.TaskStatusError, 10)
				if err != nil {
					slog.Error("dataBaseDLQTaskReader: GetByStatus error", "error", err.Error())
					if err := tx.Rollback(ctx); err != nil {
						slog.Error("dataBaseDLQTaskReader: GetByStatus error, rollback", "error", err.Error())
					}
					sleep(time.Duration(errWait) * time.Second)
					errWait *= 2
					if errWait > maxErrorWait {
						errWait = maxErrorWait
					}
					continue
				}

				if err := tx.Commit(ctx); err != nil {
					slog.Error("dataBaseDLQTaskReader: tx.Commit error", "error", err.Error())
					if err := tx.Rollback(ctx); err != nil {
						slog.Error("dataBaseDLQTaskReader: tx.Commit error, rollback", "error", err.Error())
					}
					sleep(time.Duration(errWait) * time.Second)
					errWait *= 2
					if errWait > maxErrorWait {
						errWait = maxErrorWait
					}
					continue
				}
				errWait = 1

				if len(data) == 0 {
					sleep(idleWait)
					if idleWait < maxIdleWait {
						idleWait *= 2
						if idleWait > maxIdleWait {
							idleWait = maxIdleWait
						}
					}
					continue
				}
				idleWait = 200 * time.Millisecond

				for _, item := range data {
					select {
					case taskMsg <- &item.Task:
						continue
					case <-ctx.Done():
						slog.Info("dataBaseDLQTaskReader: context canceled while sending tasks")
						if err := tx.Rollback(ctx); err != nil {
							slog.Error("dataBaseDLQTaskReader: context canceled while sending tasks, rollback", "error", err.Error())
						}
						return
					}
				}

			}
		}
	}()

	return taskMsg
}
