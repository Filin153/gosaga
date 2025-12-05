package gosaga

import (
	"context"
	"log/slog"
	"time"

	"github.com/Filin153/gosaga/domain"
	"github.com/Filin153/gosaga/storage/database"

	"github.com/jackc/pgx/v5"
)

func (s *Saga) dataBaseTaskReader(ctx context.Context, repo database.TaskRepository) <-chan *domain.SagaTask {
	taskMsg := make(chan *domain.SagaTask)
	go func() {
		defer close(taskMsg)
		waitTime := 1
		for {
			select {
			case <-ctx.Done():
				return
			default:
				tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{
					IsoLevel: pgx.ReadUncommitted,
				})
				if err != nil {
					slog.Error("pool.BeginTx", "error", err.Error())
					time.Sleep(time.Duration(waitTime) * time.Second)
					waitTime *= 2
					continue
				}

				data, err := repo.WithSession(tx).GetByStatus(ctx, domain.TaskStatusWait)
				if err != nil {
					slog.Error("dataBaseTaskReader: GetByStatus error", "error", err.Error())
					if err := tx.Rollback(ctx); err != nil {
						slog.Error("dataBaseTaskReader: GetByStatus error, rollback", "error", err.Error())
					}
					time.Sleep(time.Duration(waitTime) * time.Second)
					waitTime *= 2
					continue
				}

				if err := tx.Commit(ctx); err != nil {
					slog.Error("dataBaseTaskReader: tx.Commit error", "error", err.Error())
					if err := tx.Rollback(ctx); err != nil {
						slog.Error("dataBaseTaskReader: tx.Commit error, rollback", "error", err.Error())
					}
					time.Sleep(time.Duration(waitTime) * time.Second)
					waitTime *= 2
					continue
				}
				waitTime = 1

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

func (s *Saga) dataBaseDLQTaskReader(ctx context.Context, repo database.DLQRepository) <-chan *domain.SagaTask {
	taskMsg := make(chan *domain.SagaTask)
	go func() {
		defer close(taskMsg)
		waitTime := 1
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
					time.Sleep(time.Duration(waitTime) * time.Second)
					waitTime *= 2
					continue
				}

				data, err := repo.WithSession(tx).GetByStatus(ctx, domain.TaskStatusWait)
				if err != nil {
					slog.Error("dataBaseDLQTaskReader: GetByStatus error", "error", err.Error())
					if err := tx.Rollback(ctx); err != nil {
						slog.Error("dataBaseDLQTaskReader: GetByStatus error, rollback", "error", err.Error())
					}
					time.Sleep(time.Duration(waitTime) * time.Second)
					waitTime *= 2
					continue
				}

				if err := tx.Commit(ctx); err != nil {
					slog.Error("dataBaseDLQTaskReader: tx.Commit error", "error", err.Error())
					if err := tx.Rollback(ctx); err != nil {
						slog.Error("dataBaseDLQTaskReader: tx.Commit error, rollback", "error", err.Error())
					}
					time.Sleep(time.Duration(waitTime) * time.Second)
					waitTime *= 2
					continue
				}
				waitTime = 1

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
