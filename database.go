package gosaga

import (
	"github.com/Filin153/gosaga/domain"
	"github.com/Filin153/gosaga/storage/database"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
)

func (s *Saga[T]) dataBaseTaskReader(repo database.TaskRepository) <-chan *domain.SagaTask {
	taskMsg := make(chan *domain.SagaTask)
	go func() {
		slog.Info("dataBaseTaskReader: wait for ctx.Done to close channel")
		<-s.ctx.Done()
		close(taskMsg)
	}()

	go func() {
		waitTime := 1
		for {
			select {
			case <-s.ctx.Done():
				return
			default:
				tx, err := s.pool.BeginTx(s.ctx, pgx.TxOptions{
					IsoLevel: pgx.ReadUncommitted,
				})
				if err != nil {
					slog.Error("pool.BeginTx", "error", err.Error())
					time.Sleep(time.Duration(waitTime) * time.Second)
					waitTime *= 2
					continue
				}

				data, err := repo.WithSession(tx).GetByStatus(domain.TaskStatusWait)
				if err != nil {
					tx.Rollback(s.ctx)
					slog.Error("dataBaseTaskReader: GetByStatus error", "error", err.Error())
					time.Sleep(time.Duration(waitTime) * time.Second)
					waitTime *= 2
					continue
				}

				if err := tx.Commit(s.ctx); err != nil {
					tx.Rollback(s.ctx)
					slog.Error("dataBaseTaskReader: tx.Commit error", "error", err.Error())
					time.Sleep(time.Duration(waitTime) * time.Second)
					waitTime *= 2
					continue
				}
				waitTime = 1

				for _, item := range data {
					select {
					case taskMsg <- &item:
						continue
					case <-s.ctx.Done():
						tx.Rollback(s.ctx)
						slog.Info("dataBaseTaskReader: context canceled while sending tasks")
						return
					}
				}

			}
		}
	}()

	return taskMsg
}

func (s *Saga[T]) dataBaseDLQTaskReader(repo database.DLQRepository) <-chan *domain.SagaTask {
	taskMsg := make(chan *domain.SagaTask)
	go func() {
		slog.Info("dataBaseDLQTaskReader: wait for ctx.Done to close channel")
		<-s.ctx.Done()
		close(taskMsg)
	}()

	go func() {
		waitTime := 1
		for {
			select {
			case <-s.ctx.Done():
				return
			default:
				tx, err := s.pool.BeginTx(s.ctx, pgx.TxOptions{
					IsoLevel: pgx.ReadUncommitted,
				})
				if err != nil {
					slog.Error("dataBaseDLQTaskReader: pool.BeginTx error", "error", err.Error())
					time.Sleep(time.Duration(waitTime) * time.Second)
					waitTime *= 2
					return
				}

				data, err := repo.WithSession(tx).GetByStatus(domain.TaskStatusWait)
				if err != nil {
					tx.Rollback(s.ctx)
					slog.Error("dataBaseDLQTaskReader: GetByStatus error", "error", err.Error())
					time.Sleep(time.Duration(waitTime) * time.Second)
					waitTime *= 2
					continue
				}

				if err := tx.Commit(s.ctx); err != nil {
					tx.Rollback(s.ctx)
					slog.Error("dataBaseDLQTaskReader: tx.Commit error", "error", err.Error())
					time.Sleep(time.Duration(waitTime) * time.Second)
					waitTime *= 2
					return
				}
				waitTime = 1

				for _, item := range data {
					select {
					case taskMsg <- &item.Task:
						continue
					case <-s.ctx.Done():
						tx.Rollback(s.ctx)
						slog.Info("dataBaseDLQTaskReader: context canceled while sending tasks")
						return
					}
				}

			}
		}
	}()

	return taskMsg
}
