package gosaga

import (
	"log/slog"

	"github.com/Filin153/gosaga/domain"
	"github.com/Filin153/gosaga/storage/database"

	"github.com/jackc/pgx/v5"
)

func (s *Saga[T]) outWork(task *domain.SagaTask, do func(task *domain.SagaTask, sess database.Session) error) error {
	tx, err := s.pool.BeginTx(s.ctx, pgx.TxOptions{})
	if err != nil {
		slog.Error("outWork: pool.BeginTx", "error", err.Error(), "task_id", task.ID)
		return err
	}
	defer tx.Rollback(s.ctx)

	outTaskRepoWithSession := s.outTaskRepo.WithSession(tx)
	dlqOutTaskRepoWithSession := s.dlqOutTaskRepo.WithSession(tx)

	info := "Start"
	status := domain.TaskStatusWork
	err = outTaskRepoWithSession.UpdateByID(task.ID, domain.SagaTaskUpdate{
		Status: &status,
		Info:   &info,
	})
	if err != nil {
		slog.Error("outWork: outTaskRepo.UpdateByID (set work)", "error", err.Error(), "task_id", task.ID)
		return err
	}

	err = do(task, tx)
	if err != nil {
		info := err.Error()
		status := domain.TaskStatusError
		err = outTaskRepoWithSession.UpdateByID(task.ID, domain.SagaTaskUpdate{
			Status: &status,
			Info:   &info,
		})
		if err != nil {
			slog.Error("outWork: outTaskRepo.UpdateByID (set error)", "error", err.Error(), "task_id", task.ID)
			return err
		}

		DQLTask, err := dlqOutTaskRepoWithSession.GetByTaskID(task.ID)
		if err != nil {
			return err
		}

		if DQLTask == nil {
			dlqTask := domain.DLQTask{
				TaskID: task.ID,
			}
			_, err := dlqOutTaskRepoWithSession.Create(&dlqTask)
			if err != nil {
				slog.Error("outWork: dlqOutTaskRepo.Create", "error", err.Error(), "task_id", task.ID)
				return err
			}
		}

		if err := tx.Commit(s.ctx); err != nil {
			slog.Error("outWork: tx.Commit (on error)", "error", err.Error(), "task_id", task.ID)
		}
		return err
	}

	info = "OK"
	status = domain.TaskStatusReady
	err = outTaskRepoWithSession.UpdateByID(task.ID, domain.SagaTaskUpdate{
		Status: &status,
		Info:   &info,
	})
	if err != nil {
		slog.Error("outWork: outTaskRepo.UpdateByID (set ready)", "error", err.Error(), "task_id", task.ID)
		return err
	}

	if err := tx.Commit(s.ctx); err != nil {
		slog.Error("outWork: tx.Commit", "error", err.Error(), "task_id", task.ID)
		return err
	}

	return nil
}

func (s *Saga[T]) inWork(task *domain.SagaTask, do func(task *domain.SagaTask, sess database.Session) error) error {
	tx, err := s.pool.BeginTx(s.ctx, pgx.TxOptions{})
	if err != nil {
		slog.Error("inWork: pool.BeginTx", "error", err.Error(), "task_id", task.ID)
		return err
	}
	defer tx.Rollback(s.ctx)

	inTaskRepoWithSession := s.inTaskRepo.WithSession(tx)
	dlqInTaskRepoWithSession := s.dlqInTaskRepo.WithSession(tx)

	info := "Start"
	status := domain.TaskStatusWork
	err = inTaskRepoWithSession.UpdateByID(task.ID, domain.SagaTaskUpdate{
		Status: &status,
		Info:   &info,
	})
	if err != nil {
		slog.Error("inWork: inTaskRepo.UpdateByID (set work)", "error", err.Error(), "task_id", task.ID)
		return err
	}

	err = do(task, tx)
	if err != nil {
		info := err.Error()
		status := domain.TaskStatusError
		err = inTaskRepoWithSession.UpdateByID(task.ID, domain.SagaTaskUpdate{
			Status: &status,
			Info:   &info,
		})
		if err != nil {
			slog.Error("inWork: inTaskRepo.UpdateByID (set error)", "error", err.Error(), "task_id", task.ID)
			return err
		}

		dlqTask, err := dlqInTaskRepoWithSession.GetByTaskID(task.ID)
		if err != nil {
			return err
		}

		if dlqTask == nil {
			newDlqTask := domain.DLQTask{
				TaskID: task.ID,
			}
			_, err := dlqInTaskRepoWithSession.Create(&newDlqTask)
			if err != nil {
				slog.Error("inWork: dlqInTaskRepo.Create", "error", err.Error(), "task_id", task.ID)
				return err
			}
		}

		if err := tx.Commit(s.ctx); err != nil {
			slog.Error("inWork: tx.Commit (on error)", "error", err.Error(), "task_id", task.ID)
		}
		return err
	}

	info = "OK"
	status = domain.TaskStatusReady
	err = inTaskRepoWithSession.UpdateByID(task.ID, domain.SagaTaskUpdate{
		Status: &status,
		Info:   &info,
	})
	if err != nil {
		slog.Error("inWork: inTaskRepo.UpdateByID (set ready)", "error", err.Error(), "task_id", task.ID)
		return err
	}

	if err := tx.Commit(s.ctx); err != nil {
		slog.Error("inWork: tx.Commit", "error", err.Error(), "task_id", task.ID)
		return err
	}

	return nil
}
