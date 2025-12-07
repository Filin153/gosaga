package gosaga

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/Filin153/gosaga/domain"
	"github.com/Filin153/gosaga/storage/broker/kafka"
	"github.com/Filin153/gosaga/storage/database"
	"github.com/Filin153/gosaga/storage/database/pg"

	"github.com/IBM/sarama"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// txBeginner abstracts pgxpool.Pool/pgx.Conn for starting transactions.
type txBeginner interface {
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
}

// Saga orchestrates saga tasks stored in Postgres and flowing through Kafka.
type Saga struct {
	pool           txBeginner
	inTaskRepo     database.TaskRepository
	outTaskRepo    database.TaskRepository
	dlqInTaskRepo  database.DLQRepository
	dlqOutTaskRepo database.DLQRepository
	kafkaReader    kafka.Reader
	kafkaWriter    kafka.Writer
}

//go:embed pg-migration.sql
var migrationSQL string

// NewSaga initializes repositories, runs DB migration, and starts Kafka reader.
func NewSaga(ctx context.Context, pool *pgxpool.Pool, readerGroup string, readerTopics, hosts []string, conf *sarama.Config) (*Saga, error) {
	slog.Info("Saga.NewSaga: start")

	if err := runMigration(ctx, pool); err != nil {
		slog.Error("Saga.NewSaga: migration error", "error", err.Error())
		return nil, err
	}

	kafkaWriter, err := kafka.NewKafkaWriter(hosts, conf)
	if err != nil {
		slog.Error("Saga.NewSaga: NewKafkaWriter error", "error", err.Error())
		return nil, err
	}

	kafkaReader, err := kafka.NewKafkaRider(readerGroup, readerTopics, hosts, conf, 10)
	if err != nil {
		slog.Error("Saga.NewSaga: NewKafkaRider error", "error", err.Error())
		return nil, err
	}

	kafkaReader.Run(ctx)

	slog.Info("Saga.NewSaga: success")
	return &Saga{
		pool:           pool,
		inTaskRepo:     pg.NewInTaskRepository(ctx, pool),
		outTaskRepo:    pg.NewOutTaskRepository(ctx, pool),
		dlqInTaskRepo:  pg.NewDLQInTaskRepository(ctx, pool),
		dlqOutTaskRepo: pg.NewDLQOutTaskRepository(ctx, pool),
		kafkaWriter:    kafkaWriter,
		kafkaReader:    kafkaReader,
	}, nil
}

// RunWorkers spins up workers for in/out tasks and their DLQ counterparts.
// limiter caps concurrency per stream; total goroutines up to limiter*4.
func (s *Saga) RunWorkers(ctx context.Context, limiter int, newOutWorker WorkerInterface, newInWorker WorkerInterface) error {
	slog.Info("Saga.RunWorkers: start", "limiter", limiter)
	OutTaskWorkerCountLimiter := make(chan struct{}, limiter)
	dlqOutTaskWorkerCountLimiter := make(chan struct{}, limiter)
	InTaskWorkerCountLimiter := make(chan struct{}, limiter)
	dlqInTaskWorkerCountLimiter := make(chan struct{}, limiter)

	// Create task in database
	go func() {
		slog.Info("Saga.RunWorkers: start Kafka->DB ingestion loop")
		for msg := range s.kafkaReader.Read() {
			var msgIdempotencyKey string
			var rollbackData json.RawMessage

			var msgIdempotencyKeyHave, rollbackDataHave bool
			for _, header := range msg.Headers {
				if msgIdempotencyKeyHave && rollbackDataHave {
					break
				}

				if string(header.Key) == "idempotency_key" {
					msgIdempotencyKey = string(header.Value)
					msgIdempotencyKeyHave = true
				}

				if string(header.Key) == "rollback_data" {
					rollbackData = header.Value
					rollbackDataHave = true
				}
			}

			if msgIdempotencyKey == "" {
				slog.Warn("Saga.RunWorkers: Kafka message without idempotency_key header")
				continue
			}

			task, err := s.inTaskRepo.GetByIdempotencyKey(ctx, msgIdempotencyKey)
			if task != nil {
				slog.Info("Saga.RunWorkers: skip duplicated message by idempotency_key", "idempotency_key", msgIdempotencyKey)
				continue
			} else if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				slog.Error("Saga.RunWorkers: inTaskRepo.GetByIdempotencyKey", "error", err.Error(), "idempotency_key", msgIdempotencyKey)
				continue
			}

			// тут добавлние в БД
			sagaTaskData := domain.SagaTask{
				IdempotencyKey: msgIdempotencyKey,
				Data:           msg.Value,
				RollbackData:   &rollbackData,
			}
			_, err = s.inTaskRepo.Create(ctx, &sagaTaskData)
			if err != nil {
				slog.Error("Saga.RunWorkers: inTaskRepo.Create", "error", err.Error(), "idempotency_key", msgIdempotencyKey)
				continue
			}
		}
	}()

	// RollBack
	go func() {
		slog.Info("Saga.RunWorkers: start rollback loop")
		for {
			sleep(10 * time.Second)

			errTasks, err := s.dlqInTaskRepo.GetErrorsWithAttempts(ctx)
			if err != nil {
				slog.Error("Saga.RunWorkers: dlqInTaskRepo.GetErrorsWithAttempts", "error", err.Error())
				continue
			}

			for _, task := range errTasks {
				select {
				case <-ctx.Done():
					return
				default:
					var rollBackMsg domain.SagaMsg
					if task.Task.RollbackData == nil {
						status := domain.TaskStatusErrorRollbackNone
						err = s.inTaskRepo.UpdateByID(ctx, task.Task.ID, domain.SagaTaskUpdate{
							Status: &status,
						})
						if err != nil {
							slog.Error("Saga.RunWorkers: inTaskRepo.UpdateByID (no rollback data)", "error", err.Error(), "task_id", task.Task.ID)
							continue
						}
						continue
					}

					err := json.Unmarshal(*task.Task.RollbackData, &rollBackMsg)
					if err != nil {
						slog.Error("Saga.RunWorkers: json.Unmarshal rollbackData", "error", err.Error(), "task_id", task.Task.ID)
						continue
					}

					err = s.Write(ctx, &rollBackMsg, nil, func() {})
					if err != nil {
						slog.Error("Saga.RunWorkers: Write rollback message", "error", err.Error())
						continue
					}

					status := domain.TaskStatusRollback
					err = s.inTaskRepo.UpdateByID(ctx, task.Task.ID, domain.SagaTaskUpdate{
						Status: &status,
					})
					if err != nil {
						slog.Error("Saga.RunWorkers: inTaskRepo.UpdateByID (set rollback)", "error", err.Error(), "task_id", task.Task.ID)
						continue
					}
				}
			}
		}
	}()

	// InTask
	go func() {
		slog.Info("Saga.RunWorkers: start InTask loop")
		inTasksMsgChan := s.dataBaseTaskReader(ctx, s.inTaskRepo)
		for task := range inTasksMsgChan {
			select {
			case <-ctx.Done():
				return
			case InTaskWorkerCountLimiter <- struct{}{}:
			}
			go func(task *domain.SagaTask) {
				defer func() { <-InTaskWorkerCountLimiter }()
				_ = s.inWork(ctx, task, newInWorker.Worker)
			}(task)
		}
	}()

	// DlqInTask
	go func() {
		slog.Info("Saga.RunWorkers: start DlqInTask loop")
		dlqInTasksMsgChan := s.dataBaseDLQTaskReader(ctx, s.dlqInTaskRepo)
		for task := range dlqInTasksMsgChan {
			select {
			case <-ctx.Done():
				return
			case dlqInTaskWorkerCountLimiter <- struct{}{}:
			}
			go func(task *domain.SagaTask) {
				defer func() { <-dlqInTaskWorkerCountLimiter }()
				_ = s.inWork(ctx, task, newInWorker.DlqWorker)
			}(task)
		}
	}()

	// OutTask
	go func() {
		slog.Info("Saga.RunWorkers: start OutTask loop")
		outTasksMsgChan := s.dataBaseTaskReader(ctx, s.outTaskRepo)
		for task := range outTasksMsgChan {
			select {
			case <-ctx.Done():
				return
			case OutTaskWorkerCountLimiter <- struct{}{}:
			}
			go func(task *domain.SagaTask) {
				defer func() { <-OutTaskWorkerCountLimiter }()
				_ = s.outWork(ctx, task, newOutWorker.Worker)
			}(task)
		}
	}()

	// DlqOutTask
	go func() {
		slog.Info("Saga.RunWorkers: start DlqOutTask loop")
		dlqOutTasksMsgChan := s.dataBaseDLQTaskReader(ctx, s.dlqOutTaskRepo)
		for task := range dlqOutTasksMsgChan {
			select {
			case <-ctx.Done():
				return
			case dlqOutTaskWorkerCountLimiter <- struct{}{}:
			}
			go func(task *domain.SagaTask) {
				defer func() { <-dlqOutTaskWorkerCountLimiter }()
				_ = s.outWork(ctx, task, newOutWorker.Worker)
			}(task)
		}
	}()

	return nil
}

func (s *Saga) Write(ctx context.Context, msg *domain.SagaMsg, rollbackMsg *domain.SagaMsg, rollbackFunc func()) (err error) {
	defer func() {
		if err != nil {
			slog.Error("Saga.Write: error, calling rollback", "error", err.Error())
			rollbackFunc()
		}
	}()

	idempotencyKey, err := s.generateIdempotencyKey()
	if err != nil {
		slog.Error("Saga.Write: generateIdempotencyKey error", "error", err.Error())
		return err
	}

	jsonMarshal, err := json.Marshal(msg)
	if err != nil {
		slog.Error("Saga.Write: marshal msg error", "error", err.Error())
		return err
	}

	var rollbackPayload *json.RawMessage
	if rollbackMsg != nil {
		rollbackDataMarshal, err := json.Marshal(rollbackMsg)
		if err != nil {
			slog.Error("Saga.Write: marshal rollbackMsg error", "error", err.Error())
			return err
		}
		raw := json.RawMessage(rollbackDataMarshal)
		rollbackPayload = &raw
	}

	task := domain.SagaTask{
		IdempotencyKey: idempotencyKey,
		Data:           jsonMarshal,
		RollbackData:   rollbackPayload,
	}
	_, err = s.outTaskRepo.Create(ctx, &task)
	if err != nil {
		slog.Error("Saga.Write: outTaskRepo.Create error", "error", err.Error())
		return err
	}

	slog.Info("Saga.Write: success")
	return nil
}

func (s *Saga) AsyncWrite(ctx context.Context, msg *domain.SagaMsg, rollbackMsg *domain.SagaMsg, rollbackFunc func()) (err error) {
	defer func() {
		if err != nil {
			slog.Error("Saga.AsyncWrite: early error, calling rollback", "error", err.Error())
			go rollbackFunc()
		}
	}()

	idempotencyKey, err := s.generateIdempotencyKey()
	if err != nil {
		slog.Error("Saga.AsyncWrite: generateIdempotencyKey error", "error", err.Error())
		return err
	}

	jsonMarshal, err := json.Marshal(msg)
	if err != nil {
		slog.Error("Saga.AsyncWrite: marshal msg error", "error", err.Error())
		return err
	}

	var rollbackPayload *json.RawMessage
	if rollbackMsg != nil {
		rollbackDataMarshal, err := json.Marshal(rollbackMsg)
		if err != nil {
			slog.Error("Saga.AsyncWrite: marshal rollbackMsg error", "error", err.Error())
			return err
		}
		raw := json.RawMessage(rollbackDataMarshal)
		rollbackPayload = &raw
	}

	go func(rollbackPayload *json.RawMessage) {
		var err error
		defer func() {
			if err != nil {
				slog.Error("Saga.AsyncWrite goroutine: error, calling rollback", "error", err.Error())
				go rollbackFunc()
			}
		}()

		task := domain.SagaTask{
			IdempotencyKey: idempotencyKey,
			Data:           jsonMarshal,
			RollbackData:   rollbackPayload,
		}
		_, err = s.outTaskRepo.Create(ctx, &task)
		if err != nil {
			slog.Error("AsyncWrite go func()", "error", err.Error())
		}
	}(rollbackPayload)

	return nil
}

func runMigration(ctx context.Context, execer interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}) error {
	_, err := execer.Exec(ctx, migrationSQL)
	return err
}
