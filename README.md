# github.com/Filin153/gosaga

## EN
Saga helper with Postgres storage and Kafka transport. `NewSaga` auto-runs the migration from `v1/pg-migration.sql`, builds repos, and starts Kafka reader.

### Requirements
- PostgreSQL tables from `v1/pg-migration.sql`
- `pgxpool.Pool`
- Kafka + `sarama.Config`

### Interfaces
- `WorkerInterface`: `New(ctx)`, `Worker(task, sess)`, `DlqWorker(task, sess)`
- Task/DLQ repositories: ready-made PG impl (`storage/database/pg`), can be swapped

### Example
```go
ctx := context.Background()
pool, _ := pgxpool.New(ctx, "postgres://user:pass@host/db")
kafkaConf := sarama.NewConfig()

saga, _ := gosaga.NewSaga(ctx, pool, "consumer-group", []string{"input-topic"}, []string{"kafka:9092"}, kafkaConf)

_ = saga.RunWorkers(ctx, 4, &demoWorker{}, &demoWorker{})
_ = saga.Write(ctx, &domain.SagaMsg{Key: "k", Value: map[string]any{"foo": "bar"}, Topic: "out-topic"}, nil, func() {})
```

### Key methods
- `RunWorkers(ctx, limiter, outWorker, inWorker)` – starts in/out + DLQ loops (limiter per stream)
- `Write(ctx, msg, rollbackMsg, rollbackFunc)` – sync insert to out-task table; on error calls rollbackFunc
- `AsyncWrite(ctx, msg, rollbackMsg, rollbackFunc)` – async version; rollbackFunc on early error

### Tables (see `v1/pg-migration.sql`)
- `go_saga_in_task` / `go_saga_out_task`
- `go_saga_dlq_in_task` / `go_saga_dlq_out_task`

### Statuses (`domain.TaskStatus`)
`wait`, `work`, `ready`, `error`, `rollback`, `error_rollback_none`

### Mocks
`mocks/kafka`, `mocks/database`, `mocks/core/v1` (mockery/testify) for writer/reader/repos/worker.

---

## RU
Хелпер для саг с хранением в Postgres и транспортом Kafka. `NewSaga` автоматически запускает миграцию из `v1/pg-migration.sql`, собирает репозитории и стартует Kafka reader.

### Требования
- PostgreSQL с таблицами из `v1/pg-migration.sql`
- `pgxpool.Pool`
- Kafka + `sarama.Config`

### Интерфейсы
- `WorkerInterface`: `New(ctx)`, `Worker(task, sess)`, `DlqWorker(task, sess)`
- Репозитории задач/DLQ: готовая PG-реализация (`storage/database/pg`), можно подменять

### Пример
```go
ctx := context.Background()
pool, _ := pgxpool.New(ctx, "postgres://user:pass@host/db")
kafkaConf := sarama.NewConfig()

saga, _ := gosaga.NewSaga(ctx, pool, "consumer-group", []string{"input-topic"}, []string{"kafka:9092"}, kafkaConf)

_ = saga.RunWorkers(ctx, 4, &demoWorker{}, &demoWorker{})
_ = saga.Write(ctx, &domain.SagaMsg{Key: "k", Value: map[string]any{"foo": "bar"}, Topic: "out-topic"}, nil, func() {})
```

### Основные методы
- `RunWorkers(ctx, limiter, outWorker, inWorker)` – запускает циклы обработки in/out и DLQ (лимитер на поток)
- `Write(ctx, msg, rollbackMsg, rollbackFunc)` – синхронно пишет в out-таблицу; при ошибке вызывает rollbackFunc
- `AsyncWrite(ctx, msg, rollbackMsg, rollbackFunc)` – асинхронная запись; rollbackFunc при ранней ошибке

### Таблицы
- `go_saga_in_task` / `go_saga_out_task`
- `go_saga_dlq_in_task` / `go_saga_dlq_out_task`

### Статусы (`domain.TaskStatus`)
`wait`, `work`, `ready`, `error`, `rollback`, `error_rollback_none`

### Моки
`mocks/kafka`, `mocks/database`, `mocks/core/v1` — mockery/testify для Writer/Reader и репозиториев/воркера.
