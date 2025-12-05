# gosaga

## EN
Saga helper with Postgres storage and Kafka transport. `NewSaga` auto-runs the migration from `v1/pg-migration.sql`, builds repos, and starts Kafka reader.

### Requirements
- PostgreSQL tables from `v1/pg-migration.sql`
- `pgxpool.Pool`
- Kafka + `sarama.Config`

### Interfaces
- `WorkerInterface`: `New(ctx)`, `Worker(task, sess)`, `DlqWorker(task, sess)`
- Built-in outbound worker: `v1/out_task.go` (`NewOutWorker(kafka.Writer)`) если нужно только публиковать в Kafka.
- Task/DLQ repositories: ready-made PG impl (`storage/database/pg`), can be swapped

### Example
```go
package main

import (
    "context"
    "log"

    gosaga "github.com/Filin153/gosaga/v1"
    "github.com/Filin153/gosaga/storage/broker/kafka"
    "github.com/IBM/sarama"
    "github.com/jackc/pgx/v5/pgxpool"
)

// inbound worker example
type demoInWorker struct{}

func (w *demoInWorker) New(ctx context.Context) (gosaga.WorkerInterface, error) { return w, nil }
func (w *demoInWorker) Worker(task *gosaga.SagaTask, sess gosaga.Session) error {
    // do something with task.Data
    return nil
}
func (w *demoInWorker) DlqWorker(task *gosaga.SagaTask, sess gosaga.Session) error { return w.Worker(task, sess) }

func main() {
    ctx := context.Background()

    pool, err := pgxpool.New(ctx, "postgres://user:pass@host/db")
    if err != nil {
        log.Fatal(err)
    }

    kafkaConf := sarama.NewConfig()
    kafkaConf.Version = sarama.V3_6_0_0

    saga, err := gosaga.NewSaga(ctx, pool, "consumer-group", []string{"input-topic"}, []string{"kafka:9092"}, kafkaConf)
    if err != nil {
        log.Fatal(err)
    }

    // outWorker publishes to Kafka using built-in OutWorker
    kafkaWriter, _ := gosaga.NewKafkaWriter([]string{"kafka:9092"}, kafkaConf)
    outWorker := gosaga.NewOutWorker(kafkaWriter)

    if err := saga.RunWorkers(ctx, 4, outWorker, &demoInWorker{}); err != nil {
        log.Fatal(err)
    }

    // write outgoing task to out-topic
    _ = saga.Write(ctx, &gosaga.SagaMsg{Key: "k", Value: map[string]any{"foo": "bar"}, Topic: "out-topic"}, nil, func() {})
}
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
- Готовый воркер для исходящих задач: `v1/out_task.go` (`NewOutWorker(kafka.Writer)`), если нужно просто публиковать в Kafka.
- Репозитории задач/DLQ: готовая PG-реализация (`storage/database/pg`), можно подменять

### Пример
```go
package main

import (
    "context"
    "log"

    gosaga "github.com/Filin153/gosaga/v1"
    "github.com/Filin153/gosaga/storage/broker/kafka"
    "github.com/IBM/sarama"
    "github.com/jackc/pgx/v5/pgxpool"
)

// пример воркера для входящих задач
type demoInWorker struct{}

func (w *demoInWorker) New(ctx context.Context) (gosaga.WorkerInterface, error) { return w, nil }
func (w *demoInWorker) Worker(task *gosaga.SagaTask, sess gosaga.Session) error {
    // обработка task.Data, работа в транзакции sess
    return nil
}
func (w *demoInWorker) DlqWorker(task *gosaga.SagaTask, sess gosaga.Session) error { return w.Worker(task, sess) }

func main() {
    ctx := context.Background()

    pool, err := pgxpool.New(ctx, "postgres://user:pass@host/db")
    if err != nil {
        log.Fatal(err)
    }

    kafkaConf := sarama.NewConfig()
    kafkaConf.Version = sarama.V3_6_0_0

    saga, err := gosaga.NewSaga(ctx, pool, "consumer-group", []string{"input-topic"}, []string{"kafka:9092"}, kafkaConf)
    if err != nil {
        log.Fatal(err)
    }

    // готовый outWorker для публикации в Kafka (создаем реальный writer)
    kafkaWriter, _ := gosaga.NewKafkaWriter([]string{"kafka:9092"}, kafkaConf)
    outWorker := gosaga.NewOutWorker(kafkaWriter)

    if err := saga.RunWorkers(ctx, 4, outWorker, &demoInWorker{}); err != nil {
        log.Fatal(err)
    }

    // запись исходящей задачи в out-topic
    _ = saga.Write(ctx, &gosaga.SagaMsg{Key: "k", Value: map[string]any{"foo": "bar"}, Topic: "out-topic"}, nil, func() {})
}
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


Versioned docs:
- v1: `v1/README.md`

Документация по версиям:
- v1: `v1/README.md`
