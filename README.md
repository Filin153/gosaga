# github.com/Filin153/gosaga (v1)

## EN
Saga helper with Postgres storage and Kafka transport. `NewSaga`:
- runs migration from `v1/pg-migration.sql`
- wires PG repos
- starts Kafka reader
- exposes aliases/helpers so you import everything from `github.com/Filin153/gosaga/v1`.

### Requirements
- PostgreSQL tables from `v1/pg-migration.sql`
- `pgxpool.Pool`
- Kafka + `sarama.Config`
- Migration creates PK columns without auto-increment; configure sequences/defaults or provide IDs yourself if you change schema.

### Interfaces & exports
- `WorkerInterface`:
  - `New(ctx context.Context) (WorkerInterface, error)`
  - `Worker(ctx context.Context, task *SagaTask, sess Session) error`
  - `DlqWorker(ctx context.Context, task *SagaTask, sess Session) error`
- Built-in outbound handler: `NewOutWorker(kafka.Writer)` provides `Worker/DlqWorker`; wrap it with a tiny struct implementing `New` to satisfy the interface (see example).
- Aliases in this package: `SagaMsg`, `SagaTask`, `TaskStatus`, `Session`, `TaskRepository`, `DLQRepository`
- Kafka helpers: `NewKafkaWriter(...)`, `NewKafkaReader(...)`
- Task/DLQ repositories: PG impl (`storage/database/pg`), swappable

### Example
```go
package main

import (
    "context"
    "log"

    gosaga "github.com/Filin153/gosaga/v1"
    "github.com/IBM/sarama"
    "github.com/jackc/pgx/v5/pgxpool"
)

// inbound worker example
type demoInWorker struct{}

func (w *demoInWorker) New(ctx context.Context) (gosaga.WorkerInterface, error) { return w, nil }
func (w *demoInWorker) Worker(ctx context.Context, task *gosaga.SagaTask, sess gosaga.Session) error {
    // do something with task.Data inside sess (pgx.Tx)
    return nil
}
func (w *demoInWorker) DlqWorker(ctx context.Context, task *gosaga.SagaTask, sess gosaga.Session) error {
    return w.Worker(ctx, task, sess)
}

// thin wrapper so OutWorker satisfies WorkerInterface (adds New)
type demoOutWorker struct{ *gosaga.OutWorker }

func (w *demoOutWorker) New(ctx context.Context) (gosaga.WorkerInterface, error) { return w, nil }

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
    outWorker := &demoOutWorker{gosaga.NewOutWorker(kafkaWriter)}

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
Хелпер для саг с хранением в Postgres и транспортом Kafka. `NewSaga`:
- автоматически запускает миграцию `v1/pg-migration.sql`
- поднимает PG-репозитории
- стартует Kafka reader
- экспортирует алиасы/хелперы, чтобы импортировать всё из `github.com/Filin153/gosaga/v1`.

### Требования
- PostgreSQL с таблицами из `v1/pg-migration.sql`
- `pgxpool.Pool`
- Kafka + `sarama.Config`
- В миграции PK колонка без автоинкремента: добавьте sequence/DEFAULT в своей схеме или задавайте ID вручную, если меняете DDL.

### Интерфейсы и экспорты
- `WorkerInterface`:
  - `New(ctx context.Context) (WorkerInterface, error)`
  - `Worker(ctx context.Context, task *SagaTask, sess Session) error`
  - `DlqWorker(ctx context.Context, task *SagaTask, sess Session) error`
- Готовый обработчик исходящих задач: `NewOutWorker(kafka.Writer)` реализует `Worker/DlqWorker`; оберните в структуру с методом `New`, чтобы соответствовать интерфейсу (см. пример).
- Алиасы: `SagaMsg`, `SagaTask`, `TaskStatus`, `Session`, `TaskRepository`, `DLQRepository`
- Kafka-хелперы: `NewKafkaWriter(...)`, `NewKafkaReader(...)`
- Репозитории задач/DLQ: PG-реализация (`storage/database/pg`), можно подменять

### Пример
```go
package main

import (
    "context"
    "log"

    gosaga "github.com/Filin153/gosaga/v1"
    "github.com/IBM/sarama"
    "github.com/jackc/pgx/v5/pgxpool"
)

// пример воркера для входящих задач
type demoInWorker struct{}

func (w *demoInWorker) New(ctx context.Context) (gosaga.WorkerInterface, error) { return w, nil }
func (w *demoInWorker) Worker(ctx context.Context, task *gosaga.SagaTask, sess gosaga.Session) error {
    // обработка task.Data, работа в транзакции sess
    return nil
}
func (w *demoInWorker) DlqWorker(ctx context.Context, task *gosaga.SagaTask, sess gosaga.Session) error {
    return w.Worker(ctx, task, sess)
}

// обертка, добавляющая метод New к готовому OutWorker
type demoOutWorker struct{ *gosaga.OutWorker }

func (w *demoOutWorker) New(ctx context.Context) (gosaga.WorkerInterface, error) { return w, nil }

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
    outWorker := &demoOutWorker{gosaga.NewOutWorker(kafkaWriter)}

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
