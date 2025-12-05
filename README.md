# github.com/Filin153/gosaga

## v1 (gosaga) quick guide

Пакет `v1` реализует паттерн Saga с хранением задач в PostgreSQL и пересылкой сообщений через Kafka.

### Требования
- PostgreSQL, таблицы из `v1/pg-migration.sql`.
- Подключение `pgxpool.Pool`.
- Kafka + `sarama.Config` для продюсера/консьюмера.

### Интерфейсы
- `WorkerInterface` — ваш воркер:
  - `New(ctx) (WorkerInterface, error)`
  - `Worker(task, sess)` — обработка задачи.
  - `DlqWorker(task, sess)` — обработка из DLQ.
- Репозитории задач/ДЛК — реализованы для PostgreSQL (см. `storage/database/pg`), но вы можете подменять их реализациями/моками.

### Базовый пример
```go
package main

import (
    "context"
    gosaga "github.com/Filin153/gosaga/v1"
    "github.com/Filin153/gosaga/domain"
    "github.com/Filin153/gosaga/storage/database"
    "github.com/IBM/sarama"
    "github.com/jackc/pgx/v5/pgxpool"
)

type demoWorker struct{}

func (w *demoWorker) New(ctx context.Context) (gosaga.WorkerInterface, error) { return w, nil }
func (w *demoWorker) Worker(task *domain.SagaTask, sess database.Session) error {
    // бизнес-логика in/out задачи
    return nil
}
func (w *demoWorker) DlqWorker(task *domain.SagaTask, sess database.Session) error { return w.Worker(task, sess) }

func main() {
    ctx := context.Background()
    pool, _ := pgxpool.New(ctx, "postgres://user:pass@host/db")
    kafkaConf := sarama.NewConfig()

    saga, _ := gosaga.NewSaga[struct{}](ctx, pool, "consumer-group", []string{"input-topic"}, []string{"kafka:9092"}, kafkaConf)

    // limiter — параллелизм по каждому из 4 потоков (in/out/DLQ-in/DLQ-out)
    _ = saga.RunWorkers(ctx, 4, &demoWorker{}, &demoWorker{})

    // запись исходящей задачи
    _ = saga.Write(ctx, &domain.SagaMsg{Key: "k", Value: map[string]any{"foo": "bar"}, Topic: "out-topic"}, nil, func() {})
}
```

### Ключевые методы
- `RunWorkers(ctx, limiter, outWorker, inWorker)` — запускает обработку in/out задач и соответствующих DLQ.
- `Write(ctx, msg, rollbackMsg, rollbackFunc)` — синхронно добавляет задачу в out-таблицу с idempotency key; при ошибке вызывает `rollbackFunc`.
- `AsyncWrite(ctx, msg, rollbackMsg, rollbackFunc)` — то же, но добавление выполняется в горутине; при ранней ошибке вызывает `rollbackFunc` асинхронно.

### Таблицы (см. `v1/pg-migration.sql`)
- `go_saga_in_task` / `go_saga_out_task` — очереди входящих/исходящих задач.
- `go_saga_dlq_in_task` / `go_saga_dlq_out_task` — DLQ для ошибок/ретраев.

### Статусы задач (см. `domain.TaskStatus`)
- `wait`, `work`, `ready`, `error`, `rollback`, `error_rollback_none`.

### Моки
- Пакеты `mocks/kafka`, `mocks/database`, `mocks/core/v1` содержат mockery/testify моки для Writer/Reader Kafka и репозиториев/воркера — пригодятся в тестах.

