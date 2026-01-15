package gosaga

import (
	"github.com/Filin153/gosaga/domain"
	"github.com/Filin153/gosaga/storage/broker/kafka"
	"github.com/Filin153/gosaga/storage/database"

	"github.com/IBM/sarama"
)

// Aliases for domain types.
type (
	SagaMsg        = domain.SagaMsg
	SagaTask       = domain.SagaTask
	TaskStatus     = domain.TaskStatus
	SagaTaskUpdate = domain.SagaTaskUpdate
	DLQTask        = domain.DLQTask
	DLQEntry       = domain.DLQEntry
)

// Aliases for repositories.
type (
	TaskRepository = database.TaskRepository
	DLQRepository  = database.DLQRepository
	Session        = database.Session
)

// Aliases for Kafka interfaces.
type (
	KafkaWriter = kafka.Writer
	KafkaReader = kafka.Reader
)

// NewKafkaWriter re-exports kafka.NewKafkaWriter.
func NewKafkaWriter(hosts []string, conf *sarama.Config) (KafkaWriter, error) {
	return kafka.NewKafkaWriter(hosts, conf)
}

// NewKafkaReader re-exports kafka.NewKafkaRider (consumer group reader).
func NewKafkaReader(group string, topics []string, hosts []string, conf *sarama.Config, buffer int) (KafkaReader, error) {
	return kafka.NewKafkaRider(group, topics, hosts, conf, buffer)
}
