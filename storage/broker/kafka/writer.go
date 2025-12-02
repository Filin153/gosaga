package kafka

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/Filin153/gosaga/domain"

	"github.com/IBM/sarama"
)

type KafkaWriter struct {
	ctx          context.Context
	syncProducer sarama.SyncProducer
}

func NewKafkaWriter(ctx context.Context, hosts []string, conf *sarama.Config) (*KafkaWriter, error) {
	p, err := sarama.NewSyncProducer(hosts, conf)
	if err != nil {
		slog.Error("KafkaWriter.New: NewSyncProducer error", "error", err.Error())
		return nil, err
	}

	slog.Info("KafkaWriter.New: success")
	return &KafkaWriter{
		ctx:          ctx,
		syncProducer: p,
	}, nil
}

func (k *KafkaWriter) Write(msg *domain.SagaMsg, rollback *domain.SagaMsg, idempotencyKey string) error {
	slog.Info("KafkaWriter.Write: start", "topic", msg.Topic, "key", msg.Key)
	payload, err := json.Marshal(msg.Value)
	if err != nil {
		slog.Error("KafkaWriter.Write: marshal payload error", "error", err.Error())
		return err
	}

	headers := []sarama.RecordHeader{
		{
			Key:   []byte("idempotency_key"),
			Value: []byte(idempotencyKey),
		},
	}
	if rollback != nil {
		rollbackBytes, err := json.Marshal(rollback)
		if err != nil {
			slog.Error("KafkaWriter.Write: marshal rollback error", "error", err.Error())
			return err
		}
		headers = append(headers, sarama.RecordHeader{
			Key:   []byte("rollback_data"),
			Value: rollbackBytes,
		})
	}

	message := &sarama.ProducerMessage{
		Topic:   msg.Topic,
		Key:     sarama.StringEncoder(msg.Key),
		Value:   sarama.ByteEncoder(payload),
		Headers: headers,
	}

	select {
	case <-k.ctx.Done():
		slog.Info("KafkaWriter.Write: context canceled, closing producer")
		k.syncProducer.Close()
		return nil
	default:
		_, _, err := k.syncProducer.SendMessage(message)
		if err != nil {
			slog.Error("KafkaWriter.Write: SendMessage error", "error", err.Error(), "topic", msg.Topic, "key", msg.Key)
			return err
		}
	}

	slog.Info("KafkaWriter.Write: success", "topic", msg.Topic, "key", msg.Key)
	return nil
}
