package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Filin153/gosaga/domain"
	"github.com/IBM/sarama"
)

type testSyncProducer struct {
	lastMsg     *sarama.ProducerMessage
	sendErr     error
	closeCalled bool
}

func (p *testSyncProducer) SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	p.lastMsg = msg
	return 0, 0, p.sendErr
}

func (p *testSyncProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	panic("not implemented in tests")
}

func (p *testSyncProducer) Close() error {
	p.closeCalled = true
	return nil
}

// Transactional methods to satisfy sarama.SyncProducer interface; not used in tests.
func (p *testSyncProducer) TxnStatus() sarama.ProducerTxnStatusFlag {
	return sarama.ProducerTxnStatusFlag(0)
}

func (p *testSyncProducer) IsTransactional() bool { return false }
func (p *testSyncProducer) BeginTxn() error       { return nil }
func (p *testSyncProducer) CommitTxn() error      { return nil }
func (p *testSyncProducer) AbortTxn() error       { return nil }

func (p *testSyncProducer) AddOffsetsToTxn(offsets map[string][]*sarama.PartitionOffsetMetadata, groupId string) error {
	return nil
}

func (p *testSyncProducer) AddMessageToTxn(msg *sarama.ConsumerMessage, groupId string, metadata *string) error {
	return nil
}

func TestKafkaWriter_Write_SuccessWithRollback(t *testing.T) {
	producer := &testSyncProducer{}
	writer := &KafkaWriter{syncProducer: producer}

	msg := &domain.SagaMsg{
		Key:   "key",
		Value: map[string]string{"foo": "bar"},
		Topic: "topic",
	}
	rollback := &domain.SagaMsg{
		Key:   "r-key",
		Value: "rollback",
		Topic: "topic",
	}

	ctx := context.Background()
	if err := writer.Write(ctx, msg, rollback, "idem-key"); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	if producer.lastMsg == nil {
		t.Fatalf("Write() did not call SendMessage")
	}

	if producer.lastMsg.Topic != msg.Topic {
		t.Fatalf("Write() Topic = %s, want %s", producer.lastMsg.Topic, msg.Topic)
	}

	if string(producer.lastMsg.Key.(sarama.StringEncoder)) != msg.Key {
		t.Fatalf("Write() Key = %s, want %s", producer.lastMsg.Key, msg.Key)
	}

	// payload is marshalled from msg.Value
	var payload map[string]string
	valueBytes, err := producer.lastMsg.Value.Encode()
	if err != nil {
		t.Fatalf("Value.Encode() error = %v", err)
	}
	if err := json.Unmarshal(valueBytes, &payload); err != nil {
		t.Fatalf("payload unmarshal error = %v", err)
	}
	if payload["foo"] != "bar" {
		t.Fatalf("payload[\"foo\"] = %s, want %s", payload["foo"], "bar")
	}

	// headers must contain idempotency_key and rollback_data
	var haveIdem, haveRollback bool
	for _, h := range producer.lastMsg.Headers {
		switch string(h.Key) {
		case "idempotency_key":
			haveIdem = string(h.Value) == "idem-key"
		case "rollback_data":
			haveRollback = len(h.Value) > 0
		}
	}
	if !haveIdem {
		t.Fatalf("Write() did not set idempotency_key header correctly")
	}
	if !haveRollback {
		t.Fatalf("Write() did not set rollback_data header")
	}
}

func TestKafkaWriter_Write_ContextCanceled(t *testing.T) {
	producer := &testSyncProducer{}
	writer := &KafkaWriter{syncProducer: producer}

	msg := &domain.SagaMsg{
		Key:   "key",
		Value: "value",
		Topic: "topic",
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := writer.Write(ctx, msg, nil, "idem"); err != nil {
		t.Fatalf("Write() with canceled context error = %v, want nil", err)
	}

	if producer.lastMsg != nil {
		t.Fatalf("Write() should not call SendMessage when context is canceled")
	}
	if !producer.closeCalled {
		t.Fatalf("Write() did not close producer on context cancellation")
	}
}

func TestKafkaWriter_Write_SendError(t *testing.T) {
	producer := &testSyncProducer{sendErr: errors.New("send error")}
	writer := &KafkaWriter{syncProducer: producer}

	msg := &domain.SagaMsg{
		Key:   "key",
		Value: "value",
		Topic: "topic",
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := writer.Write(ctx, msg, nil, "idem"); err == nil {
		t.Fatalf("Write() error = nil, want non-nil when SendMessage fails")
	}
}
