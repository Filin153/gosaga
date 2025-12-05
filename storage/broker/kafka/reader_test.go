package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

type stubConsumerGroup struct {
	errorsCh    chan error
	closeCalled bool
}

func (s *stubConsumerGroup) Consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	// Immediately return to allow backoff/cancellation behaviour in Run; not used in these tests.
	return nil
}

func (s *stubConsumerGroup) Errors() <-chan error { return s.errorsCh }
func (s *stubConsumerGroup) Close() error {
	s.closeCalled = true
	return nil
}
func (s *stubConsumerGroup) Pause(partitions map[string][]int32)  {}
func (s *stubConsumerGroup) Resume(partitions map[string][]int32) {}
func (s *stubConsumerGroup) PauseAll()                            {}
func (s *stubConsumerGroup) ResumeAll()                           {}

type stubSession struct {
	ctx    context.Context
	marked []*sarama.ConsumerMessage
}

func (s *stubSession) Claims() map[string][]int32                                               { return nil }
func (s *stubSession) MemberID() string                                                         { return "" }
func (s *stubSession) GenerationID() int32                                                      { return 0 }
func (s *stubSession) MarkOffset(topic string, partition int32, offset int64, metadata string)  {}
func (s *stubSession) ResetOffset(topic string, partition int32, offset int64, metadata string) {}
func (s *stubSession) MarkMessage(msg *sarama.ConsumerMessage, metadata string) {
	s.marked = append(s.marked, msg)
}
func (s *stubSession) Context() context.Context { return s.ctx }
func (s *stubSession) Commit()                  {}

type stubClaim struct {
	msgs <-chan *sarama.ConsumerMessage
}

func (c *stubClaim) Topic() string                            { return "topic" }
func (c *stubClaim) Partition() int32                         { return 0 }
func (c *stubClaim) InitialOffset() int64                     { return 0 }
func (c *stubClaim) HighWaterMarkOffset() int64               { return 0 }
func (c *stubClaim) Messages() <-chan *sarama.ConsumerMessage { return c.msgs }
func (c *stubClaim) Errors() <-chan error                     { return nil }

func TestKafkaRider_ConsumeClaim_ForwardsMessages(t *testing.T) {
	errorsCh := make(chan error)
	cg := &stubConsumerGroup{errorsCh: errorsCh}
	rider := &KafkaRider{
		consumerGroup: cg,
		outChan:       make(chan *sarama.ConsumerMessage, 1),
	}

	msgCh := make(chan *sarama.ConsumerMessage, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sess := &stubSession{ctx: ctx}
	claim := &stubClaim{msgs: msgCh}

	go func() {
		if err := rider.ConsumeClaim(sess, claim); err != nil {
			t.Errorf("ConsumeClaim returned error: %v", err)
		}
	}()

	m := &sarama.ConsumerMessage{Value: []byte("data")}
	msgCh <- m

	select {
	case got := <-rider.outChan:
		if got != m {
			t.Fatalf("ConsumeClaim forwarded wrong message")
		}
	case <-time.After(time.Second):
		t.Fatalf("ConsumeClaim did not forward message")
	}

	// cancel context to stop loop
	cancel()
}

func TestKafkaRider_ConsumeClaim_HandlesErrors(t *testing.T) {
	errorsCh := make(chan error, 1)
	cg := &stubConsumerGroup{errorsCh: errorsCh}
	rider := &KafkaRider{
		consumerGroup: cg,
		outChan:       make(chan *sarama.ConsumerMessage, 1),
	}

	msgCh := make(chan *sarama.ConsumerMessage)
	ctx, cancel := context.WithCancel(context.Background())
	sess := &stubSession{ctx: ctx}
	claim := &stubClaim{msgs: msgCh}

	go rider.ConsumeClaim(sess, claim)
	errorsCh <- sarama.ErrOutOfBrokers
	cancel()
}

func TestKafkaRider_ReadAndShutdown(t *testing.T) {
	cg := &stubConsumerGroup{errorsCh: make(chan error)}
	rider := &KafkaRider{
		consumerGroup: cg,
		outChan:       make(chan *sarama.ConsumerMessage),
	}

	if rider.Read() != rider.outChan {
		t.Fatalf("Read() returned unexpected channel")
	}

	if err := rider.Shutdown(); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if !cg.closeCalled {
		t.Fatalf("Shutdown() did not close consumer group")
	}
}
