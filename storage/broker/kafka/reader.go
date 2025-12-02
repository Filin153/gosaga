package kafka

import (
	"context"
	"log/slog"
	"time"

	"github.com/IBM/sarama"
)

type KafkaRider struct {
	ctx           context.Context
	topics        []string
	group         string
	consumerGroup sarama.ConsumerGroup
	outChan       chan *sarama.ConsumerMessage
}

func NewKafkaRider(ctx context.Context, group string, topics []string, hosts []string, conf *sarama.Config, buffer int) (*KafkaRider, error) {
	conf.Consumer.Return.Errors = true
	conf.Consumer.Offsets.AutoCommit.Enable = true

	consumerGroup, err := sarama.NewConsumerGroup(hosts, group, conf)
	if err != nil {
		return nil, err
	}

	return &KafkaRider{
		ctx:           ctx,
		topics:        topics,
		group:         group,
		consumerGroup: consumerGroup,
		outChan:       make(chan *sarama.ConsumerMessage, buffer),
	}, nil
}

func (k *KafkaRider) Run() {
	go func() {
		backoff := time.Second
		for {
			select {
			case <-k.ctx.Done():
				slog.Info("KafkaRider.Run: context canceled, stop consuming")
				return
			default:
				if err := k.consumerGroup.Consume(k.ctx, k.topics, k); err != nil {
					slog.Error("KafkaRider.Run: Consume error", "error", err.Error())
					time.Sleep(backoff)
					if backoff < 30*time.Second {
						backoff *= 2
					}
					continue
				}
				backoff = time.Second
			}
		}
	}()
}

func (k *KafkaRider) Shutdown() error {
	return k.consumerGroup.Close()
}

func (k *KafkaRider) Read() <-chan *sarama.ConsumerMessage {
	return k.outChan
}

func (k *KafkaRider) Setup(_ sarama.ConsumerGroupSession) error { return nil }
func (k *KafkaRider) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}
func (k *KafkaRider) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	msgChan := claim.Messages()
	errChan := k.consumerGroup.Errors()
	for {
		select {
		case msg := <-msgChan:
			if msg == nil {
				continue
			}
			// Помечаем сообщение для автокоммита оффсета
			sess.MarkMessage(msg, "")
			k.outChan <- msg
		case err := <-errChan:
			if err != nil {
				slog.Error("KafkaRider.ConsumeClaim", "error", err.Error())
			}
		case <-k.ctx.Done():
			slog.Info("KafkaRider.ConsumeClaim: context canceled")
			return nil
		}
	}
}
