package producer

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/douyu/jupiter/pkg/client/kafka/admin"
	"github.com/douyu/jupiter/pkg/xlog"
)

type Producer struct {
	*Config
	*kafka.Producer
}

func (p *Producer) ProduceTo(topic string, partition int32, offset kafka.Offset, data []byte) error {
	return p.Producer.Produce(
		&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: partition,
				Offset:    offset,
			},
			Value: data,
		},
		nil,
	)
}

func (p *Producer) ReadProducedEvent() {
	e := <-p.Events()
	switch ev := e.(type) {
	case *kafka.Message:
		if ev.TopicPartition.Error != nil {
			fmt.Printf("Produce failed: %v\n", ev.TopicPartition)
			return
		}

		fmt.Printf("Produce msg: %+v\n", ev.TopicPartition)
	}
}

func (p *Producer) RunMonitor() {
	go func() {
		for {
			p.ReadProducedEvent()
		}
	}()
}

func (p *Producer) NewAdminClient() *admin.Admin {
	var a, err = kafka.NewAdminClientFromProducer(p.Producer)
	if err != nil {
		p.logger.Panic("new kafka admin failed", xlog.String("error", err.Error()))
		return nil
	}

	var ac admin.Admin
	ac.AdminClient = a
	return &ac
}
