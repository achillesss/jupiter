package consumer

import "github.com/confluentinc/confluent-kafka-go/kafka"

type Consumer struct {
	*Config
	*kafka.Consumer
}
