package producer

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Producer struct {
	*Config
	*kafka.Producer
}
