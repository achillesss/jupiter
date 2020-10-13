package kafka

import (
	"fmt"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/douyu/jupiter/pkg/client/kafka/consumer"
	"github.com/douyu/jupiter/pkg/client/kafka/producer"
)

func runProducer(p *producer.Producer, topics ...string) {
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Produce failed: %v\n", ev.TopicPartition)
					continue
				}

				fmt.Printf("Produce msg: %+v\n", ev.TopicPartition)
			}
		}
	}()

	// Produce messages to topic (asynchronously)
	for _, topic := range topics {
		for _, word := range []string{"Welcome", "to", "the", "Confluent", "Kafka", "Golang", "client"} {
			p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Value:          []byte(word),
			}, nil)
		}
	}

	// Wait for message deliveries before shutting down
	p.Flush(15 * 1000)
}

func runConsumer(c *consumer.Consumer, topics ...string) {
	c.SubscribeTopics(topics, nil)

	for {
		var msg, err = c.ReadMessage(-1)
		if err != nil {
			fmt.Printf("Comsume failed: %v\n", err)
			continue
		}

		if err == nil {
			fmt.Printf("Receive: %s: %s\n", msg.TopicPartition, string(msg.Value))
		}

	}
}

func TestKafka(t *testing.T) {
	var producerConfig = producer.DefaultKafkaConfig()

	var consumerConfig = consumer.DefaultKafkaConfig()
	consumerConfig.GroupID = "TestConsumerGroup"

	var topics = []string{"test_topic1", "test_topic2"}

	var p = producerConfig.Build()
	defer p.Close()
	go runProducer(p, topics...)

	var c = consumerConfig.Build()
	defer c.Close()
	runConsumer(c, topics...)
}
