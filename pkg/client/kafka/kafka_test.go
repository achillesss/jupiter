package kafka

import (
	"fmt"
	"testing"

	"github.com/douyu/jupiter/pkg/client/kafka/consumer"
	"github.com/douyu/jupiter/pkg/client/kafka/producer"
)

func runProducer(p *producer.Producer, topics ...string) {
	for _, topic := range topics {
		for _, word := range []string{"Welcome", "to", "the", "Confluent", "Kafka", "Golang", "client"} {
			var err = p.ProduceTo(topic, -1, 0, []byte(word))
			if err != nil {
				fmt.Printf("Produce failed: %v\n", err)
			} else {
				fmt.Printf("Produce %s to %s Success\n", word, topic)
			}
		}
	}
}

func runConsumer(c *consumer.Consumer) {
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
	var topics = []string{"test_topic1", "test_topic2"}
	var producerConfig = producer.DefaultKafkaConfig()

	var consumerConfig = consumer.DefaultKafkaConfig()
	consumerConfig.KafkaConfig.GroupID = "TestConsumerGroup"

	var p = producerConfig.Build()
	defer func() {
		p.Flush(15 * 1000)
		p.Close()
	}()

	// 	var newTopic = "test_topic_3"
	// 	var a = p.NewAdminClient()
	// 	var md, err = a.GetMetadata(&newTopic, false, 1000)
	// 	if err != nil {
	// 		fmt.Printf("get md failed: %v\n", err)
	// 		return
	// 	}
	// 	fmt.Printf("get md: %+v\n", md)
	//
	// 	result, err := a.DeleteTopics(context.Background(), topics)
	// 	if err != nil {
	// 		fmt.Printf("rm topics failed: %v\n", err)
	// 		return
	// 	}
	//
	// 	fmt.Printf("result: %+v\n", result)

	p.RunMonitor()
	go runProducer(p, topics...)

	var c = consumerConfig.Build()
	c.SubscribeTopics(topics, nil)

	defer c.Close()
	runConsumer(c)
}
