package main

import (
	"errors"

	"github.com/Shopify/sarama"
)

var (
	brokers        = []string{"127.0.0.1:9092"}
	errUnreachable = errors.New("Kafka cluster is unreachable")
)

func newProducer() (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	// config.Net.SASL.Enable = true
	// config.Net.SASL.User = "user"
	// config.Net.SASL.Password = "Wh46SARTtyt6"
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		if err.Error() == "kafka: client has run out of available brokers to talk to (Is your cluster reachable?)" {
			return nil, errUnreachable
		}
		return nil, err
	}
	return producer, nil
}

func prepareMessage(topic, message string, partition int32) *sarama.ProducerMessage {
	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: partition,
		Value:     sarama.StringEncoder(message),
	}

	return msg
}
