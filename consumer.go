package main

import (
	"fmt"

	"github.com/Shopify/sarama"
)

func newConsumer(brokers []string, config *sarama.Config) (sarama.Consumer, error) {
	consumer, err := sarama.NewConsumer(brokers, nil)
	if err != nil {
		if err.Error() == "kafka: client has run out of available brokers to talk to (Is your cluster reachable?)" {
			return nil, errUnreachable
		}
		return nil, err
	}
	return consumer, nil
}

func subscribe(topic string, consumer sarama.Consumer) error {
	partitionList, err := consumer.Partitions(topic) //get all partitions on the given topic
	if err != nil {
		fmt.Println("Error retrieving partitionList ", err)
		return err
	}
	initialOffset := sarama.OffsetOldest //get offset for the oldest message on the topic

	for _, partition := range partitionList {
		pc, err := consumer.ConsumePartition(topic, partition, initialOffset)

		if err != nil {
			fmt.Println("Error consuming partition", err)
			return err
		}
		go func(pc sarama.PartitionConsumer) {
			for message := range pc.Messages() {
				messageReceived(message)
			}
		}(pc)
	}
	return nil
}

func messageReceived(message *sarama.ConsumerMessage) {
	saveMessage(string(message.Value))
}
