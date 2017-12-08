package main

import (
	"testing"

	"github.com/Shopify/sarama"

	"github.com/Shopify/sarama/mocks"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Consumer test", func() {
	var (
		mc *mocks.Consumer
		t  *testing.T
	)
	BeforeEach(func() {
		mc = mocks.NewConsumer(t, nil)
		brokers = []string{"127.0.0.1:9092"}
	})
	AfterEach(func() {
		err := mc.Close()
		Expect(err).To(BeNil())
	})
	It("Should receive and handle message", func() {
		mc.ExpectConsumePartition("test", 0, sarama.OffsetOldest).YieldMessage(&sarama.ConsumerMessage{Value: []byte("Hello world")})
		pc, err := mc.ConsumePartition("test", 0, sarama.OffsetOldest)
		testMsg := <-pc.Messages()
		msg := string(testMsg.Value)
		Expect(msg).To(Equal("Hello world"))
		Expect(err).To(BeNil())
	})
	It("Should return out of broker error", func() {
		mc.ExpectConsumePartition("test", 0, sarama.OffsetOldest).YieldError(sarama.ErrOutOfBrokers)
		pc, err := mc.ConsumePartition("test", 0, sarama.OffsetOldest)
		Expect(err).To(BeNil())
		warning := <-pc.Errors()
		Expect(warning.Err).To(Equal(sarama.ErrOutOfBrokers))
	})
	It("Should be able to create new consumer", func() {
		_, err := newConsumer(brokers, nil)
		Expect(err).To(BeNil())
	})
	It("Should return cluster unreachable when broker IP is wrong", func() {
		brokers = []string{"someIPaddress"}
		_, err := newConsumer(brokers, nil)
		Expect(err).To(Equal(errUnreachable))
	})
	It("Should receive message which belongs to subscribed topic", func() {
		topic := "testTopic"
		consumer, err := newConsumer(brokers, nil)
		Expect(err).To(BeNil())
		subscribe(topic, consumer)

		producer, _ := newProducer()

		msg := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder("This is dummy message"),
		}

		producer.SendMessage(msg)

		Expect(getMessage()).To(Equal("This is dummy message"))
	})
})
