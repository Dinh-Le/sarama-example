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
		mc.ExpectConsumePartition("test", 0, sarama.OffsetOldest).YieldMessage(&sarama.ConsumerMessage{Value: []byte("Free bird")})
		pc, err := mc.ConsumePartition("test", 0, sarama.OffsetOldest)
		testMsg := <-pc.Messages()
		msg := string(testMsg.Value)
		Expect(msg).To(Equal("Free bird"))
		Expect(err).To(BeNil())
	})
	It("Should receive and handle messages in order", func() {
		mc.ExpectConsumePartition("test", 0, sarama.OffsetOldest).YieldMessage(&sarama.ConsumerMessage{Value: []byte("Stairway to heaven")})
		mc.ExpectConsumePartition("test", 0, sarama.OffsetOldest).YieldMessage(&sarama.ConsumerMessage{Value: []byte("Hotel California")})
		pc, err := mc.ConsumePartition("test", 0, sarama.OffsetOldest)
		Expect(err).To(BeNil())
		testMsg := <-pc.Messages()
		Expect(string(testMsg.Value)).To(Equal("Stairway to heaven"))
		testMsg2 := <-pc.Messages()
		Expect(string(testMsg2.Value)).To(Equal("Hotel California"))
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
	It("Should be able to subscribe a topic", func() {
		topic := "someTopic"
		topicMap := make(map[string][]int32)
		topicMap[topic] = []int32{0}
		mc.SetTopicMetadata(topicMap)
		mc.ExpectConsumePartition(topic, 0, sarama.OffsetOldest)
		err := subscribe(topic, mc)
		Expect(err).To(BeNil())
	})
	It("Should receive message which belongs to subscribed topic from a real producer", func() {
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
