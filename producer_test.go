package main

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Producer test", func() {
	var (
		sp *mocks.SyncProducer
		t  *testing.T
	)
	BeforeEach(func() {
		sp = mocks.NewSyncProducer(t, nil)
	})
	AfterEach(func() {
		err := sp.Close()
		Expect(err).To(BeNil())
		brokers = []string{"127.0.0.1:9092"}
	})
	It("Should be able to create new producer", func() {
		_, err := newProducer()
		Expect(err).To(BeNil())
	})
	It("Should return error kafka cluster unreachable when IP is wrong ", func() {
		brokers = []string{"someIPaddress"}
		_, err := newProducer()
		Expect(err).To(Equal(errUnreachable))
	})
	It("Should be able to prepare message", func() {
		msg := prepareMessage("testing", "Just another test message")
		Expect(msg.Topic).To(Equal("testing"))
		Expect(msg.Value).To(Equal(sarama.StringEncoder("Just another test message")))
	})
	It("Should send message and succeed", func() {
		sp.ExpectSendMessageAndSucceed()

		msg := prepareMessage("testing", "Just another test message")
		partition, offset, err := sp.SendMessage(msg)
		Expect(partition).To(Equal(int32(0)))
		Expect(offset).To(Equal(int64(1)))
		Expect(err).To(BeNil())
	})
	It("Should return error broker unreachable", func() {
		sp.ExpectSendMessageAndFail(sarama.ErrOutOfBrokers)

		msg := &sarama.ProducerMessage{
			Topic: "test",
			Value: sarama.StringEncoder("This is test message"),
		}

		_, _, err := sp.SendMessage(msg)
		Expect(err).To(Equal(sarama.ErrOutOfBrokers))
	})

})
