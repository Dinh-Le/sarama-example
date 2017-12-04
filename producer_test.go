package main

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Producer", func() {
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
	})
	It("Should send message and succeed", func() {
		sp.ExpectSendMessageAndSucceed()

		msg := &sarama.ProducerMessage{
			Topic: "test",
			Value: sarama.StringEncoder("This is test message"),
		}
		_, _, err := sp.SendMessage(msg)
		Expect(err).To(BeNil())
	})
})
