package main

import (
	"fmt"
	"html"
	"log"
	"net/http"
)

const topic = "sample-topic"
const testTopic = "test"

func main() {
	fmt.Println("This is running")
	producer, err := newProducer()
	if err != nil {
		fmt.Println("Could not create producer: ", err)
	}

	consumer, err := newConsumer(brokers, nil)
	if err != nil {
		fmt.Println("Could not create consumer: ", err)
	}

	_ = subscribe(testTopic, consumer)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "Hello Sarama!") })

	http.HandleFunc("/save", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		r.ParseForm()
		msg := prepareMessage(testTopic, r.FormValue("q"), 1)
		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			fmt.Fprintf(w, "%s error occured.", err.Error())
		}
		fmt.Fprintf(w, "Message: %s.\nSaved to partion: %d.\nMessage offset is: %d", getMessage(), partition, offset)
	})

	http.HandleFunc("/retrieve", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, html.EscapeString(getMessage())) })

	log.Fatal(http.ListenAndServe(":8081", nil))
}
