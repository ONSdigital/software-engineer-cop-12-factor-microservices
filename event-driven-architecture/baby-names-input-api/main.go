package main

import (
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"github.com/Shopify/sarama"
)

var producer sarama.SyncProducer

type babyInput struct {
	Name string `json:"name"`
	Sex  string `json:"sex"`
}

func main() {
	bindAddr := ":8081"
	kafkaHost := "localhost:9092"

	if v := os.Getenv("BIND_ADDR"); len(v) > 0 {
		bindAddr = v
	}
	if v := os.Getenv("KAFKA_HOST"); len(v) > 0 {
		kafkaHost = v
	}

	var err error
	if producer, err = sarama.NewSyncProducer([]string{kafkaHost}, nil); err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	http.HandleFunc("/baby", newBaby)

	log.Printf("Listening on %s", bindAddr)
	if err := http.ListenAndServe(bindAddr, nil); err != nil {
		log.Fatal(err)
	}
}

func newBaby(w http.ResponseWriter, req *http.Request) {
	b, err := ioutil.ReadAll(req.Body)
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error()))
		return
	}

	msg := &sarama.ProducerMessage{Partition: 0, Topic: "baby", Value: sarama.ByteEncoder(b)}
	if _, _, err = producer.SendMessage(msg); err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	}

	w.WriteHeader(201)
}
