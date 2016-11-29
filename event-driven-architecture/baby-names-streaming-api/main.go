package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/Shopify/sarama"
)

var kafkaHost string

func main() {
	bindAddr := ":8082"
	kafkaHost = "localhost:9092"

	if v := os.Getenv("BIND_ADDR"); len(v) > 0 {
		bindAddr = v
	}
	if v := os.Getenv("KAFKA_HOST"); len(v) > 0 {
		kafkaHost = v
	}

	http.HandleFunc("/stream", streamHandler)

	log.Printf("Listening on %s", bindAddr)
	if err := http.ListenAndServe(bindAddr, nil); err != nil {
		log.Fatal(err)
	}
}

func streamHandler(w http.ResponseWriter, req *http.Request) {
	offset, _ := strconv.ParseInt(req.URL.Query().Get("offset"), 10, 64)

	consumer, err := sarama.NewConsumer([]string{kafkaHost}, nil)
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	}

	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition("baby", 0, offset)
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	}

	defer partitionConsumer.Close()
	w.WriteHeader(200)

	for {
		select {
		case msg := <-partitionConsumer.Messages():
			w.Write([]byte(fmt.Sprintf("%d:", msg.Offset)))
			w.Write(msg.Value)
			w.Write([]byte("\n"))
			w.(http.Flusher).Flush()
		}
	}
}
