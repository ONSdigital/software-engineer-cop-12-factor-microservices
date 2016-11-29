package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"reflect"
	"time"

	_ "github.com/lib/pq"

	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kafka/consumergroup"
	"github.com/wvanbergen/kazoo-go"
)

var db *sql.DB
var producer sarama.SyncProducer
var response *top10Response

type top10Response struct {
	Boys  []top10ResponseEntry `json:"boys"`
	Girls []top10ResponseEntry `json:"girls"`
}

type top10ResponseEntry struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

type babyInput struct {
	Name string `json:"name"`
	Sex  string `json:"sex"`
}

func main() {
	connect := "postgres://postgres:password@localhost/babynames?sslmode=disable"
	zkHost := "localhost:2181"
	kafkaHost := "localhost:9092"

	if v := os.Getenv("DB_CONNECT"); len(v) > 0 {
		connect = v
	}
	if v := os.Getenv("ZK_HOST"); len(v) > 0 {
		zkHost = v
	}
	if v := os.Getenv("KAFKA_HOST"); len(v) > 0 {
		kafkaHost = v
	}

	var err error
	db, err = sql.Open("postgres", connect)
	if err != nil {
		log.Fatal(err)
	}

	if producer, err = sarama.NewSyncProducer([]string{kafkaHost}, nil); err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	checkTop10()
	startKafka(zkHost)
}

func startKafka(zookeeperHost string) {
	config := consumergroup.NewConfig()
	config.Offsets.Initial = sarama.OffsetNewest
	config.Offsets.ProcessingTimeout = 10 * time.Second

	var zookeeperNodes []string
	zookeeperNodes, config.Zookeeper.Chroot = kazoo.ParseConnectionString(zookeeperHost)

	consumer, err := consumergroup.JoinConsumerGroup("cg1", []string{"baby"}, zookeeperNodes, config)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		for err := range consumer.Errors() {
			log.Println("ERROR:", err.Error())
		}
	}()

	for msg := range consumer.Messages() {
		newBaby(msg.Value)
		consumer.CommitUpto(msg)
	}
}

func newBaby(b []byte) {
	var input babyInput
	err := json.Unmarshal(b, &input)
	if err != nil {
		log.Println("ERROR:", err.Error())
		return
	}

	stmt, err := db.Prepare("INSERT INTO baby_names (name, sex, total) VALUES ($1, $2, 1) ON CONFLICT (name, sex) DO UPDATE SET total = baby_names.total + 1;")
	if err != nil {
		log.Println("ERROR:", err.Error())
		return
	}

	if _, err = stmt.Exec(input.Name, input.Sex); err != nil {
		log.Println("ERROR:", err.Error())
		return
	}

	checkTop10()
}

func checkTop10() {
	newResponse, err := getResponse()
	if err != nil {
		log.Println("ERROR:", err.Error())
		return
	}

	if response == nil || !reflect.DeepEqual(response, newResponse) {
		msg := &sarama.ProducerMessage{Partition: 0, Topic: "top10changed", Value: sarama.StringEncoder("updated")}
		_, _, err := producer.SendMessage(msg)
		if err != nil {
			log.Println("ERROR:", err.Error())
		}
	}
}

func getResponse() (response top10Response, err error) {
	response.Boys, err = getTop10("male")
	if err != nil {
		return
	}

	response.Girls, err = getTop10("female")
	return
}

func getTop10(sex string) (entries []top10ResponseEntry, err error) {
	var rows *sql.Rows
	if rows, err = db.Query("SELECT name, total FROM baby_names WHERE sex=$1 ORDER BY total DESC LIMIT 10", sex); err != nil {
		return
	}

	defer rows.Close()
	for rows.Next() {
		var entry top10ResponseEntry
		if err = rows.Scan(&entry.Name, &entry.Count); err != nil {
			return
		}
		entries = append(entries, entry)
	}

	err = rows.Err()
	return
}
