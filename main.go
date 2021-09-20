package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"

	sarama "github.com/Shopify/sarama"
	gen "github.com/brianvoe/gofakeit/v5"
	"github.com/google/uuid"
	flags "github.com/jessevdk/go-flags"
)

var opts struct {
	Count      int    `short:"c" long:"count" env:"COUNT" default:"100" description:"count of records"`
	Topic      string `short:"t" long:"topic" env:"QUEUE" default:"output" description:"name of the topic"`
	Broker     string `short:"b" long:"broker" env:"BROKER" default:"localhost:9092" description:"Kafka brocker"`
	SilentMode string `short:"s" long:"silent" env:"SILENT" default:"false" description:"Silent mode"`
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	if _, err := flags.Parse(&opts); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	brokerList := []string{opts.Broker}
	topic := opts.Topic

	config := sarama.NewConfig()

	config.Version = sarama.MaxVersion

	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true

	syncProducer, err := sarama.NewSyncProducer(brokerList, config)
	failOnError(err, "Failed to create producer")
	defer syncProducer.Close()

	for i := 1; i <= opts.Count; i++ {
		data, err := generateData()
		failOnError(err, "Failed to generate data")

		key, _ := uuid.NewUUID()

		partition, offset, err := syncProducer.SendMessage(&sarama.ProducerMessage{
			Key:   sarama.ByteEncoder(key.String()),
			Topic: topic,
			Value: sarama.ByteEncoder(data),
		})
		failOnError(err, "Failed to send message")

		if opts.SilentMode == "false" {
			log.Printf("Wrote message at partition: %d, offset: %d\n", partition, offset)
		}

	}

}

func generateData() ([]byte, error) {
	gen.Seed(rand.Int63())

	var value, err = gen.JSON(&gen.JSONOptions{
		Type: "object",
		Fields: []gen.Field{
			{Name: "first_name", Function: "firstname"},
			{Name: "last_name", Function: "lastname"},
			{Name: "address", Function: "address"},
			{Name: "password", Function: "password", Params: map[string][]string{"special": {"false"}}},
		},
		Indent: true,
	})
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	return value, nil
}
