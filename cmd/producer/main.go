package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	fmt.Println("Starting server")
}

func NewKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"boostrap.servers": "kafka-kafka-1:9092",
	}

	p, err := kafka.NewProducer(configMap)
	if err != nil {
		log.Println(err.Error())
	}
	return p
}
