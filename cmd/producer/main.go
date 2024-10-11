package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	fmt.Println("Starting server")
	producer := NewKafkaProducer()
	Publish("mensagem", "teste", producer, nil)

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

func Publish(msg string, topic string, producer *kafka.Producer, key []byte) error {
	message := &kafka.Message{
		Value:          []byte(msg),
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
	}

	err := producer.Produce(message, nil)
	if err != nil {
		return err
	}

	return nil

}
