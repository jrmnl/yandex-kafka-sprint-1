package main

import (
	"context"
	"log"
	"os"
	"os/signal"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt)

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":  "host.docker.internal:9092",
		"message.timeout.ms": 10000,
	})
	if err != nil {
		log.Fatalf("Ошибка при отправке сообщения: %v\n", err)
	}

	topic := "test-topic"
	deliveryChan := make(chan kafka.Event)
	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          make([]byte, 120),
	}, deliveryChan)

	if err != nil {
		log.Fatalf("Ошибка при отправке сообщения: %v\n", err)
	}

	event := <-deliveryChan
	// Приводим Events к типу *kafka.Message
	msg := event.(*kafka.Message)

	if msg.TopicPartition.Error != nil {
		log.Fatalf("Ошибка доставки сообщения: %v\n", msg.TopicPartition.Error)
	} else {
		log.Printf("Сообщение отправлено в топик %s [%d] офсет %v\n",
			*msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset)
	}

	log.Println("жду ктрл+ц")
	<-ctx.Done()
	log.Println("готово")
}
