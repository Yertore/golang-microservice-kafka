package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "test-topic",
	})
	defer writer.Close()

	for i := 1; i <= 5; i++ {
		msg := fmt.Sprintf("Message #%d", i)
		err := writer.WriteMessages(context.Background(),
			kafka.Message{
				Key:   []byte(fmt.Sprintf("key-%d", i)),
				Value: []byte(msg),
			},
		)
		if err != nil {
			log.Fatalf("❌ Failed to write message: %v", err)
		}
		log.Printf("📤 Sent: %s", msg)
		time.Sleep(time.Second)
	}
}
