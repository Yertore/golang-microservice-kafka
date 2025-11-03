package main

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
)

func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "test-topic",
		GroupID: "group-1",
	})
	defer reader.Close()

	log.Println("🚀 Consumer listening for messages...")
	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatalf("❌ Error reading message: %v", err)
		}
		log.Printf("✅ Received: key=%s value=%s", string(msg.Key), string(msg.Value))
	}
}
