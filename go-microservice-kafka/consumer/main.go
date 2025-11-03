package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/segmentio/kafka-go"
)

func main() {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "localhost:9092"
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker},
		Topic:   "test-topic",
		GroupID: "group-1",
	})
	defer reader.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go handleShutdown(cancel)

	const workerCount = 3
	var wg sync.WaitGroup
	wg.Add(workerCount)

	log.Printf("🚀 Consumer started with %d workers...", workerCount)

	for w := 1; w <= workerCount; w++ {
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					log.Printf("🟡 Consumer worker #%d stopping gracefully...", id)
					return
				default:
					msg, err := reader.ReadMessage(ctx)
					if err != nil {
						log.Printf("❌ Worker #%d error reading message: %v", id, err)
						continue
					}
					log.Printf("✅ Worker #%d received: key=%s value=%s",
						id, string(msg.Key), string(msg.Value))
				}
			}
		}(w)
	}

	wg.Wait()
	log.Println("✅ All consumer workers stopped.")
}

func handleShutdown(cancel context.CancelFunc) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan
	cancel()
}
