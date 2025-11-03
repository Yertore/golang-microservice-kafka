package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "localhost:9092"
	}

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{broker},
		Topic:    "test-topic",
		Balancer: &kafka.LeastBytes{},
	})
	defer writer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go handleShutdown(cancel)

	const workerCount = 3
	var wg sync.WaitGroup
	wg.Add(workerCount)

	log.Printf("🚀 Producer started with %d workers...", workerCount)

	for w := 1; w <= workerCount; w++ {
		go func(id int) {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-ctx.Done():
					log.Printf("🟡 Worker #%d stopping gracefully...", id)
					return
				default:
					i++
					msg := fmt.Sprintf("[Worker %d] Message #%d", id, i)
					err := writer.WriteMessages(ctx, kafka.Message{
						Key:   []byte(fmt.Sprintf("key-%d-%d", id, i)),
						Value: []byte(msg),
					})
					if err != nil {
						log.Printf("❌ Worker #%d failed to send: %v", id, err)
						time.Sleep(time.Second)
						continue
					}
					log.Printf("📤 Worker #%d sent: %s", id, msg)
					time.Sleep(2 * time.Second)
				}
			}
		}(w)
	}

	wg.Wait()
	log.Println("✅ All producer workers stopped.")
}

func handleShutdown(cancel context.CancelFunc) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan
	cancel()
}
