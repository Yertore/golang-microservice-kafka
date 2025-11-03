package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
)

var (
	messagesConsumed = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "consumer_messages_consumed_total",
			Help: "Total number of messages consumed by consumer",
		},
	)
	errorsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "consumer_errors_total",
			Help: "Total number of consumer read errors",
		},
	)
)

func init() {
	prometheus.MustRegister(messagesConsumed)
	prometheus.MustRegister(errorsTotal)
}

func main() {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "localhost:9092"
	}

	// Start Prometheus metrics server
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Println("📊 Consumer metrics on :2113/metrics")
		if err := http.ListenAndServe(":2113", nil); err != nil {
			log.Fatalf("❌ Metrics server failed: %v", err)
		}
	}()

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
						errorsTotal.Inc()
						log.Printf("❌ Worker #%d error reading message: %v", id, err)
						continue
					}
					messagesConsumed.Inc()
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
