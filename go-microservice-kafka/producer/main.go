package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
)

var (
	messagesSent = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "producer_messages_sent_total",
			Help: "Total number of messages sent by producer",
		},
	)
	errorsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "producer_errors_total",
			Help: "Total number of send errors",
		},
	)
)

func init() {
	prometheus.MustRegister(messagesSent)
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
		log.Println("📊 Producer metrics on :2112/metrics")
		if err := http.ListenAndServe(":2112", nil); err != nil {
			log.Fatalf("❌ Metrics server failed: %v", err)
		}
	}()

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
						errorsTotal.Inc()
						log.Printf("❌ Worker #%d failed to send: %v", id, err)
						time.Sleep(time.Second)
						continue
					}
					messagesSent.Inc()
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
