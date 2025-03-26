package main

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

type KafkaWikimediaProducer struct{}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Idempotent = true
	config.Net.MaxOpenRequests = 1

	bootstrapAddrs := []string{"localhost:9092"}
	fmt.Println(fmt.Sprintf("%+v", config.Producer))

	producer, err := sarama.NewAsyncProducer(bootstrapAddrs, config)
	if err != nil {
		panic(err.Error())
	}
	defer func() {
		slog.InfoContext(ctx, "Shutting down producer gracefully")
		if err := producer.Close(); err != nil {
			slog.ErrorContext(ctx, "error shutting down producer", "details", err.Error())
		}
	}()

	url := "https://stream.wikimedia.org/v2/stream/recentchange"
	resp, err := http.Get(url)
	if err != nil {
		panic(err.Error())
	}

	defer resp.Body.Close()

	var (
		wg                                      = new(sync.WaitGroup)
		enqueued, producerErrs, producerSuccess int
		eventsChan                              = make(chan string)
		topic                                   = "wikimedia.recentchange"
	)

	scanner := bufio.NewScanner(resp.Body)

	wg.Add(1)
	go func() {
		defer close(eventsChan)
		defer wg.Done()

	MessageLoop:
		for {
			select {
			case <-ctx.Done():
				break MessageLoop
			default:
				if scanner.Scan() {
					if err := scanner.Err(); err != nil {
						panic(err)
					}
					if strings.Contains(scanner.Text(), "data:") {
						eventsChan <- scanner.Text()
					}
				}
			}
		}

		fmt.Println("done", "message loop")

	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

	ErrorsLoop:
		for {
			select {
			case <-ctx.Done():
				break ErrorsLoop
			case err := <-producer.Errors():
				slog.ErrorContext(ctx, "failed to produce message", "details", err.Error())
				producerErrs++
			}
		}

		fmt.Println("done", "errors loop")

	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

	SuccessLoop:
		for {
			select {
			case <-ctx.Done():
				break SuccessLoop
			case success := <-producer.Successes():
				slog.InfoContext(ctx, "success to produce message", "details", success)
				producerSuccess++
			}
		}

		fmt.Println("done", "success loop")

	}()

ProducerLoop:
	for {
		select {
		case <-ctx.Done():
			slog.ErrorContext(ctx, "context was cancelled")
			break ProducerLoop
		case <-signals:
			slog.ErrorContext(ctx, "shut down signal was detected")
			cancel()
			break ProducerLoop
		case msg := <-eventsChan:
			fmt.Println("=================\n", msg)
			producer.Input() <- &sarama.ProducerMessage{
				Topic: topic,
				Value: sarama.StringEncoder(msg),
			}
			enqueued++

		}
	}

	fmt.Println("done", "main loop")

	fmt.Println("TOTAL MESSAGES: ", enqueued)
	fmt.Println("TOTAL MESSAGES KAFKA: ", producerSuccess)
	fmt.Println("TOTAL ERROR KAFKA: ", producerErrs)

	wg.Wait()

}
