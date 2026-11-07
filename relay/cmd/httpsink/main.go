// httpsink is the simulated Pub/Sub for the debezium transport. Debezium Server's HTTP
// sink posts batches of routed outbox events here; this counts them, charges the same
// fixed latency per batch that the Go relays' simulated sink charges, and answers 200.
// It logs the running total in the same form as the relays so the benchmark harness
// can read it.
package main

import (
	"encoding/json"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	addr := flag.String("addr", envOr("SINK_ADDR", ":8080"), "listen address")
	latency := flag.Duration("sink-latency", envDurationOr("RELAY_SINK_LATENCY", 200*time.Millisecond), "cost of one publish batch")
	flag.Parse()

	var total atomic.Int64
	var once sync.Once
	http.HandleFunc("/publish", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		n := count(body)
		once.Do(func() { log.Printf("first request: %d events, headers=%v body[:200]=%.200s", n, r.Header, body) })
		select {
		case <-time.After(*latency):
		case <-r.Context().Done():
			return
		}
		t := total.Add(int64(n))
		log.Printf("published %d (debezium) total=%d", n, t)
		w.WriteHeader(http.StatusOK)
	})
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	log.Printf("relaying debezium http sink on %s: %s per publish batch, nothing is sent", *addr, *latency)
	log.Fatal(http.ListenAndServe(*addr, nil))
}

// count returns how many events a request body carries: the length of a JSON array in
// batch mode, otherwise one.
func count(body []byte) int {
	var arr []json.RawMessage
	if err := json.Unmarshal(body, &arr); err == nil {
		return len(arr)
	}
	return 1
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func envDurationOr(k string, d time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if p, err := time.ParseDuration(v); err == nil {
			return p
		}
	}
	return d
}
