// Command demo measures the connection churn described in the connchurn
// README: of `rounds` successive bursts of `burstWidth` fully concurrent
// requests to a single host, how many had to dial a brand new connection
// versus how many found one in the idle pool — compared between a client
// left at the default MaxIdleConnsPerHost (2) and one tuned up.
//
// Run it several times; the numbers move with goroutine scheduling. That is
// the point: see README.md's "Measurement caveat" section.
package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"

	"github.com/agentscope-ai-java/connchurn"
)

const (
	burstWidth = 20 // concurrent requests fired at once, per round
	rounds     = 5  // rounds, run back to back on the same *http.Client
	total      = burstWidth * rounds
)

func main() {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	fmt.Printf("workload: %d rounds of %d concurrent requests = %d requests to one host\n\n", rounds, burstWidth, total)

	fmt.Println("-- default MaxIdleConnsPerHost (0 => http.DefaultMaxIdleConnsPerHost == 2) --")
	defaultNew, defaultTotal := run(srv.URL, 0, nil)
	fmt.Printf("new=%d reused=%d / %d\n\n", defaultNew, defaultTotal-defaultNew, defaultTotal)

	fmt.Println("-- tuned MaxIdleConnsPerHost = 100 --")
	var alerts int
	onThreshold := func(s connchurn.Stats) {
		alerts++
		fmt.Fprintf(os.Stderr, "  [alert] %s crossed churn threshold\n", s)
	}
	tunedNew, tunedTotal := run(srv.URL, 100, onThreshold)
	fmt.Printf("new=%d reused=%d / %d (threshold alerts fired: %d)\n", tunedNew, tunedTotal-tunedNew, tunedTotal, alerts)
}

// run fires `rounds` successive bursts of `burstWidth` fully concurrent
// requests at target, on one shared *http.Client, using a transport with the
// given MaxIdleConnsPerHost (0 means "leave it at the http.Transport zero
// value", which net/http interprets as http.DefaultMaxIdleConnsPerHost == 2).
// It returns (new connections, total requests) for that host as seen by a
// connchurn.Tracker wrapping the transport.
//
// Bursts, not a tight per-goroutine loop, are what actually exercises
// MaxIdleConnsPerHost: when burstWidth requests finish at roughly the same
// moment, burstWidth connections go idle at once. Only MaxIdleConnsPerHost of
// them survive; the rest are closed immediately. The next burst then finds
// only that many idle connections waiting, and must dial fresh for the rest.
// A single long-lived goroutine that loops on its own connection would just
// keep reusing it and never expose this.
func run(target string, maxIdlePerHost int, onThreshold connchurn.ThresholdFunc) (newConns, totalReqs uint64) {
	transport := &http.Transport{MaxIdleConnsPerHost: maxIdlePerHost}

	var opts []connchurn.Option
	if onThreshold != nil {
		// Fire once a host has at least 20 samples and >=30% of its
		// connections were fresh dials.
		opts = append(opts, connchurn.WithThreshold(0.30, 20, onThreshold))
	}
	tracker := connchurn.New(transport, opts...)
	client := &http.Client{Transport: tracker}

	for r := 0; r < rounds; r++ {
		var wg sync.WaitGroup
		for i := 0; i < burstWidth; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				resp, err := client.Get(target)
				if err != nil {
					fmt.Fprintf(os.Stderr, "request error: %v\n", err)
					return
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}()
		}
		wg.Wait() // let this burst fully settle before the next one starts
	}

	for _, s := range tracker.Snapshot() {
		newConns += s.New
		totalReqs += s.Total()
	}
	return newConns, totalReqs
}
