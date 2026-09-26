package connchurn

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/url"
	"sync"
	"testing"
)

func TestHostKey(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{"http no port", "http://example.com/path", "http://example.com:80"},
		{"http explicit default port", "http://example.com:80/path", "http://example.com:80"},
		{"https no port", "https://example.com/path", "https://example.com:443"},
		{"https explicit default port", "https://example.com:443/path", "https://example.com:443"},
		{"non-default port kept", "http://example.com:8080/path", "http://example.com:8080"},
		{"http and https differ", "http://example.com/path", "http://example.com:80"},
		{"ipv4 with port", "http://127.0.0.1:9000/x", "http://127.0.0.1:9000"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, err := url.Parse(tt.raw)
			if err != nil {
				t.Fatalf("parse %q: %v", tt.raw, err)
			}
			got := HostKey(u)
			if got != tt.want {
				t.Errorf("HostKey(%q) = %q, want %q", tt.raw, got, tt.want)
			}
		})
	}
}

func TestHostKey_SchemeSeparatesSameHost(t *testing.T) {
	h1, _ := url.Parse("http://example.com/")
	h2, _ := url.Parse("https://example.com/")
	if HostKey(h1) == HostKey(h2) {
		t.Fatalf("http and https to the same host should not collapse to one key, got %q for both", HostKey(h1))
	}
}

// newTestServer returns an httptest server that responds immediately, and a
// cleanup func.
func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func drain(t *testing.T, resp *http.Response) {
	t.Helper()
	_, _ = io.Copy(io.Discard, resp.Body)
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("close body: %v", err)
	}
}

func TestTracker_FirstRequestIsNew(t *testing.T) {
	srv := newTestServer(t)

	tr := New(&http.Transport{})
	client := &http.Client{Transport: tr}

	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	drain(t, resp)

	u, _ := url.Parse(srv.URL)
	stats, ok := tr.StatsFor(HostKey(u))
	if !ok {
		t.Fatalf("no stats recorded for %s", HostKey(u))
	}
	if stats.New != 1 || stats.Reused != 0 {
		t.Fatalf("first request: got New=%d Reused=%d, want New=1 Reused=0", stats.New, stats.Reused)
	}
}

func TestTracker_SecondSequentialRequestIsReused(t *testing.T) {
	srv := newTestServer(t)

	// A single Transport with default keep-alive behavior, hit sequentially
	// by one client: the second request should find the first connection
	// idle and reuse it.
	tr := New(&http.Transport{})
	client := &http.Client{Transport: tr}

	for i := 0; i < 2; i++ {
		resp, err := client.Get(srv.URL)
		if err != nil {
			t.Fatalf("GET %d: %v", i, err)
		}
		drain(t, resp)
	}

	u, _ := url.Parse(srv.URL)
	stats, ok := tr.StatsFor(HostKey(u))
	if !ok {
		t.Fatalf("no stats recorded")
	}
	if stats.New != 1 || stats.Reused != 1 {
		t.Fatalf("two sequential requests: got New=%d Reused=%d, want New=1 Reused=1 (%s)", stats.New, stats.Reused, stats)
	}
}

func TestTracker_DisableKeepAlivesForcesAllNew(t *testing.T) {
	srv := newTestServer(t)

	tr := New(&http.Transport{DisableKeepAlives: true})
	client := &http.Client{Transport: tr}

	const n = 5
	for i := 0; i < n; i++ {
		resp, err := client.Get(srv.URL)
		if err != nil {
			t.Fatalf("GET %d: %v", i, err)
		}
		drain(t, resp)
	}

	u, _ := url.Parse(srv.URL)
	stats, _ := tr.StatsFor(HostKey(u))
	if stats.New != n || stats.Reused != 0 {
		t.Fatalf("with DisableKeepAlives: got New=%d Reused=%d, want New=%d Reused=0", stats.New, stats.Reused, n)
	}
	if got := stats.ChurnRatio(); got != 1.0 {
		t.Fatalf("ChurnRatio = %v, want 1.0", got)
	}
}

// TestTracker_ConcurrentRequests exercises Tracker under real concurrency
// against a real httptest.Server, run with -race to catch data races in the
// counters. It only asserts the invariant that must always hold regardless
// of scheduling: every completed request is tallied exactly once, as either
// new or reused.
func TestTracker_ConcurrentRequests(t *testing.T) {
	srv := newTestServer(t)

	tr := New(&http.Transport{MaxIdleConnsPerHost: 2})
	client := &http.Client{Transport: tr}

	const workers = 10
	const perWorker = 10
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				resp, err := client.Get(srv.URL)
				if err != nil {
					t.Errorf("GET: %v", err)
					return
				}
				drain(t, resp)
			}
		}()
	}
	wg.Wait()

	u, _ := url.Parse(srv.URL)
	stats, ok := tr.StatsFor(HostKey(u))
	if !ok {
		t.Fatalf("no stats recorded")
	}
	want := uint64(workers * perWorker)
	if stats.Total() != want {
		t.Fatalf("Total() = %d, want %d (New=%d Reused=%d)", stats.Total(), want, stats.New, stats.Reused)
	}
	if stats.New == 0 {
		t.Fatalf("expected at least one new connection under MaxIdleConnsPerHost=2 with %d concurrent workers, got New=0", workers)
	}
}

func TestTracker_ThresholdFiresOncePerCrossing(t *testing.T) {
	srv := newTestServer(t)

	var fired int
	var lastStats Stats
	var mu sync.Mutex

	const minSamples = 5
	tr := New(&http.Transport{DisableKeepAlives: true}, WithThreshold(0.9, minSamples, func(s Stats) {
		mu.Lock()
		defer mu.Unlock()
		fired++
		lastStats = s
	}))
	client := &http.Client{Transport: tr}

	// DisableKeepAlives forces every request to be a new connection, so the
	// churn ratio is 1.0 from the very first sample. The callback must not
	// fire before minSamples requests have landed, and after that it must
	// fire exactly once (it stays above the bound forever, since every
	// request here is a fresh dial).
	const total = 12
	for i := 0; i < total; i++ {
		resp, err := client.Get(srv.URL)
		if err != nil {
			t.Fatalf("GET %d: %v", i, err)
		}
		drain(t, resp)
	}

	mu.Lock()
	defer mu.Unlock()
	if fired != 1 {
		t.Fatalf("threshold callback fired %d times, want exactly 1", fired)
	}
	if lastStats.Total() < minSamples {
		t.Fatalf("callback fired with only %d samples, want >= %d", lastStats.Total(), minSamples)
	}
}

func TestTracker_ThresholdDoesNotFireBelowMinSamples(t *testing.T) {
	srv := newTestServer(t)

	var fired int
	var mu sync.Mutex

	tr := New(&http.Transport{DisableKeepAlives: true}, WithThreshold(0.1, 1000, func(s Stats) {
		mu.Lock()
		defer mu.Unlock()
		fired++
	}))
	client := &http.Client{Transport: tr}

	// One single request: even though the ratio (1.0) is far above bound
	// (0.1), minSamples (1000) has not been reached, so it must not fire.
	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	drain(t, resp)

	mu.Lock()
	defer mu.Unlock()
	if fired != 0 {
		t.Fatalf("threshold fired on a single sample below minSamples, fired=%d", fired)
	}
}

func TestTracker_ComposesWithExistingClientTrace(t *testing.T) {
	srv := newTestServer(t)

	tr := New(&http.Transport{})

	var outerCalled bool
	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	ctx := httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{
		GotConn: func(httptrace.GotConnInfo) {
			outerCalled = true
		},
	})
	req = req.WithContext(ctx)

	resp, err := tr.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip: %v", err)
	}
	drain(t, resp)

	if !outerCalled {
		t.Fatal("pre-existing ClientTrace on the request context was not invoked; Tracker must compose, not overwrite")
	}

	u, _ := url.Parse(srv.URL)
	stats, ok := tr.StatsFor(HostKey(u))
	if !ok || stats.Total() != 1 {
		t.Fatalf("Tracker's own counters did not also record the request: stats=%+v ok=%v", stats, ok)
	}
}

func TestTracker_ContextDroppedBetweenTrackerAndTransportIsInvisible(t *testing.T) {
	// This test documents a real limitation rather than a bug: httptrace
	// hooks are read off the request's context by the *http.Transport that
	// actually performs the dial. If something sits BETWEEN Tracker and that
	// real Transport and drops the context Tracker attached (as naive
	// middleware sometimes does, e.g. by building a brand new *http.Request
	// instead of carrying req's context forward), Tracker's GotConn hook
	// never runs and its counters silently stay at zero. Tracker has no way
	// to detect that this has happened.
	srv := newTestServer(t)

	realTransport := &http.Transport{}
	// contextDroppingMiddleware sits between Tracker and the real
	// Transport, and forgets to carry the context Tracker attached forward.
	contextDroppingMiddleware := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		fresh, err := http.NewRequest(req.Method, req.URL.String(), nil)
		if err != nil {
			return nil, err
		}
		return realTransport.RoundTrip(fresh)
	})

	tr := New(contextDroppingMiddleware)
	client := &http.Client{Transport: tr}

	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	drain(t, resp)

	if snap := tr.Snapshot(); len(snap) != 0 {
		t.Fatalf("expected no stats recorded once the context Tracker attached is dropped before reaching the real Transport, got %v", snap)
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestStats_ChurnRatioAndString(t *testing.T) {
	s := Stats{Host: "http://example.com:80", New: 3, Reused: 7}
	if got, want := s.Total(), uint64(10); got != want {
		t.Fatalf("Total() = %d, want %d", got, want)
	}
	if got, want := s.ChurnRatio(), 0.3; got != want {
		t.Fatalf("ChurnRatio() = %v, want %v", got, want)
	}
	if str := s.String(); str == "" {
		t.Fatal("String() returned empty string")
	}

	var zero Stats
	if got := zero.ChurnRatio(); got != 0 {
		t.Fatalf("ChurnRatio() of empty Stats = %v, want 0", got)
	}
}

func TestTracker_StringAndReset(t *testing.T) {
	srv := newTestServer(t)

	tr := New(&http.Transport{})
	if got := tr.String(); got == "" {
		t.Fatal("String() on an empty Tracker returned empty string")
	}

	client := &http.Client{Transport: tr}
	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	drain(t, resp)

	if len(tr.Snapshot()) == 0 {
		t.Fatal("expected at least one host recorded after a request")
	}

	tr.Reset()
	if len(tr.Snapshot()) != 0 {
		t.Fatal("Reset() did not clear recorded stats")
	}
}

func TestNew_NilTransportDefaultsToDefaultTransport(t *testing.T) {
	srv := newTestServer(t)

	tr := New(nil)
	client := &http.Client{Transport: tr}
	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET with nil-wrapped Tracker: %v", err)
	}
	drain(t, resp)

	if len(tr.Snapshot()) != 1 {
		t.Fatalf("expected exactly one host recorded, got %v", tr.Snapshot())
	}
}
