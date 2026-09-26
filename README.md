# connchurn

A diagnostic `http.RoundTripper` wrapper that tells you, per host, how many of
your HTTP connections were reused from the idle pool versus freshly dialed —
built on `net/http/httptrace` only.

## The problem

`http.DefaultMaxIdleConnsPerHost` is `2`. An unmodified `http.Client{}` keeps
at most two idle connections open per host. The moment you send even mildly
concurrent requests to one host, more than two connections finish at around
the same time, the transport closes the excess instead of holding them idle,
and the next round of requests has to pay for a fresh TCP handshake (and, for
HTTPS, a fresh TLS handshake) instead of reusing a warm connection.

Everyone "knows" the constant is low. Almost nobody measures how much churn
their own client is actually eating in production, and there is no drop-in
way to watch it happen. `connchurn` is that watch: wrap your transport with
it and ask, at any time, "how many of my connections to host X were new
dials versus reused?"

It is a **diagnostic, not a config nanny**. It never touches the
`http.RoundTripper` it wraps, never rewrites your `Transport`, and never
changes the request it forwards. It reports what is happening; fixing it
(raising `MaxIdleConnsPerHost`, adding a threshold alert to your own
monitoring, etc.) is still your call.

## Install

```sh
go get github.com/__GH_OWNER__/connchurn
```

Standard library only — no runtime dependencies. See `go.mod` for the minimum
Go version (developed and tested against 1.27.1).

## Usage

```go
transport := &http.Transport{} // or your own, already configured

tracker := connchurn.New(transport, connchurn.WithThreshold(
    0.5,  // bound: alert once >=50% of a host's connections are fresh dials
    50,   // minSamples: but not before we've seen at least 50 requests to it
    func(s connchurn.Stats) {
        log.Printf("connchurn: %s", s) // "api.example.com:443: new=31 reused=29 churn=51.7% (n=60)"
    },
))

client := &http.Client{Transport: tracker}

// ... use client normally ...

// At any time, ask what's going on:
log.Print(tracker)                     // multi-line report, all hosts
u, _ := url.Parse("https://api.example.com")
stats, ok := tracker.StatsFor(connchurn.HostKey(u))
fmt.Println(stats.ChurnRatio())        // 0.0 - 1.0
```

`Tracker` is safe for concurrent use — it is meant to sit inside an
`*http.Client` shared across goroutines.

## API

| Type / func | What it's for |
| --- | --- |
| `connchurn.New(next http.RoundTripper, opts ...Option) *Tracker` | Wrap a `RoundTripper`. `next == nil` uses `http.DefaultTransport`. |
| `connchurn.WithThreshold(bound float64, minSamples uint64, fn ThresholdFunc)` | Arms an edge-triggered alert: `fn` fires once a host has `minSamples` requests and its churn ratio crosses `bound` going upward. Fires once per crossing, not once per request. |
| `(*Tracker).Snapshot() []Stats` | Every host observed so far, sorted, as a point-in-time copy. |
| `(*Tracker).StatsFor(host string) (Stats, bool)` | Counters for one host key (see `HostKey`). |
| `(*Tracker).Reset()` | Clears all counters (keeps configuration). |
| `(*Tracker).String() string` | Multi-line, log-friendly report of every host. |
| `connchurn.HostKey(u *url.URL) string` | Normalizes a URL to the `scheme://host:port` key `Tracker` counts under. |
| `Stats{Host, New, Reused}` | `.Total()`, `.ChurnRatio()` (`New / (New+Reused)`), `.String()`. |

### Why keying is `scheme + host + port`, not just host

`http://example.com`, `http://example.com:80`, and `https://example.com` are
not the same connection pool. The first two are (port 80 is the implied
default for `http`, so they're normalized to the same key); the third is a
wholly separate, TLS-negotiated pool and is kept as a distinct key on
purpose — churn on your HTTP and HTTPS legs to the same hostname are
different problems with different fixes.

### Reused vs. new, and the idle pool

`httptrace.GotConnInfo.Reused` is exactly the signal for "was this connection
already idle, or did we just dial it": `true` means the transport handed back
a connection sitting in its idle pool; `false` means a fresh dial (and, for
HTTPS, a fresh handshake) happened for this request. `connchurn` tallies
directly off that field — `GotConnInfo.WasIdle` (further distinguishing *how*
idle a reused connection was) isn't surfaced, since it doesn't change the
new-vs-reused question this package answers.

### The one thing to get right when composing RoundTrippers

`httptrace` hooks are read off the request's `context.Context` by whichever
`http.Transport` actually performs the dial. `Tracker` attaches its hook to
the context and forwards the request unmodified — but if something sits
**between** `Tracker` and the real `*http.Transport` and drops that context
(some middleware builds a brand-new `*http.Request` instead of carrying the
incoming one's context forward), `Tracker`'s hook never runs, and its
counters silently stay at zero for those requests. `Tracker` has no way to
detect this — a wrapped RoundTripper that discards context is invisible to
it, by construction of how `httptrace` works. Put `Tracker` as close as
possible to the real `Transport` (ideally directly wrapping it). This is
covered by `TestTracker_ContextDroppedBetweenTrackerAndTransportIsInvisible`
in `churn_test.go`, which demonstrates the failure mode directly.

Conversely, if an *outer* RoundTripper (or the caller) also attaches its own
`httptrace.ClientTrace` to the context, `net/http/httptrace` composes them
automatically — both fire. `Tracker` never overwrites another trace; see
`TestTracker_ComposesWithExistingClientTrace`.

## Measurement caveat — read this before trusting any single number

`examples/demo` fires 5 successive bursts of 20 fully concurrent requests
(100 requests total) at a real `httptest.Server`, once with the default
`MaxIdleConnsPerHost` (2) and once tuned to 100, and reports how many
connections were new dials in each case.

**The exact counts move with goroutine scheduling, every run.** The
mechanism — how many connections happen to go idle at the same instant, and
which goroutine wins the race for a surviving idle connection — is
inherently non-deterministic. Below are 8 consecutive runs on this machine
(`go run ./examples/demo`, unmodified, back to back):

```
run  default new/100   tuned new/100
1         39               11
2         69               19
3         53               20
4         53               18
5         58               15
6         51               20
7         69               20
8         56               15
```

- **Default (`MaxIdleConnsPerHost = 2`): range 39-69 new dials per 100
  requests, median ~55.**
- **Tuned (`MaxIdleConnsPerHost = 100`): range 11-20 new dials per 100
  requests, median ~19.** The floor here is the first burst's width (20):
  with a large idle limit, only the very first burst has to dial fresh — but
  even that floor isn't exact, since goroutines within one burst don't start
  in perfect lockstep and a few can beat others to an idle connection freed
  moments earlier in the same burst.

The direction (tuned is always lower) and the rough magnitude (roughly
2.5-4x fewer fresh dials in this run of runs) are stable. The precise numbers
are not, and anyone telling you "the fix gives you exactly N%" without having
run it several times is guessing. **Run `go run ./examples/demo` yourself,
more than once, before trusting a number from it — including these.**

Raw output from one of the runs above, unedited:

```
workload: 5 rounds of 20 concurrent requests = 100 requests to one host

-- default MaxIdleConnsPerHost (0 => http.DefaultMaxIdleConnsPerHost == 2) --
new=53 reused=47 / 100

-- tuned MaxIdleConnsPerHost = 100 --
  [alert] http://127.0.0.1:52339: new=20 reused=0 churn=100.0% (n=20) crossed churn threshold
new=20 reused=80 / 100 (threshold alerts fired: 1)
```

The `[alert]` line is `connchurn`'s own threshold callback firing mid-run
(bound 0.30, minSamples 20) — proof the threshold API works against a real
transport, not just in a unit test.

## What it does not do

- **It does not change anything.** It does not raise `MaxIdleConnsPerHost`,
  retry requests, or rewrite your `Transport`. It only counts and reports.
- **It cannot see connection reuse it isn't told about.** If a RoundTripper
  between `Tracker` and the real `*http.Transport` drops the request context,
  `Tracker` sees nothing for that request and does not error — see "The one
  thing to get right" above. There is no way to detect this from inside
  `Tracker`.
- **It does not distinguish *why* a connection was new** — DNS change, TLS
  session ticket miss, the idle pool being empty, `DisableKeepAlives`, or a
  server-initiated close all look identical: one `GotConnInfo.Reused == false`
  sample. If you need to tell those apart, add other `httptrace` hooks (e.g.
  `DNSStart`/`TLSHandshakeStart`) alongside this package.
- **It is not a rate limiter, connection pool, or circuit breaker.** It has
  no opinion about what you should do when churn is high — that's your
  `ThresholdFunc`'s job, or your own monitoring.
- **It does not track HTTP/2 stream multiplexing separately from TCP
  connection reuse.** `GotConnInfo.Reused` reflects the underlying
  connection, which for HTTP/2 may already be multiplexing many logical
  requests — `connchurn` reports what `httptrace` reports, no more.
- **No persistence.** Counters live in memory for the life of the `Tracker`;
  restart your process and they reset. `Reset()` clears them manually if you
  want windowed reporting yourself.

## Name check

Checked `pkg.go.dev` for `connchurn` before starting: zero existing modules
match. No well-established incumbent does exactly this (an `httptrace`-based,
per-host, new-vs-reused connection counter with a threshold callback) — there
are general HTTP client metrics/middleware packages, but not this specific,
narrow diagnostic.

## Develop

```sh
go build ./...
go vet ./...
go test -race -count=1 ./...
gofmt -l .          # must print nothing
go run ./examples/demo
```

## License

MIT — see `LICENSE`.
