// Package connchurn is a diagnostic http.RoundTripper wrapper that measures
// connection reuse versus new-dial churn per host, using net/http/httptrace.
//
// Go's http.Transport defaults DefaultMaxIdleConnsPerHost to 2. A client that
// sends even mildly concurrent requests to one host will frequently find no
// idle connection available and pay for a fresh TCP (and, for https, TLS)
// handshake instead of reusing one. Most people know the constant is low;
// almost nobody measures how much churn their own client is actually eating
// in production. Tracker answers that question: wrap a Transport with it and
// ask it, at any time, "how many of my connections to host X were new dials
// versus reused?"
//
// Tracker is a diagnostic, not a config nanny. It never touches the
// http.RoundTripper it wraps or the *http.Request it forwards; it only
// counts.
package connchurn

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// Stats is a point-in-time snapshot of the connection counters for one host.
type Stats struct {
	// Host is the scheme://host:port key the counters were recorded under.
	// See HostKey for exactly how a request URL is normalized into this form.
	Host string
	// New is the number of RoundTrips for this host that dialed a brand new
	// connection (httptrace reported GotConnInfo.Reused == false).
	New uint64
	// Reused is the number of RoundTrips for this host that were served by a
	// connection taken out of the idle pool (GotConnInfo.Reused == true).
	Reused uint64
}

// Total returns the number of samples (New + Reused) recorded for the host.
func (s Stats) Total() uint64 { return s.New + s.Reused }

// ChurnRatio returns New / (New + Reused), the fraction of connections for
// this host that had to be freshly dialed rather than reused. It returns 0
// when Total is 0.
func (s Stats) ChurnRatio() float64 {
	total := s.Total()
	if total == 0 {
		return 0
	}
	return float64(s.New) / float64(total)
}

// String formats Stats as a single log-friendly line, e.g.:
//
//	example.com:443: new=41 reused=59 churn=41.0% (n=100)
func (s Stats) String() string {
	return fmt.Sprintf("%s: new=%d reused=%d churn=%.1f%% (n=%d)",
		s.Host, s.New, s.Reused, s.ChurnRatio()*100, s.Total())
}

// hostStats holds the live, concurrency-safe counters for one host plus the
// edge-detection state used for threshold callbacks.
type hostStats struct {
	host   string
	new    atomic.Uint64
	reused atomic.Uint64
	// over records whether this host was already above the configured
	// threshold the last time it was checked, so ThresholdFunc fires once
	// per crossing rather than once per request while it stays above bound.
	over atomic.Bool
}

func (hs *hostStats) snapshot() Stats {
	return Stats{Host: hs.host, New: hs.new.Load(), Reused: hs.reused.Load()}
}

// ThresholdFunc is invoked when a host's churn ratio crosses Bound (see
// WithThreshold) after at least MinSamples requests have been recorded for
// that host. It fires once per crossing: it will not fire again for the same
// host until the ratio has dropped back below Bound and crossed it again.
// ThresholdFunc must not block for long and must not call back into the
// Tracker that invoked it while holding external locks that Tracker's own
// callers might depend on; it runs synchronously on the goroutine that
// completed the triggering RoundTrip.
type ThresholdFunc func(Stats)

// Tracker wraps an http.RoundTripper and tallies, per host, how many
// connections were reused from the idle pool versus freshly dialed. All
// methods are safe for concurrent use, which matters because Tracker is
// designed to sit inside an *http.Client's Transport and see one goroutine
// per in-flight request.
//
// Tracker only observes; it never modifies the request it forwards and never
// changes the wrapped RoundTripper's behavior (no Transport field is read or
// written). If you want fewer new dials, raise MaxIdleConnsPerHost on your
// own Transport — Tracker will simply report the result.
type Tracker struct {
	next http.RoundTripper

	hosts sync.Map // string host key -> *hostStats

	onThreshold ThresholdFunc
	bound       float64
	minSamples  uint64
}

// Option configures a Tracker at construction time.
type Option func(*Tracker)

// WithThreshold arms a threshold callback: once a host has recorded at least
// minSamples requests (guarding against firing on a single unlucky request),
// fn is invoked every time that host's churn ratio crosses bound going
// upward. bound is a fraction in [0,1]; for example 0.5 means "half of this
// host's connections are being freshly dialed."
//
// Passing minSamples == 0 is treated as 1, since a threshold decision needs
// at least one sample, but the whole point of this option is to require more
// than that — callers should pass a realistic floor (10, 50, ...).
func WithThreshold(bound float64, minSamples uint64, fn ThresholdFunc) Option {
	return func(t *Tracker) {
		t.bound = bound
		if minSamples == 0 {
			minSamples = 1
		}
		t.minSamples = minSamples
		t.onThreshold = fn
	}
}

// New wraps next with a Tracker. If next is nil, http.DefaultTransport is
// used.
//
// Tracker must be the innermost RoundTripper — the one that directly wraps
// the *http.Transport actually performing dials — or as close to it as
// possible. httptrace hooks are read off the request's context by
// net/http.Transport itself; if any RoundTripper between Tracker and the
// real Transport drops or replaces the request's context (some
// middleware does this, e.g. via context.Background() or by building a new
// *http.Request without carrying the context forward), Tracker's counters
// will silently stay at zero for those requests because its GotConn hook
// never runs. Tracker cannot detect that this has happened; it can only
// report what it did see.
func New(next http.RoundTripper, opts ...Option) *Tracker {
	t := &Tracker{next: next}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

// RoundTrip implements http.RoundTripper. It attaches an httptrace.ClientTrace
// that observes GotConn for the request's host, then forwards the request
// unmodified (via a shallow copy carrying the new context, per
// http.RoundTripper's "must not modify the request" contract) to the wrapped
// RoundTripper.
//
// If the request's context already carries a ClientTrace (for example
// because an outer RoundTripper also uses httptrace), net/http/httptrace
// composes the two automatically: both sets of callbacks run. Tracker never
// overwrites another trace.
func (t *Tracker) RoundTrip(req *http.Request) (*http.Response, error) {
	key := HostKey(req.URL)

	ctx := httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			t.record(key, info.Reused)
		},
	})
	req = req.WithContext(ctx)

	next := t.next
	if next == nil {
		next = http.DefaultTransport
	}
	return next.RoundTrip(req)
}

// record tallies one sample for host and, if a threshold callback is armed,
// checks whether it should fire.
func (t *Tracker) record(host string, reused bool) {
	v, _ := t.hosts.LoadOrStore(host, &hostStats{host: host})
	hs := v.(*hostStats)
	if reused {
		hs.reused.Add(1)
	} else {
		hs.new.Add(1)
	}
	if t.onThreshold != nil {
		t.checkThreshold(hs)
	}
}

func (t *Tracker) checkThreshold(hs *hostStats) {
	stats := hs.snapshot()
	if stats.Total() < t.minSamples {
		return
	}
	above := stats.ChurnRatio() >= t.bound
	if above {
		// CompareAndSwap ensures exactly one goroutine observes the
		// false->true transition and fires the callback, even when many
		// requests complete this same instant.
		if hs.over.CompareAndSwap(false, true) {
			t.onThreshold(stats)
		}
	} else {
		hs.over.Store(false)
	}
}

// Snapshot returns the current counters for every host Tracker has observed,
// sorted by host key. The returned slice is a copy; mutating it does not
// affect Tracker.
func (t *Tracker) Snapshot() []Stats {
	var out []Stats
	t.hosts.Range(func(_, v any) bool {
		out = append(out, v.(*hostStats).snapshot())
		return true
	})
	sort.Slice(out, func(i, j int) bool { return out[i].Host < out[j].Host })
	return out
}

// StatsFor returns the current counters for a single host key (as produced
// by HostKey), and reports whether that host has been observed at all.
func (t *Tracker) StatsFor(host string) (Stats, bool) {
	v, ok := t.hosts.Load(host)
	if !ok {
		return Stats{Host: host}, false
	}
	return v.(*hostStats).snapshot(), true
}

// Reset discards all recorded counters and threshold state for every host.
// It does not change configuration (the threshold callback, bound and
// minSamples set at construction remain in effect).
func (t *Tracker) Reset() {
	t.hosts.Range(func(k, _ any) bool {
		t.hosts.Delete(k)
		return true
	})
}

// String renders a multi-line, log-friendly report of every host Tracker has
// observed, one Stats.String() per line, sorted by host.
func (t *Tracker) String() string {
	snap := t.Snapshot()
	if len(snap) == 0 {
		return "connchurn: no requests observed"
	}
	var b strings.Builder
	b.WriteString("connchurn report:\n")
	for _, s := range snap {
		b.WriteString("  ")
		b.WriteString(s.String())
		b.WriteByte('\n')
	}
	return b.String()
}

// HostKey normalizes a request URL into the scheme+host+port key Tracker
// counts against, so that "http://example.com", "http://example.com:80" and
// an explicit "example.com:80" all collapse to the same host, and so that
// http and https to the same hostname are kept separate (they are different
// connections with different security properties, and worth reporting on
// separately).
func HostKey(u *url.URL) string {
	scheme := u.Scheme
	host := u.Host
	h, port, err := net.SplitHostPort(host)
	if err != nil {
		h = host
		port = defaultPort(scheme)
	} else if port == "" {
		port = defaultPort(scheme)
	}
	return scheme + "://" + h + ":" + port
}

func defaultPort(scheme string) string {
	if scheme == "https" {
		return "443"
	}
	return "80"
}
