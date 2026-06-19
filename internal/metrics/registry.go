// Package metrics implements a Prometheus-compatible exposition endpoint
// without depending on the prometheus/client_golang library.
//
// Why implement from scratch instead of using the library?
// 1. Zero external dependencies — the metrics package works everywhere Go does.
// 2. Forces precise understanding of the Prometheus data model (counters vs
//    gauges vs histograms, label cardinality, staleness semantics).
// 3. The text exposition format is 12 lines of spec — not worth a 50K dep.
//
// Prometheus text format reference: https://prometheus.io/docs/instrumenting/exposition_formats/
package metrics

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Registry holds all registered metrics. The zero value is not useful;
// use NewRegistry().
type Registry struct {
	mu      sync.RWMutex
	metrics map[string]Metric
}

// NewRegistry creates a new empty Registry.
func NewRegistry() *Registry {
	return &Registry{metrics: make(map[string]Metric)}
}

// Metric is the interface implemented by Counter, Gauge, and Histogram.
type Metric interface {
	name() string
	help() string
	mtype() string
	write(w io.Writer)
}

// -------------------------------------------------------------------
// Counter
// -------------------------------------------------------------------

// Counter is a monotonically increasing float64 value.
// Use it for: requests_total, errors_total, bytes_sent_total.
type Counter struct {
	n    string
	h    string
	val  atomic.Uint64 // stored as bits of float64
}

func (r *Registry) NewCounter(name, help string) *Counter {
	c := &Counter{n: name, h: help}
	r.register(c)
	return c
}

func (c *Counter) Inc()            { c.Add(1) }
func (c *Counter) Add(delta float64) {
	for {
		old := c.val.Load()
		newVal := math.Float64frombits(old) + delta
		if c.val.CompareAndSwap(old, math.Float64bits(newVal)) {
			return
		}
	}
}
func (c *Counter) Value() float64  { return math.Float64frombits(c.val.Load()) }
func (c *Counter) name() string    { return c.n }
func (c *Counter) help() string    { return c.h }
func (c *Counter) mtype() string   { return "counter" }
func (c *Counter) write(w io.Writer) {
	writeMetricHeader(w, c.h, c.n, "counter")
	fmt.Fprintf(w, "%s %.17g\n", c.n, c.Value())
}

// -------------------------------------------------------------------
// CounterVec — Counter with label support
// -------------------------------------------------------------------

// CounterVec is a set of Counters partitioned by label values.
// Use it when the same metric exists for multiple dimensions (e.g. per-peer lag).
type CounterVec struct {
	n      string
	h      string
	labels []string
	mu     sync.RWMutex
	vecs   map[string]*Counter
}

func (r *Registry) NewCounterVec(name, help string, labels []string) *CounterVec {
	cv := &CounterVec{n: name, h: help, labels: labels, vecs: make(map[string]*Counter)}
	r.register(cv)
	return cv
}

func (cv *CounterVec) With(labelValues ...string) *Counter {
	key := strings.Join(labelValues, "\x00")
	cv.mu.RLock()
	if c, ok := cv.vecs[key]; ok {
		cv.mu.RUnlock()
		return c
	}
	cv.mu.RUnlock()
	cv.mu.Lock()
	defer cv.mu.Unlock()
	if c, ok := cv.vecs[key]; ok {
		return c
	}
	c := &Counter{n: cv.n, h: cv.h}
	cv.vecs[key] = c
	return c
}

func (cv *CounterVec) name() string  { return cv.n }
func (cv *CounterVec) help() string  { return cv.h }
func (cv *CounterVec) mtype() string { return "counter" }
func (cv *CounterVec) write(w io.Writer) {
	cv.mu.RLock()
	defer cv.mu.RUnlock()
	if len(cv.vecs) == 0 {
		return
	}
	writeMetricHeader(w, cv.h, cv.n, "counter")
	keys := sortedKeys(cv.vecs)
	for _, key := range keys {
		c := cv.vecs[key]
		parts := strings.Split(key, "\x00")
		labelStr := buildLabelStr(cv.labels, parts)
		fmt.Fprintf(w, "%s%s %.17g\n", cv.n, labelStr, c.Value())
	}
}

// -------------------------------------------------------------------
// Gauge
// -------------------------------------------------------------------

// Gauge is a float64 that can go up or down.
// Use it for: current_leader, raft_term, commit_index.
type Gauge struct {
	n   string
	h   string
	val atomic.Uint64
}

func (r *Registry) NewGauge(name, help string) *Gauge {
	g := &Gauge{n: name, h: help}
	r.register(g)
	return g
}

func (g *Gauge) Set(v float64) {
	g.val.Store(math.Float64bits(v))
}
func (g *Gauge) SetUint(v uint64)  { g.Set(float64(v)) }
func (g *Gauge) SetInt(v int64)    { g.Set(float64(v)) }
func (g *Gauge) Inc()              { g.Add(1) }
func (g *Gauge) Dec()              { g.Add(-1) }
func (g *Gauge) Add(delta float64) {
	for {
		old := g.val.Load()
		newVal := math.Float64frombits(old) + delta
		if g.val.CompareAndSwap(old, math.Float64bits(newVal)) {
			return
		}
	}
}
func (g *Gauge) Value() float64  { return math.Float64frombits(g.val.Load()) }
func (g *Gauge) name() string    { return g.n }
func (g *Gauge) help() string    { return g.h }
func (g *Gauge) mtype() string   { return "gauge" }
func (g *Gauge) write(w io.Writer) {
	writeMetricHeader(w, g.h, g.n, "gauge")
	fmt.Fprintf(w, "%s %.17g\n", g.n, g.Value())
}

// -------------------------------------------------------------------
// Histogram
// -------------------------------------------------------------------

// Histogram tracks the distribution of observed values with configurable
// bucket boundaries. The _count, _sum, and _bucket{le} series are written.
// Use it for: latency, message size, WAL fsync duration.
type Histogram struct {
	n       string
	h       string
	bounds  []float64 // upper bound per bucket (must be sorted ascending)
	mu      sync.Mutex
	buckets []uint64 // counts per bucket (len == len(bounds))
	count   uint64
	sum     float64
}

// DefaultLatencyBuckets covers 1µs to 10s — appropriate for RPC latencies.
var DefaultLatencyBuckets = []float64{
	0.000001, 0.00001, 0.0001, 0.001, 0.002, 0.005,
	0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
}

func (r *Registry) NewHistogram(name, help string, buckets []float64) *Histogram {
	if len(buckets) == 0 {
		buckets = DefaultLatencyBuckets
	}
	h := &Histogram{
		n:       name,
		h:       help,
		bounds:  buckets,
		buckets: make([]uint64, len(buckets)),
	}
	r.register(h)
	return h
}

func (h *Histogram) Observe(v float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.count++
	h.sum += v
	for i, bound := range h.bounds {
		if v <= bound {
			h.buckets[i]++
		}
	}
}

func (h *Histogram) ObserveDuration(start time.Time) {
	h.Observe(time.Since(start).Seconds())
}

func (h *Histogram) Snapshot() (count uint64, sum float64, buckets []uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	b := make([]uint64, len(h.buckets))
	copy(b, h.buckets)
	return h.count, h.sum, b
}

func (h *Histogram) name() string  { return h.n }
func (h *Histogram) help() string  { return h.h }
func (h *Histogram) mtype() string { return "histogram" }
func (h *Histogram) write(w io.Writer) {
	count, sum, buckets := h.Snapshot()
	writeMetricHeader(w, h.h, h.n, "histogram")
	cumulative := uint64(0)
	for i, bound := range h.bounds {
		cumulative += buckets[i]
		le := strconv.FormatFloat(bound, 'g', -1, 64)
		fmt.Fprintf(w, "%s_bucket{le=%q} %d\n", h.n, le, cumulative)
	}
	fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n", h.n, count)
	fmt.Fprintf(w, "%s_sum %.17g\n", h.n, sum)
	fmt.Fprintf(w, "%s_count %d\n", h.n, count)
}

// -------------------------------------------------------------------
// Summary (percentile estimation via reservoir)
// -------------------------------------------------------------------

// Summary computes quantiles over a sliding window using a t-digest-inspired
// reservoir. For p50/p95/p99 Raft latency reporting.
type Summary struct {
	n        string
	h        string
	mu       sync.Mutex
	samples  []float64
	maxSamp  int
	inserted uint64
}

func (r *Registry) NewSummary(name, help string) *Summary {
	s := &Summary{n: name, h: help, maxSamp: 1024, samples: make([]float64, 0, 1024)}
	r.register(s)
	return s
}

func (s *Summary) Observe(v float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inserted++
	if len(s.samples) < s.maxSamp {
		s.samples = append(s.samples, v)
	} else {
		// Reservoir sampling: replace random element.
		idx := s.inserted % uint64(s.maxSamp)
		s.samples[idx] = v
	}
}

func (s *Summary) Quantile(q float64) float64 {
	s.mu.Lock()
	cp := make([]float64, len(s.samples))
	copy(cp, s.samples)
	s.mu.Unlock()
	if len(cp) == 0 {
		return 0
	}
	sort.Float64s(cp)
	idx := int(float64(len(cp)-1) * q)
	return cp[idx]
}

func (s *Summary) name() string  { return s.n }
func (s *Summary) help() string  { return s.h }
func (s *Summary) mtype() string { return "summary" }
func (s *Summary) write(w io.Writer) {
	writeMetricHeader(w, s.h, s.n, "summary")
	for _, q := range []float64{0.5, 0.9, 0.95, 0.99} {
		v := s.Quantile(q)
		fmt.Fprintf(w, "%s{quantile=\"%g\"} %.17g\n", s.n, q, v)
	}
	s.mu.Lock()
	count := s.inserted
	s.mu.Unlock()
	fmt.Fprintf(w, "%s_count %d\n", s.n, count)
}

// -------------------------------------------------------------------
// Registry internals
// -------------------------------------------------------------------

func (r *Registry) register(m Metric) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.metrics[m.name()] = m
}

// WriteText writes all metrics in Prometheus text exposition format to w.
func (r *Registry) WriteText(w io.Writer) {
	r.mu.RLock()
	names := make([]string, 0, len(r.metrics))
	for n := range r.metrics {
		names = append(names, n)
	}
	r.mu.RUnlock()
	sort.Strings(names)
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, n := range names {
		r.metrics[n].write(w)
		fmt.Fprintln(w)
	}
}

// Handler returns an http.Handler that serves the Prometheus text format.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		r.WriteText(w)
	})
}

func writeMetricHeader(w io.Writer, help, name, mtype string) {
	if help != "" {
		fmt.Fprintf(w, "# HELP %s %s\n", name, help)
	}
	fmt.Fprintf(w, "# TYPE %s %s\n", name, mtype)
}

func buildLabelStr(keys, values []string) string {
	if len(keys) == 0 {
		return ""
	}
	var parts []string
	for i, k := range keys {
		v := ""
		if i < len(values) {
			v = values[i]
		}
		parts = append(parts, fmt.Sprintf("%s=%q", k, v))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
