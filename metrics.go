package chronos

import (
	"os"
	"runtime"
	"time"

	"chronos.dev/collector/sdk/go/spool"
)

// EmitRuntimeMetrics writes a chronos.runtime.metrics.v1 point for this process.
func (c *Client) EmitRuntimeMetrics() error {
	if c == nil || !c.cfg.Enabled || !c.cfg.MetricsEnabled {
		return nil
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	// Drain request counters for the catalog scrape path. The processor still
	// folds these into chronos_php_* Prometheus series today.
	reqs := c.httpRequests.Swap(0)
	errs := c.httpErrors.Swap(0)
	durNs := c.httpDuration.Swap(0)
	if reqs == 0 {
		// Keep the application visible in the service catalog between quiet
		// windows (same role as a PHP process heartbeat).
		reqs = 1
	}
	durationMs := float64(durNs) / 1_000_000.0
	collections, pauseMs := c.gcDelta(ms)

	point := metricsPoint{
		Schema: "chronos.runtime.metrics.v1",
		Time:   formatRFC3339Nano(time.Now().UTC()),
		Service: metricsService{
			Organisation: c.cfg.Organisation,
			Project:      c.cfg.Project,
			Application:  c.cfg.Application,
			Version:      c.cfg.ServiceVersion,
		},
		Resource: map[string]any{
			"process.pid":              os.Getpid(),
			"process.runtime.name":     "go",
			"process.runtime.version":  runtime.Version(),
			"process.runtime.compiler": runtime.Compiler,
			"process.thread.count":     runtime.NumGoroutine(),
		},
		Attributes: map[string]string{
			"service.name": c.cfg.ServiceName,
		},
		Metrics: map[string]float64{
			// ADR 0024 §5: the quantity names the key, not the language. A heap,
			// a GC and a request count are not Go's or PHP's, and a Go process
			// emitting process.runtime.php.request.count (which this did) was the
			// reductio of the old convention.
			"process.runtime.memory.heap_alloc_bytes":  float64(ms.HeapAlloc),
			"process.runtime.memory.heap_sys_bytes":    float64(ms.HeapSys),
			"process.runtime.memory.heap_inuse_bytes":  float64(ms.HeapInuse),
			"process.runtime.memory.stack_inuse_bytes": float64(ms.StackInuse),
			"process.runtime.gc.collections":           float64(collections),
			"process.runtime.gc.pause_ms":              pauseMs,
			// A goroutine is not a thread, but the series answers the same
			// question a thread count answers — how many concurrent things is
			// this process carrying, and is it climbing.
			"process.runtime.threads":             float64(runtime.NumGoroutine()),
			"process.runtime.request.count":       float64(reqs),
			"process.runtime.request.duration_ms": durationMs,
			"process.runtime.request.errors":      float64(errs),
		},
	}
	_, err := c.writer.WriteJSON(spool.SignalMetrics, point)
	return err
}

type metricsPoint struct {
	Schema     string             `json:"schema"`
	Time       string             `json:"time"`
	Service    metricsService     `json:"service"`
	Resource   map[string]any     `json:"resource"`
	Attributes map[string]string  `json:"attributes"`
	Metrics    map[string]float64 `json:"metrics"`
}

type metricsService struct {
	Organisation string `json:"organisation"`
	Project      string `json:"project"`
	Application  string `json:"application"`
	Version      string `json:"version,omitempty"`
}

// gcDelta converts Go's process-cumulative GC counters into the per-window
// quantities the shared keys mean.
//
// ADR 0024 §5: process.runtime.gc.collections and .gc.pause_ms are the same
// series for every runtime, and the PHP extension reports both PER REQUEST.
// Go's runtime.MemStats reports NumGC and PauseTotalNs since process start, so
// publishing them raw under a shared key would put two different quantities on
// one chart — a monotonic ramp beside a per-request cost. The delta since this
// client's previous point is the reading that matches.
//
// The first point after boot reports the process's whole history, which is
// correct: that IS the collection work done in the window since the process
// started, and there is no earlier point to subtract.
func (c *Client) gcDelta(ms runtime.MemStats) (collections uint32, pauseMs float64) {
	const nanosPerMilli = 1_000_000.0
	previousNum := c.gcNum.Swap(ms.NumGC)
	previousPause := c.gcPauseNs.Swap(ms.PauseTotalNs)
	// A counter that went backwards means MemStats was read out of order across
	// goroutines; zero is the honest answer, not a wrapped-around huge number.
	if ms.NumGC >= previousNum {
		collections = ms.NumGC - previousNum
	}
	if ms.PauseTotalNs >= previousPause {
		pauseMs = float64(ms.PauseTotalNs-previousPause) / nanosPerMilli
	}
	return collections, pauseMs
}
