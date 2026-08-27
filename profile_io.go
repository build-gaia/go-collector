package chronos

import (
	"context"
	"runtime"
	"sync"
	"time"
)

// Exact I/O profiling.
//
// Unlike the wall sampler this is not statistical: an instrumented call reports its real
// duration and the real stack at the blocking call, which is strictly better attribution
// than off-CPU sampling for the calls it covers. The trade-off is coverage — it only sees
// I/O the SDK already brackets (the database/sql driver installed by chronos.Open, and
// the HTTP transport). This mirrors the PHP sampler, where IO samples come from the Zend
// observer around PDO / mysqli / Redis / curl rather than from the interval timer.
//
// Overhead is bounded by CHRONOS_GO_IO_MIN_WAIT: a job issuing thousands of sub-
// millisecond queries pays a duration comparison per call, not a stack walk.

// ioWait is one measured blocking call awaiting flush.
type ioWait struct {
	pcs     []uintptr
	op      string
	system  string
	nanos   int64
	at      time.Time
	traceID string
	spanID  string
}

// ioRecorder is a bounded buffer of measured I/O waits.
type ioRecorder struct {
	mu       sync.Mutex
	waits    []ioWait
	max      int
	maxDepth int
	dropped  int64
}

func newIORecorder(max, maxDepth int) *ioRecorder {
	if max <= 0 {
		max = 4096
	}
	if maxDepth <= 0 {
		maxDepth = 64
	}
	return &ioRecorder{waits: make([]ioWait, 0, 64), max: max, maxDepth: maxDepth}
}

func (r *ioRecorder) add(w ioWait) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Drop newest rather than evicting oldest: the buffer is a per-flush window, and
	// keeping its head means the samples that survive still describe one coherent period.
	if len(r.waits) >= r.max {
		r.dropped++
		return
	}
	r.waits = append(r.waits, w)
}

func (r *ioRecorder) drain() []ioWait {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.waits) == 0 {
		return nil
	}
	out := r.waits
	r.waits = make([]ioWait, 0, min(cap(out), 256))
	return out
}

// RecordIOWait records a measured blocking call as a PROFILE_SAMPLE_TYPE_IO sample.
//
// The SQL driver and HTTP transport call this for you. Call it directly to cover an I/O
// client the SDK does not wrap (Redis, S3, a queue broker): pass the operation name you
// want to see as the flame chart's leaf, and the wall duration of the call.
//
// skip is the number of stack frames between the blocking call and this function, so the
// captured stack starts at the caller's own code.
func (c *Client) RecordIOWait(ctx context.Context, op string, d time.Duration) {
	c.recordIOWait(ctx, op, "", d, 3)
}

func (c *Client) recordIOWait(ctx context.Context, op, system string, d time.Duration, skip int) {
	if c == nil || !c.cfg.Enabled || !c.cfg.ProfilerEnabled || !c.cfg.IOProfileEnabled {
		return
	}
	if c.io == nil || d < c.cfg.IOMinWait {
		return
	}
	pcs := make([]uintptr, c.cfg.ProfileMaxStackDepth)
	n := runtime.Callers(skip, pcs)
	if n == 0 {
		return
	}
	wait := ioWait{pcs: pcs[:n], op: op, system: system, nanos: d.Nanoseconds(), at: time.Now().UTC()}
	if span := SpanFromContext(ctx); span != nil {
		wait.traceID, wait.spanID = span.TraceID, span.SpanID
	}
	c.io.add(wait)
}

// ioSamplesFrom converts measured waits into sample records.
//
// The operation becomes a synthetic leaf frame so a flame chart bottoms out in
// "SQL SELECT" rather than in database/sql plumbing — the same shape the PHP sampler
// gives an instrumented PDO call.
func (c *Client) ioSamplesFrom(waits []ioWait) []ProfileSample {
	out := make([]ProfileSample, 0, len(waits))
	for i, w := range waits {
		frames := symbolizeRootFirst(w.pcs)
		frames = append(frames, ProfileFrame{Module: "io", Function: w.op})
		extra := map[string]string{"wait": "io"}
		if w.system != "" {
			extra["db.system"] = w.system
		}
		var correlation *ProfileCorrelation
		if w.traceID != "" || w.spanID != "" {
			correlation = &ProfileCorrelation{TraceID: w.traceID, SpanID: w.spanID}
		}
		// An exact measurement carries no sampling period; period 0 tells the engine
		// not to scale it up the way it scales a statistical sample.
		out = append(out, c.profileSample(ioSeriesID, "PROFILE_SAMPLE_TYPE_IO", "nanoseconds",
			i+1, w.at, 0, w.nanos, frames, c.baseProfileLabels(extra), correlation))
	}
	return out
}

// flushIOProfiles spools the measured I/O waits banked since the last flush.
func (c *Client) flushIOProfiles() error {
	if c == nil || c.io == nil {
		return nil
	}
	waits := c.io.drain()
	if len(waits) == 0 {
		return nil
	}
	return c.writeProfileSamples(ioSeriesID, c.ioSamplesFrom(waits))
}

// FlushProfiles spools whatever the wall and IO samplers have banked so far.
//
// Background clients flush on their own interval and again on Shutdown; call this
// directly from a short-lived process (a one-shot command, a test) that would otherwise
// exit before its first flush tick.
func (c *Client) FlushProfiles() error {
	if err := c.flushWallProfiles(); err != nil {
		return err
	}
	return c.flushIOProfiles()
}
