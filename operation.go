package chronos

import (
	"context"
	"fmt"
	"runtime/pprof"
)

// Do runs fn under runtime/pprof labels so process-wide CPU / goroutine / mutex
// profiles attribute samples to this work unit (job, queue, route). Prefer this
// at a few dispatch entrypoints — not inside every business function.
func Do(ctx context.Context, labels map[string]string, fn func(context.Context)) {
	if len(labels) == 0 {
		fn(ctx)
		return
	}
	pprof.Do(ctx, labelSet(labels), fn)
}

// DoErr is Do for fallible work.
func DoErr(ctx context.Context, labels map[string]string, fn func(context.Context) error) error {
	var err error
	Do(ctx, labels, func(ctx context.Context) {
		err = fn(ctx)
	})
	return err
}

func labelSet(labels map[string]string) pprof.LabelSet {
	pairs := make([]string, 0, len(labels)*2)
	for k, v := range labels {
		if k == "" || v == "" {
			continue
		}
		pairs = append(pairs, k, v)
	}
	if len(pairs) == 0 {
		return pprof.Labels()
	}
	return pprof.Labels(pairs...)
}

// Run starts a span named name, runs fn, records errors on the span, and ends it.
// When profiling is enabled, samples during fn are also labelled job=<name> so
// continuous profiles can filter worker / job stacks without wrapping each call site.
func (c *Client) Run(ctx context.Context, name string, fn func(context.Context) error) error {
	if c == nil || !c.cfg.Enabled {
		return fn(ctx)
	}
	if !c.cfg.APMEnabled {
		if c.cfg.ProfilerEnabled {
			return DoErr(ctx, map[string]string{"job": name}, fn)
		}
		return fn(ctx)
	}

	ctx, span := c.StartSpan(ctx, name)
	run := func(ctx context.Context) (err error) {
		defer func() {
			if err != nil {
				span.SetStatus("error")
				span.SetAttribute("error.message", err.Error())
			}
			span.End()
		}()
		return fn(ctx)
	}
	if c.cfg.ProfilerEnabled {
		// The span is started before the labels so its identifiers can be stamped on
		// every CPU sample taken inside fn; NormalizePprof lifts them back out into the
		// sample's correlation field, which is what lets the desktop jump from a flame
		// frame to the trace that produced it.
		return DoErr(ctx, map[string]string{
			"job":      name,
			"trace_id": span.TraceID,
			"span_id":  span.SpanID,
		}, run)
	}
	return run(ctx)
}

// RunDefault is Run against Default().
func Run(ctx context.Context, name string, fn func(context.Context) error) error {
	return Default().Run(ctx, name, fn)
}

// Annotate copies string attributes onto the ambient span (no-op if none).
func Annotate(ctx context.Context, attrs map[string]string) {
	span := SpanFromContext(ctx)
	if span == nil {
		return
	}
	for k, v := range attrs {
		span.SetAttribute(k, v)
	}
}

// FormatAttr is a tiny helper for numeric attributes.
func FormatAttr(v any) string {
	return fmt.Sprint(v)
}

// Bind captures the ambient trace and profiling labels so they can be restored on a
// different goroutine.
//
// WHY THIS EXISTS. runtime/pprof labels live on the goroutine that set them, and Go does
// not carry them across a channel. A worker pool whose goroutines were started at boot
// therefore runs every job unlabelled, so CPU samples taken during the job cannot be tied
// back to the job or its trace. The WALL / OFF_CPU / IO profiles do not have this problem
// — they observe goroutines wherever they are — so this is only needed to correlate CPU
// samples and to give pool work a child span of the dispatching trace.
//
// Call it at the dispatch site and invoke the result inside the worker:
//
//	run := chronos.Bind(ctx, "chunk-"+id)
//	pool.Enqueue(func() { run(func(ctx context.Context) error { return job.Run(ctx) }) })
func Bind(ctx context.Context, name string) func(func(context.Context) error) error {
	return Default().Bind(ctx, name)
}

// Bind is the Client-scoped form of Bind.
func (c *Client) Bind(ctx context.Context, name string) func(func(context.Context) error) error {
	if c == nil || !c.cfg.Enabled {
		return func(fn func(context.Context) error) error { return fn(ctx) }
	}
	// ctx is captured, not re-derived: it still carries the dispatching span (so the
	// worker's span becomes its child) and still carries the job's deadline and
	// cancellation, which the worker must continue to honour.
	//
	// The work is that Run — and with it pprof.Do and the span start — now executes on
	// the worker goroutine, which is the only goroutine whose labels the profiler sees.
	return func(fn func(context.Context) error) error {
		return c.Run(ctx, name, fn)
	}
}
