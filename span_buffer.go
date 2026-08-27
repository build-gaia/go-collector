package chronos

import (
	"context"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"chronos.dev/collector/sdk/go/spool"
)

// Span batching.
//
// WHY. Span.End used to spool one file per span. That is fine for a request/response
// service where a trace is a handful of spans, and pathological for a fan-out job
// workload: a client job that dispatches 5,000 chunks, each issuing queries, produces
// tens of thousands of files for the sidecar to stat, read, ship and unlink — the
// per-file cost dominates and the spool directory becomes the bottleneck before the
// ingest route does. The batch schema has always carried a `spans` array and a
// `spanCount`; only the Go writer was ignoring it.
//
// WHEN IT FLUSHES. When the buffer reaches SpanBatchSize, on the flush ticker, and on
// Shutdown. The size-triggered flush runs inline, on whichever goroutine ended the span
// that filled the buffer: it is one marshal and one write for work already amortised
// across the whole batch, and doing it inline keeps backpressure where it belongs
// instead of hiding an unbounded queue behind a channel.
//
// WHAT IT COSTS. Spans live in memory until flushed, so an abrupt kill loses at most one
// window. SpanMaxBuffered bounds that exposure; past it, spans are dropped and counted
// rather than growing the heap without limit.

type spanBuffer struct {
	mu          sync.Mutex
	spans       []spanRecord
	batchSize   int
	maxBuffered int
	dropped     atomic.Uint64
}

func newSpanBuffer(batchSize, maxBuffered int) *spanBuffer {
	if batchSize <= 0 {
		batchSize = defaultSpanBatchSize
	}
	// A cap below the batch size would mean the buffer can never reach its flush
	// threshold. Honour the memory cap and lower the batch size to match, rather than
	// silently raising the cap the caller asked for.
	if maxBuffered > 0 && maxBuffered < batchSize {
		batchSize = maxBuffered
	}
	if maxBuffered <= 0 {
		maxBuffered = batchSize
	}
	return &spanBuffer{
		spans:       make([]spanRecord, 0, batchSize),
		batchSize:   batchSize,
		maxBuffered: maxBuffered,
	}
}

// add banks a span and reports whether the buffer has reached its flush threshold.
func (b *spanBuffer) add(record spanRecord) (full bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.spans) >= b.maxBuffered {
		// Drop the newest: the spans already banked form a coherent window, and
		// discarding them to make room would throw away completed work for pending work.
		b.dropped.Add(1)
		return false
	}
	b.spans = append(b.spans, record)
	return len(b.spans) >= b.batchSize
}

// take removes up to batchSize spans, or everything when all is set.
func (b *spanBuffer) take(all bool) []spanRecord {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.spans) == 0 {
		return nil
	}
	n := len(b.spans)
	if !all && n > b.batchSize {
		n = b.batchSize
	}
	out := b.spans[:n:n]
	remaining := b.spans[n:]
	b.spans = make([]spanRecord, 0, max(b.batchSize, len(remaining)))
	b.spans = append(b.spans, remaining...)
	return out
}

// requeue puts an unwritten batch back at the head of the buffer, so a transient spool
// failure costs a retry rather than the spans. Whatever no longer fits under the cap is
// dropped and counted — a spool that never recovers must shed load, not grow the heap.
func (b *spanBuffer) requeue(records []spanRecord) {
	b.mu.Lock()
	defer b.mu.Unlock()
	room := b.maxBuffered - len(b.spans)
	if room < 0 {
		room = 0
	}
	if len(records) > room {
		b.dropped.Add(uint64(len(records) - room))
		records = records[:room]
	}
	// Prepend: the returned batch is older than anything banked while it was in flight.
	b.spans = append(records, b.spans...)
}

func (b *spanBuffer) len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.spans)
}

// enqueueSpan buffers a finished span, flushing inline once the batch is full.
func (c *Client) enqueueSpan(record spanRecord) {
	if c == nil || c.spans == nil {
		return
	}
	if !c.spans.add(record) {
		return
	}
	if err := c.flushSpanBatch(false); err != nil {
		slog.Debug("chronos: span flush failed", "err", err)
	}
}

// FlushSpans writes every buffered span to the spool.
//
// Background clients flush on their own interval and again on Shutdown; call this from a
// short-lived process, or a test, that would otherwise exit before its first flush tick.
func (c *Client) FlushSpans() error {
	return c.flushSpanBatch(true)
}

func (c *Client) flushSpanBatch(all bool) error {
	if c == nil || c.spans == nil {
		return nil
	}
	for {
		records := c.spans.take(all)
		if len(records) == 0 {
			return nil
		}
		batch := spanBatch{
			Schema:     "chronos.tracing.span-batch.v1",
			Processing: c.cfg.processing(seedNow() + records[0].SpanID),
			Spans:      records,
			SpanCount:  strconv.Itoa(len(records)),
		}
		if _, err := c.writer.WriteJSON(spool.SignalTrace, batch); err != nil {
			c.spans.requeue(records)
			return err
		}
		if !all {
			return nil
		}
	}
}

// Flush writes every buffered signal — spans first, since a profile sample's correlation
// is only useful once the trace it points at exists.
func (c *Client) Flush() error {
	if err := c.FlushSpans(); err != nil {
		return err
	}
	return c.FlushProfiles()
}

// spanLoop flushes buffered spans on a fixed interval so a low-traffic service does not
// sit on a half-full batch indefinitely.
func (c *Client) spanLoop(ctx context.Context) {
	defer c.wg.Done()
	interval := c.cfg.SpanFlushInterval
	if interval <= 0 {
		interval = defaultSpanFlushInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.flushSpanBatch(true); err != nil {
				slog.Debug("chronos: span flush failed", "err", err)
			}
		}
	}
}

// DroppedSpans counts spans discarded because the buffer was full — non-zero means the
// spool is not draining, or SpanMaxBuffered is too small for the fan-out.
func (c *Client) DroppedSpans() uint64 {
	if c == nil || c.spans == nil {
		return 0
	}
	return c.spans.dropped.Load()
}
