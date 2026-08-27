package chronos_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	chronos "chronos.dev/collector/sdk/go"
)

func batchingClient(t *testing.T, dir string, batchSize, maxBuffered int) *chronos.Client {
	t.Helper()
	return chronos.StartWithConfig(chronos.Config{
		Enabled:         true,
		Organisation:    "org-local",
		Project:         "project-a",
		Application:     "stock-analytics",
		SpoolDir:        dir,
		ServiceName:     "stock-analytics",
		APMEnabled:      true,
		SpanBatchSize:   batchSize,
		SpanMaxBuffered: maxBuffered,
	})
}

// readSpanBatches returns each spooled .trace document's span list.
func readSpanBatches(t *testing.T, dir string) [][]map[string]any {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.trace"))
	require.NoError(t, err)
	out := make([][]map[string]any, 0, len(files))
	for _, f := range files {
		raw, err := os.ReadFile(f)
		require.NoError(t, err)
		var batch struct {
			Schema    string           `json:"schema"`
			SpanCount string           `json:"spanCount"`
			Spans     []map[string]any `json:"spans"`
		}
		require.NoError(t, json.Unmarshal(raw, &batch))
		require.Equal(t, "chronos.tracing.span-batch.v1", batch.Schema)
		// spanCount must describe the document it travels in.
		require.Equal(t, fmt.Sprint(len(batch.Spans)), batch.SpanCount)
		out = append(out, batch.Spans)
	}
	return out
}

// The fan-out case this batching exists for: one job's chunk spans must not become one
// spool file each.
func TestSpansBatchIntoFewFiles(t *testing.T) {
	dir := t.TempDir()
	client := batchingClient(t, dir, 200, 10000)
	defer client.Shutdown(context.Background())

	ctx, root := client.StartSpan(context.Background(), "job stock-42")
	for i := range 1000 {
		_, chunk := client.StartSpan(ctx, fmt.Sprintf("chunk-%d", i))
		chunk.End()
	}
	root.End()
	require.NoError(t, client.FlushSpans())

	batches := readSpanBatches(t, dir)
	total := 0
	for _, spans := range batches {
		total += len(spans)
	}
	require.Equal(t, 1001, total, "every span must survive batching")
	// 1001 spans at 200 per document — six files, not 1001.
	require.Len(t, batches, 6)
}

func TestSpanBatchFlushesWhenFull(t *testing.T) {
	dir := t.TempDir()
	client := batchingClient(t, dir, 3, 100)
	defer client.Shutdown(context.Background())

	for i := range 3 {
		_, span := client.StartSpan(context.Background(), fmt.Sprintf("s-%d", i))
		span.End()
	}
	// Reaching the batch size flushes inline — no explicit flush, no ticker.
	batches := readSpanBatches(t, dir)
	require.Len(t, batches, 1)
	require.Len(t, batches[0], 3)
}

func TestSpanBatchHoldsPartialBatchUntilFlush(t *testing.T) {
	dir := t.TempDir()
	client := batchingClient(t, dir, 10, 100)
	defer client.Shutdown(context.Background())

	_, span := client.StartSpan(context.Background(), "lonely")
	span.End()
	require.Empty(t, readSpanBatches(t, dir), "a partial batch must not be written yet")

	require.NoError(t, client.FlushSpans())
	batches := readSpanBatches(t, dir)
	require.Len(t, batches, 1)
	require.Len(t, batches[0], 1)
}

func TestShutdownFlushesBufferedSpans(t *testing.T) {
	dir := t.TempDir()
	client := batchingClient(t, dir, 100, 1000)

	_, span := client.StartSpan(context.Background(), "in flight")
	span.End()
	require.Empty(t, readSpanBatches(t, dir))

	client.Shutdown(context.Background())

	batches := readSpanBatches(t, dir)
	require.Len(t, batches, 1)
	require.Equal(t, "in flight", batches[0][0]["name"])
}

// A buffer that cannot drain must shed load rather than grow without limit, and say how
// much it shed. This is the backstop for a spool that has stopped accepting writes.
func TestSpanBufferDropsWhenSpoolIsUnwritable(t *testing.T) {
	dir := t.TempDir()
	client := chronos.StartWithConfig(chronos.Config{
		Enabled: true, Organisation: "org-local", Project: "p", Application: "a",
		SpoolDir: dir, APMEnabled: true,
		SpanBatchSize: 4, SpanMaxBuffered: 8,
	})
	defer client.Shutdown(context.Background())

	// Every flush from here on fails, so the buffer fills and then starts dropping.
	require.NoError(t, os.Chmod(dir, 0o500))
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

	for i := range 20 {
		_, span := client.StartSpan(context.Background(), fmt.Sprintf("s-%d", i))
		span.End()
	}

	require.Positive(t, client.DroppedSpans(), "a wedged spool must shed spans, not grow the heap")
	require.Empty(t, readSpanBatches(t, dir))
}

// A batch size above the memory cap is lowered to it, so the buffer can still reach its
// flush threshold instead of relying on the ticker.
func TestSpanBatchSizeIsClampedToMaxBuffered(t *testing.T) {
	dir := t.TempDir()
	client := chronos.StartWithConfig(chronos.Config{
		Enabled: true, Organisation: "org-local", Project: "p", Application: "a",
		SpoolDir: dir, APMEnabled: true,
		SpanBatchSize: 1000, SpanMaxBuffered: 5,
	})
	defer client.Shutdown(context.Background())

	for i := range 10 {
		_, span := client.StartSpan(context.Background(), fmt.Sprintf("s-%d", i))
		span.End()
	}
	require.EqualValues(t, 0, client.DroppedSpans())
	require.Len(t, readSpanBatches(t, dir), 2)
}

func TestSpanEndIsSafeUnderConcurrency(t *testing.T) {
	dir := t.TempDir()
	client := batchingClient(t, dir, 16, 10000)
	defer client.Shutdown(context.Background())

	ctx, root := client.StartSpan(context.Background(), "job")
	var wg sync.WaitGroup
	for i := range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, span := client.StartSpan(ctx, fmt.Sprintf("chunk-%d", i))
			span.SetAttribute("chunk", fmt.Sprint(i))
			span.End()
			span.End() // double End must not double-record
		}()
	}
	wg.Wait()
	root.End()
	require.NoError(t, client.FlushSpans())

	names := map[string]int{}
	for _, spans := range readSpanBatches(t, dir) {
		for _, s := range spans {
			names[s["name"].(string)]++
		}
	}
	require.Len(t, names, 201)
	for name, count := range names {
		require.Equal(t, 1, count, "span %q recorded %d times", name, count)
	}
}

func TestSpansShareOneTraceAcrossBatches(t *testing.T) {
	dir := t.TempDir()
	client := batchingClient(t, dir, 4, 1000)
	defer client.Shutdown(context.Background())

	ctx, root := client.StartSpan(context.Background(), "job")
	for i := range 9 {
		_, chunk := client.StartSpan(ctx, fmt.Sprintf("chunk-%d", i))
		chunk.End()
	}
	root.End()
	require.NoError(t, client.FlushSpans())

	// Splitting a trace across documents must not disturb its parentage.
	for _, spans := range readSpanBatches(t, dir) {
		for _, s := range spans {
			require.Equal(t, root.TraceID, s["traceId"])
			if strings.HasPrefix(s["name"].(string), "chunk-") {
				require.Equal(t, root.SpanID, s["parentSpanId"])
			}
		}
	}
}

// The point of requeueing: a spool that recovers loses nothing.
func TestSpansSurviveATransientSpoolFailure(t *testing.T) {
	dir := t.TempDir()
	client := chronos.StartWithConfig(chronos.Config{
		Enabled: true, Organisation: "org-local", Project: "p", Application: "a",
		SpoolDir: dir, APMEnabled: true,
		SpanBatchSize: 2, SpanMaxBuffered: 100,
	})
	defer client.Shutdown(context.Background())

	require.NoError(t, os.Chmod(dir, 0o500))
	for i := range 4 {
		_, span := client.StartSpan(context.Background(), fmt.Sprintf("s-%d", i))
		span.End()
	}
	require.Empty(t, readSpanBatches(t, dir))
	require.EqualValues(t, 0, client.DroppedSpans())

	require.NoError(t, os.Chmod(dir, 0o700))
	require.NoError(t, client.FlushSpans())

	var names []string
	for _, spans := range readSpanBatches(t, dir) {
		for _, s := range spans {
			names = append(names, s["name"].(string))
		}
	}
	require.ElementsMatch(t, []string{"s-0", "s-1", "s-2", "s-3"}, names)
}
