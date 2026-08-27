package chronos_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	chronos "chronos.dev/collector/sdk/go"
	"chronos.dev/collector/sdk/go/redact"
	"chronos.dev/collector/sdk/go/spool"
)

func TestSpoolAtomicWrite(t *testing.T) {
	dir := t.TempDir()
	w := spool.Writer{Dir: dir}
	path, err := w.WriteJSON(spool.SignalTrace, map[string]any{"schema": "test", "n": 1})
	require.NoError(t, err)
	require.True(t, strings.HasSuffix(path, ".trace"))
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.False(t, strings.HasPrefix(entries[0].Name(), "tmp-"))
	body, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Contains(t, string(body), `"schema":"test"`)
}

func TestRedactMasksSensitiveKeys(t *testing.T) {
	out := redact.Map(map[string]string{
		"Authorization": "Bearer super-secret-token",
		"X-Request-Id":  "abc",
	}, []string{"authorization", "token"})
	require.Equal(t, "abc", out["X-Request-Id"])
	require.True(t, strings.HasPrefix(out["Authorization"], "*********"))
	require.True(t, strings.HasSuffix(out["Authorization"], "oken"))
}

func TestConfigDisabledWithoutIdentity(t *testing.T) {
	t.Setenv("CHRONOS_ENABLED", "1")
	t.Setenv("CHRONOS_ORGANISATION_ID", "")
	t.Setenv("CHRONOS_PROJECT_ID", "")
	t.Setenv("CHRONOS_APPLICATION_ID", "")
	t.Setenv("CHRONOS_SPOOL_DIRECTORY", "")
	cfg := chronos.LoadConfig()
	require.False(t, cfg.Enabled)
}

func TestConfigEnabledWithIdentity(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("CHRONOS_ENABLED", "1")
	t.Setenv("CHRONOS_ORGANISATION_ID", "org-local")
	t.Setenv("CHRONOS_PROJECT_ID", "project-a")
	t.Setenv("CHRONOS_APPLICATION_ID", "stock-analytics")
	t.Setenv("CHRONOS_SPOOL_DIRECTORY", dir)
	t.Setenv("CHRONOS_PROFILER_ENABLED", "0")
	t.Setenv("CHRONOS_METRICS_ENABLED", "0")
	cfg := chronos.LoadConfig()
	require.True(t, cfg.Enabled)
	require.Equal(t, "stock-analytics", cfg.Application)
}

func TestHTTPMiddlewareWritesTrace(t *testing.T) {
	dir := t.TempDir()
	client := chronos.StartWithConfig(chronos.Config{
		Enabled:        true,
		Organisation:   "org-local",
		Project:        "project-a",
		Application:    "stock-analytics",
		SpoolDir:       dir,
		ServiceName:    "stock-analytics",
		APMEnabled:     true,
		HTTPCapture:    true,
		HTTPRedact:     true,
		RedactPatterns: []string{"authorization"},
	})
	defer client.Shutdown(context.Background())

	mux := http.NewServeMux()
	mux.HandleFunc("/stock", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	handler := client.Handler(mux)

	req := httptest.NewRequest(http.MethodGet, "http://example.test/stock?q=1", nil)
	req.Header.Set("Authorization", "Bearer abcdefghijklmnop")
	req.Header.Set("X-Forwarded-Proto", "https")
	req.Header.Set("X-Forwarded-Host", "edge.example")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
	require.NotEmpty(t, rr.Header().Get("traceparent"))

	require.NoError(t, client.FlushSpans())
	files, err := filepath.Glob(filepath.Join(dir, "*.trace"))
	require.NoError(t, err)
	require.Len(t, files, 1)

	raw, err := os.ReadFile(files[0])
	require.NoError(t, err)
	var batch map[string]any
	require.NoError(t, json.Unmarshal(raw, &batch))
	require.Equal(t, "chronos.tracing.span-batch.v1", batch["schema"])
	spans := batch["spans"].([]any)
	require.Len(t, spans, 1)
	span := spans[0].(map[string]any)
	require.Equal(t, "GET /stock", span["name"])
	attrs := span["attributes"].(map[string]any)
	require.Equal(t, "https", attrs["url.scheme"])
	headersRaw, ok := attrs["http.request.headers"].(string)
	require.True(t, ok, "expected http.request.headers JSON map")
	var headers map[string]string
	require.NoError(t, json.Unmarshal([]byte(headersRaw), &headers))
	auth := headers["Authorization"]
	require.True(t, strings.HasPrefix(auth, "*********"), "auth=%q", auth)
	queryRaw, ok := attrs["http.request.query"].(string)
	require.True(t, ok)
	var query map[string]string
	require.NoError(t, json.Unmarshal([]byte(queryRaw), &query))
	require.Equal(t, "1", query["q"])
	_, ok = attrs["http.response.headers"].(string)
	require.True(t, ok, "expected http.response.headers JSON map")
}

func TestHTTPMiddlewareSkipsConfiguredPaths(t *testing.T) {
	dir := t.TempDir()
	client := chronos.StartWithConfig(chronos.Config{
		Enabled:       true,
		Organisation:  "org-local",
		Project:       "project-a",
		Application:   "stock-analytics",
		SpoolDir:      dir,
		ServiceName:   "stock-analytics",
		APMEnabled:    true,
		HTTPSkipPaths: []string{"/status", "/api/stats"},
	})
	defer client.Shutdown(context.Background())

	mux := http.NewServeMux()
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/work", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := client.Handler(mux)

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "http://example.test/status", nil))
	rr2 := httptest.NewRecorder()
	handler.ServeHTTP(rr2, httptest.NewRequest(http.MethodGet, "http://example.test/work", nil))

	require.NoError(t, client.FlushSpans())
	files, err := filepath.Glob(filepath.Join(dir, "*.trace"))
	require.NoError(t, err)
	require.Len(t, files, 1)
	raw, err := os.ReadFile(files[0])
	require.NoError(t, err)
	require.Contains(t, string(raw), `"name":"GET /work"`)
	require.NotContains(t, string(raw), `"name":"GET /status"`)
}

func TestLogWritesBatch(t *testing.T) {
	dir := t.TempDir()
	client := chronos.StartWithConfig(chronos.Config{
		Enabled:      true,
		Organisation: "org-local",
		Project:      "project-a",
		Application:  "stock-analytics",
		SpoolDir:     dir,
		LogsEnabled:  true,
	})
	defer client.Shutdown(context.Background())

	ctx, span := client.StartSpan(context.Background(), "job")
	client.Log(ctx, "error", "checkout failed", map[string]string{"order": "789"})
	span.End()

	files, err := filepath.Glob(filepath.Join(dir, "*.log"))
	require.NoError(t, err)
	require.Len(t, files, 1)
	raw, err := os.ReadFile(files[0])
	require.NoError(t, err)
	require.Contains(t, string(raw), `"schema":"chronos.tracing.log-batch.v1"`)
	require.Contains(t, string(raw), `"body":"checkout failed"`)
	require.Contains(t, string(raw), span.TraceID)
}

func TestMetricsPoint(t *testing.T) {
	dir := t.TempDir()
	client := chronos.StartWithConfig(chronos.Config{
		Enabled:        true,
		Organisation:   "org-local",
		Project:        "project-a",
		Application:    "stock-analytics",
		SpoolDir:       dir,
		MetricsEnabled: true,
		ServiceName:    "stock-analytics",
		ServiceVersion: "1.0.0",
	})
	defer client.Shutdown(context.Background())
	require.NoError(t, client.EmitRuntimeMetrics())
	files, err := filepath.Glob(filepath.Join(dir, "*.metrics"))
	require.NoError(t, err)
	require.Len(t, files, 1)
	raw, err := os.ReadFile(files[0])
	require.NoError(t, err)
	require.Contains(t, string(raw), `"schema":"chronos.runtime.metrics.v1"`)
	require.Contains(t, string(raw), `"process.runtime.name":"go"`)
}

func TestCPUProfileNormalizesToSampleBatch(t *testing.T) {
	dir := t.TempDir()
	client := chronos.StartWithConfig(chronos.Config{
		Enabled:         true,
		Organisation:    "org-local",
		Project:         "project-a",
		Application:     "stock-analytics",
		SpoolDir:        dir,
		ProfilerEnabled: true,
		ServiceName:     "stock-analytics",
		ProfileDuration: 50 * time.Millisecond,
	})
	defer client.Shutdown(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		deadline := time.Now().Add(80 * time.Millisecond)
		for time.Now().Before(deadline) {
			_ = strings.Repeat("x", 64)
		}
	}()
	require.NoError(t, client.CollectCPUProfile(50*time.Millisecond))
	<-done

	files, err := filepath.Glob(filepath.Join(dir, "*.profile"))
	require.NoError(t, err)
	if len(files) == 0 {
		t.Skip("CPU profile produced no samples in this environment")
	}
	raw, err := os.ReadFile(files[0])
	require.NoError(t, err)
	var batch map[string]any
	require.NoError(t, json.Unmarshal(raw, &batch))
	require.Equal(t, "chronos.profiling.sample-batch.v1", batch["schema"])
	samples := batch["samples"].([]any)
	require.NotEmpty(t, samples)
	sample := samples[0].(map[string]any)
	require.Equal(t, "go-cpu", sample["profileSeriesId"])
	require.Equal(t, "PROFILE_SAMPLE_TYPE_CPU", sample["sampleType"])
	require.Equal(t, "nanoseconds", sample["unit"])
	stack := sample["stack"].([]any)
	require.NotEmpty(t, stack)
}

// ADR 0024 §5: the metric point names quantities, not languages. A Go process
// must never emit a process.runtime.php.* key, and the shared keys must be the
// normalised spellings the engine's mapping table knows.
func TestRuntimeMetricsUsePolyglotNames(t *testing.T) {
	dir := t.TempDir()
	client := chronos.StartWithConfig(chronos.Config{
		Enabled:        true,
		Organisation:   "org-local",
		Project:        "project-a",
		Application:    "stock-analytics",
		SpoolDir:       dir,
		ServiceName:    "stock-analytics",
		MetricsEnabled: true,
	})
	defer client.Shutdown(context.Background())

	require.NoError(t, client.EmitRuntimeMetrics())

	point := readMetricsPoint(t, dir)
	metrics, _ := point["metrics"].(map[string]any)
	require.NotEmpty(t, metrics)

	for _, key := range []string{
		"process.runtime.request.count",
		"process.runtime.request.duration_ms",
		"process.runtime.request.errors",
		"process.runtime.memory.heap_alloc_bytes",
		"process.runtime.memory.heap_inuse_bytes",
		"process.runtime.memory.heap_sys_bytes",
		"process.runtime.memory.stack_inuse_bytes",
		"process.runtime.gc.collections",
		"process.runtime.gc.pause_ms",
		"process.runtime.threads",
	} {
		require.Contains(t, metrics, key)
	}
	for key := range metrics {
		require.NotContains(t, key, ".php.", "a Go process must not emit a PHP-namespaced metric")
		require.NotContains(t, key, ".go.", "a shared quantity must not be Go-namespaced")
	}

	resource, _ := point["resource"].(map[string]any)
	require.Contains(t, resource, "process.thread.count")
	require.NotContains(t, resource, "process.num_goroutine")
}

// GC counters are cumulative in Go and per-request in PHP; the shared key means
// the latter, so a second point must report only the work since the first.
func TestGCMetricsReportTheWindowNotTheProcess(t *testing.T) {
	dir := t.TempDir()
	client := chronos.StartWithConfig(chronos.Config{
		Enabled:        true,
		Organisation:   "org-local",
		Project:        "project-a",
		Application:    "stock-analytics",
		SpoolDir:       dir,
		MetricsEnabled: true,
	})
	defer client.Shutdown(context.Background())

	require.NoError(t, client.EmitRuntimeMetrics())
	first := readMetricsPoint(t, dir)
	firstMetrics, _ := first["metrics"].(map[string]any)
	// The first point reports the process's history, which is the window since
	// boot. It is only the SECOND point that must be a delta.
	require.NotNil(t, firstMetrics["process.runtime.gc.collections"])

	runtime.GC()
	require.NoError(t, client.EmitRuntimeMetrics())

	// Both points are on disk, oldest first. If the SDK published Go's
	// cumulative counters raw, the two windows would each carry the whole
	// history and their sum would be roughly double it.
	var collections []float64
	for _, point := range readMetricsPoints(t, dir) {
		metrics, _ := point["metrics"].(map[string]any)
		value, _ := metrics["process.runtime.gc.collections"].(float64)
		collections = append(collections, value)
	}
	require.Len(t, collections, 2)
	require.LessOrEqual(t, collections[0]+collections[1], float64(testTotalGC()),
		"the two windows together cannot exceed the collections the process has ever done")
	require.GreaterOrEqual(t, collections[1], 1.0, "the forced GC must land in the second window")
}

func testTotalGC() uint32 {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.NumGC
}

// readMetricsPoints returns every runtime point in the spool, OLDEST FIRST.
// Spool file names are content-addressed, so the write order is only recoverable
// from the file times.
func readMetricsPoints(t *testing.T, dir string) []map[string]any {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	type spooled struct {
		modified time.Time
		point    map[string]any
	}
	var found []spooled
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".metrics") {
			continue
		}
		info, err := entry.Info()
		require.NoError(t, err)
		body, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		require.NoError(t, err)
		var point map[string]any
		require.NoError(t, json.Unmarshal(body, &point))
		found = append(found, spooled{modified: info.ModTime(), point: point})
	}
	sort.Slice(found, func(i, j int) bool { return found[i].modified.Before(found[j].modified) })

	points := make([]map[string]any, 0, len(found))
	for _, entry := range found {
		points = append(points, entry.point)
	}
	return points
}

func readMetricsPoint(t *testing.T, dir string) map[string]any {
	t.Helper()
	points := readMetricsPoints(t, dir)
	require.Len(t, points, 1)
	return points[0]
}
