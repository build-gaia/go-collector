package chronos_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	chronos "chronos.dev/collector/sdk/go"
)

func TestSQLOpenWritesChildSpan(t *testing.T) {
	dir := t.TempDir()
	client := chronos.StartWithConfig(chronos.Config{
		Enabled:      true,
		Organisation: "org-local",
		Project:      "project-a",
		Application:  "stock-analytics",
		SpoolDir:     dir,
		ServiceName:  "stock-analytics",
		APMEnabled:   true,
	})
	defer client.Shutdown(context.Background())

	driverName := "chronos_fake_" + strings.ReplaceAll(t.Name(), "/", "_")
	sql.Register(driverName, &fakeSQLDriver{})

	db, err := client.OpenDB(driverName, "mem")
	require.NoError(t, err)
	defer db.Close()

	ctx, parent := client.StartSpan(context.Background(), "GET /stock")
	defer parent.End()

	rows, err := db.QueryContext(ctx, "SELECT id FROM items WHERE client_id = ?", 42)
	require.NoError(t, err)
	require.NoError(t, rows.Close())

	_, err = db.ExecContext(ctx, "UPDATE items SET n = n + 1")
	require.NoError(t, err)

	parent.End()

	require.NoError(t, client.FlushSpans())
	traces := readTraceBodies(t, dir)
	require.NotEmpty(t, traces)

	var sqlSpans []map[string]any
	for _, body := range traces {
		var batch map[string]any
		require.NoError(t, json.Unmarshal([]byte(body), &batch))
		spans, _ := batch["spans"].([]any)
		for _, raw := range spans {
			span, _ := raw.(map[string]any)
			name, _ := span["name"].(string)
			if strings.HasPrefix(name, "SQL ") {
				sqlSpans = append(sqlSpans, span)
			}
		}
	}
	require.Len(t, sqlSpans, 2)

	byName := map[string]map[string]any{}
	for _, span := range sqlSpans {
		name, _ := span["name"].(string)
		byName[name] = span
	}
	selectSpan := byName["SQL SELECT"]
	require.NotNil(t, selectSpan)
	attrs, _ := selectSpan["attributes"].(map[string]any)
	require.Equal(t, strings.ToLower(driverName), attrs["db.system"])
	require.Equal(t, "SELECT", attrs["db.statement.verb"])
	require.Contains(t, attrs["db.statement"], "SELECT id FROM items")
	require.Equal(t, "client", attrs["span.kind"])
	require.Equal(t, parent.SpanID, selectSpan["parentSpanId"])
	require.Equal(t, parent.TraceID, selectSpan["traceId"])

	require.NotNil(t, byName["SQL UPDATE"])
}

func TestSQLVerb(t *testing.T) {
	driverName := "chronos_fake_verb_" + strings.ReplaceAll(t.Name(), "/", "_")
	sql.Register(driverName, &fakeSQLDriver{})

	dir := t.TempDir()
	client := chronos.StartWithConfig(chronos.Config{
		Enabled:      true,
		Organisation: "org-local",
		Project:      "project-a",
		Application:  "app",
		SpoolDir:     dir,
		APMEnabled:   true,
	})
	defer client.Shutdown(context.Background())

	db, err := client.OpenDB(driverName, "mem")
	require.NoError(t, err)
	defer db.Close()

	ctx, span := client.StartSpan(context.Background(), "job")
	defer span.End()
	_, err = db.ExecContext(ctx, "\n  insert into t values (1)")
	require.NoError(t, err)
	span.End()

	require.NoError(t, client.FlushSpans())
	found := false
	for _, body := range readTraceBodies(t, dir) {
		if strings.Contains(body, `"name":"SQL INSERT"`) {
			found = true
			break
		}
	}
	require.True(t, found)
}

func TestSQLWithoutParentEmitsNothing(t *testing.T) {
	dir := t.TempDir()
	client := chronos.StartWithConfig(chronos.Config{
		Enabled:      true,
		Organisation: "org-local",
		Project:      "project-a",
		Application:  "app",
		SpoolDir:     dir,
		APMEnabled:   true,
	})
	defer client.Shutdown(context.Background())

	driverName := "chronos_fake_orphan_" + strings.ReplaceAll(t.Name(), "/", "_")
	sql.Register(driverName, &fakeSQLDriver{})

	db, err := client.OpenDB(driverName, "mem")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.ExecContext(context.Background(), "SELECT 1")
	require.NoError(t, err)

	require.NoError(t, client.FlushSpans())
	require.Empty(t, readTraceBodies(t, dir))
}

func readTraceBodies(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var out []string
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".trace") {
			continue
		}
		body, err := os.ReadFile(filepath.Join(dir, e.Name()))
		require.NoError(t, err)
		out = append(out, string(body))
	}
	return out
}

// Minimal driver implementing QueryerContext / ExecerContext for unit tests.
type fakeSQLDriver struct{}

func (d *fakeSQLDriver) Open(name string) (driver.Conn, error) {
	return &fakeSQLConn{}, nil
}

type fakeSQLConn struct{}

func (c *fakeSQLConn) Prepare(query string) (driver.Stmt, error) {
	return &fakeSQLStmt{query: query}, nil
}
func (c *fakeSQLConn) Close() error              { return nil }
func (c *fakeSQLConn) Begin() (driver.Tx, error) { return nil, driver.ErrSkip }

func (c *fakeSQLConn) QueryContext(_ context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	return &fakeSQLRows{}, nil
}

func (c *fakeSQLConn) ExecContext(_ context.Context, _ string, _ []driver.NamedValue) (driver.Result, error) {
	return driver.RowsAffected(1), nil
}

type fakeSQLStmt struct{ query string }

func (s *fakeSQLStmt) Close() error  { return nil }
func (s *fakeSQLStmt) NumInput() int { return -1 }
func (s *fakeSQLStmt) Exec([]driver.Value) (driver.Result, error) {
	return driver.RowsAffected(1), nil
}
func (s *fakeSQLStmt) Query([]driver.Value) (driver.Rows, error) {
	return &fakeSQLRows{}, nil
}

type fakeSQLRows struct{ closed bool }

func (r *fakeSQLRows) Columns() []string { return []string{"id"} }
func (r *fakeSQLRows) Close() error      { r.closed = true; return nil }
func (r *fakeSQLRows) Next([]driver.Value) error {
	return io.EOF
}

func TestSQLWritesExactIOProfileSamples(t *testing.T) {
	dir := t.TempDir()
	client := chronos.StartWithConfig(chronos.Config{
		Enabled:          true,
		Organisation:     "org-local",
		Project:          "project-a",
		Application:      "stock-analytics",
		SpoolDir:         dir,
		ServiceName:      "stock-analytics",
		APMEnabled:       true,
		ProfilerEnabled:  true,
		IOProfileEnabled: true,
		IOMaxSamples:     16,
	})
	defer client.Shutdown(context.Background())

	driverName := "chronos_fake_" + strings.ReplaceAll(t.Name(), "/", "_")
	sql.Register(driverName, &fakeSQLDriver{})
	db, err := client.OpenDB(driverName, "mem")
	require.NoError(t, err)
	defer db.Close()

	ctx, parent := client.StartSpan(context.Background(), "job stock-42")
	rows, err := db.QueryContext(ctx, "SELECT id FROM items WHERE client_id = ?", 42)
	require.NoError(t, err)
	require.NoError(t, rows.Close())
	parent.End()

	require.NoError(t, client.Flush())

	samples := readProfileSamples(t, dir, "go-io")
	require.NotEmpty(t, samples, "expected an IO sample for the query")

	sample := samples[0]
	require.Equal(t, "PROFILE_SAMPLE_TYPE_IO", sample["sampleType"])
	require.Equal(t, "nanoseconds", sample["unit"])
	// Exact measurements carry no sampling period to scale up.
	require.Equal(t, "0", sample["periodNanoseconds"])

	labels, _ := sample["labels"].(map[string]any)
	require.Equal(t, "io", labels["wait"])
	require.Equal(t, strings.ToLower(driverName), labels["db.system"])

	// The operation is the flame chart's leaf, under the caller's own frames.
	stack, _ := sample["stack"].([]any)
	require.NotEmpty(t, stack)
	leaf, _ := stack[len(stack)-1].(map[string]any)
	require.Equal(t, "SQL SELECT", leaf["function"])

	// The sample points back at the trace that issued the query.
	correlation, _ := sample["correlation"].(map[string]any)
	require.NotNil(t, correlation)
	require.Equal(t, parent.TraceID, correlation["traceId"])
}

func TestSQLSkipsIOSamplesBelowMinWait(t *testing.T) {
	dir := t.TempDir()
	client := chronos.StartWithConfig(chronos.Config{
		Enabled:          true,
		Organisation:     "org-local",
		Project:          "project-a",
		Application:      "stock-analytics",
		SpoolDir:         dir,
		ServiceName:      "stock-analytics",
		ProfilerEnabled:  true,
		IOProfileEnabled: true,
		// The fake driver returns instantly, so every query is below this floor.
		IOMinWait: time.Hour,
	})
	defer client.Shutdown(context.Background())

	driverName := "chronos_fake_" + strings.ReplaceAll(t.Name(), "/", "_")
	sql.Register(driverName, &fakeSQLDriver{})
	db, err := client.OpenDB(driverName, "mem")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.ExecContext(context.Background(), "UPDATE items SET n = n + 1")
	require.NoError(t, err)

	require.NoError(t, client.FlushProfiles())
	require.Empty(t, readProfileSamples(t, dir, "go-io"))
}

// readProfileSamples returns every spooled sample belonging to one profile series.
func readProfileSamples(t *testing.T, dir, seriesID string) []map[string]any {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var out []map[string]any
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".profile") {
			continue
		}
		body, err := os.ReadFile(filepath.Join(dir, e.Name()))
		require.NoError(t, err)
		var batch struct {
			Samples []map[string]any `json:"samples"`
		}
		require.NoError(t, json.Unmarshal(body, &batch))
		for _, sample := range batch.Samples {
			if sample["profileSeriesId"] == seriesID {
				out = append(out, sample)
			}
		}
	}
	return out
}

// The connection identity the engine's data-source sweep reads must reach the
// span, not just the parser: the DSN is seen at Open/OpenConnector and the span
// is written per statement, so the two are wired through the driver and the
// prepared statement separately.
func TestSQLSpanCarriesConnectionIdentity(t *testing.T) {
	dir := t.TempDir()
	client := chronos.StartWithConfig(chronos.Config{
		Enabled:      true,
		Organisation: "org-local",
		Project:      "project-a",
		Application:  "stock-analytics",
		SpoolDir:     dir,
		ServiceName:  "stock-analytics",
		APMEnabled:   true,
	})
	defer client.Shutdown(context.Background())

	driverName := "chronos_fake_dsn_" + strings.ReplaceAll(t.Name(), "/", "_")
	sql.Register(driverName, &fakeSQLDriver{})

	db, err := client.OpenDB(driverName, "fake://reader:hunter2@db-1.internal:1234/analytics")
	require.NoError(t, err)
	defer db.Close()

	ctx, parent := client.StartSpan(context.Background(), "GET /stock")

	rows, err := db.QueryContext(ctx, "SELECT id FROM items")
	require.NoError(t, err)
	require.NoError(t, rows.Close())

	// The prepared-statement path carries the target separately from the
	// connection's own Query/Exec fast paths.
	stmt, err := db.PrepareContext(ctx, "UPDATE items SET n = n + 1")
	require.NoError(t, err)
	_, err = stmt.ExecContext(ctx)
	require.NoError(t, err)
	require.NoError(t, stmt.Close())

	parent.End()
	require.NoError(t, client.FlushSpans())

	var sqlSpans []map[string]any
	for _, body := range readTraceBodies(t, dir) {
		var batch map[string]any
		require.NoError(t, json.Unmarshal([]byte(body), &batch))
		spans, _ := batch["spans"].([]any)
		for _, raw := range spans {
			span, _ := raw.(map[string]any)
			if name, _ := span["name"].(string); strings.HasPrefix(name, "SQL ") {
				sqlSpans = append(sqlSpans, span)
			}
		}
	}
	require.Len(t, sqlSpans, 2)

	for _, span := range sqlSpans {
		attrs, _ := span["attributes"].(map[string]any)
		require.Equal(t, "db-1.internal", attrs["db.host"], span["name"])
		require.Equal(t, "db-1.internal", attrs["server.address"], span["name"])
		require.Equal(t, "1234", attrs["server.port"], span["name"])
		require.Equal(t, "1234", attrs["net.peer.port"], span["name"])
		require.Equal(t, "analytics", attrs["db.name"], span["name"])
		require.Equal(t, "reader", attrs["db.user"], span["name"])
		require.Equal(t, driverName, attrs["db.driver"], span["name"])
		for key, value := range attrs {
			require.NotEqual(t, "hunter2", value, "password reached attribute %s", key)
		}
	}
}

// The values a statement ran with are what let the desktop rebuild it for
// EXPLAIN, so they must reach the span on both the connection's Query/Exec fast
// path and the prepared-statement path.
func TestSQLSpanCarriesBoundParameters(t *testing.T) {
	dir := t.TempDir()
	client := chronos.StartWithConfig(chronos.Config{
		Enabled:      true,
		Organisation: "org-local",
		Project:      "project-a",
		Application:  "stock-analytics",
		SpoolDir:     dir,
		ServiceName:  "stock-analytics",
		APMEnabled:   true,
	})
	defer client.Shutdown(context.Background())

	driverName := "chronos_fake_params_" + strings.ReplaceAll(t.Name(), "/", "_")
	sql.Register(driverName, &fakeSQLDriver{})

	db, err := client.OpenDB(driverName, "mem")
	require.NoError(t, err)
	defer db.Close()

	ctx, parent := client.StartSpan(context.Background(), "GET /stock")

	rows, err := db.QueryContext(ctx, "SELECT id FROM items WHERE client_id = ? AND state = ?", 42, "active")
	require.NoError(t, err)
	require.NoError(t, rows.Close())

	stmt, err := db.PrepareContext(ctx, "UPDATE items SET n = ? WHERE client_id = ?")
	require.NoError(t, err)
	_, err = stmt.ExecContext(ctx, 7, 42)
	require.NoError(t, err)
	require.NoError(t, stmt.Close())

	// A statement with no bindings writes neither attribute.
	_, err = db.ExecContext(ctx, "DELETE FROM items")
	require.NoError(t, err)

	parent.End()
	require.NoError(t, client.FlushSpans())

	byName := map[string]map[string]any{}
	for _, body := range readTraceBodies(t, dir) {
		var batch map[string]any
		require.NoError(t, json.Unmarshal([]byte(body), &batch))
		spans, _ := batch["spans"].([]any)
		for _, raw := range spans {
			span, _ := raw.(map[string]any)
			if name, _ := span["name"].(string); strings.HasPrefix(name, "SQL ") {
				byName[name] = span
			}
		}
	}

	selectAttrs, _ := byName["SQL SELECT"]["attributes"].(map[string]any)
	require.Equal(t, `["42","active"]`, selectAttrs["db.parameters"])
	require.Equal(t, "2", selectAttrs["db.parameters.count"])

	updateAttrs, _ := byName["SQL UPDATE"]["attributes"].(map[string]any)
	require.Equal(t, `["7","42"]`, updateAttrs["db.parameters"])
	require.Equal(t, "2", updateAttrs["db.parameters.count"])

	deleteAttrs, _ := byName["SQL DELETE"]["attributes"].(map[string]any)
	require.NotContains(t, deleteAttrs, "db.parameters")
	require.NotContains(t, deleteAttrs, "db.parameters.count")
}
