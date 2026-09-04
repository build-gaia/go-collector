package chronos

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
)

const (
	maxSQLTextLength = 16384
	maxCachedDSNs    = 64
)

var (
	sqlDriversMu sync.Mutex
	sqlDrivers   = map[string]string{} // parent driver name → chronos-wrapped name
)

// Open opens a database with Chronos instrumentation using the process-wide
// Default client. The named driver must already be registered (blank-import it).
//
// Replace sql.Open(driver, dsn) with chronos.Open(driver, dsn). Existing
// QueryContext/ExecContext call sites keep working; each statement becomes a
// child span under the ambient context span (HTTP / job).
func Open(driverName, dsn string) (*sql.DB, error) {
	return Default().OpenDB(driverName, dsn)
}

// OpenDB opens a database with Chronos instrumentation for this client.
func (c *Client) OpenDB(driverName, dsn string) (*sql.DB, error) {
	if c == nil {
		return sql.Open(driverName, dsn)
	}
	name, err := c.ensureSQLDriver(driverName)
	if err != nil {
		return nil, err
	}
	return sql.Open(name, dsn)
}

func (c *Client) ensureSQLDriver(parentName string) (string, error) {
	sqlDriversMu.Lock()
	defer sqlDriversMu.Unlock()
	if name, ok := sqlDrivers[parentName]; ok {
		return name, nil
	}

	probe, err := sql.Open(parentName, "")
	if err != nil {
		return "", fmt.Errorf("chronos: unknown sql driver %q: %w", parentName, err)
	}
	parent := probe.Driver()
	_ = probe.Close()

	wrappedName := "chronos-" + parentName
	sql.Register(wrappedName, &sqlDriver{
		parent: parent,
		client: c,
		name:   parentName,
		system: systemFromDriver(parentName),
	})
	sqlDrivers[parentName] = wrappedName
	return wrappedName, nil
}

func systemFromDriver(name string) string {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "mysql":
		return "mysql"
	case "sqlite", "sqlite3":
		return "sqlite"
	case "postgres", "postgresql", "pgx", "pq":
		return "postgresql"
	case "sqlserver", "mssql":
		return "mssql"
	default:
		if name == "" {
			return "sql"
		}
		return strings.ToLower(name)
	}
}

func sqlVerb(query string) string {
	trimmed := strings.TrimLeftFunc(query, unicode.IsSpace)
	if trimmed == "" {
		return "QUERY"
	}
	end := 0
	for end < len(trimmed) && !unicode.IsSpace(rune(trimmed[end])) {
		end++
	}
	verb := strings.ToUpper(trimmed[:end])
	if verb == "" {
		return "QUERY"
	}
	return verb
}

func truncateSQL(s string) string {
	if len(s) <= maxSQLTextLength {
		return s
	}
	return s[:maxSQLTextLength]
}

// sqlOp brackets one statement: the APM span (nil when there is no ambient trace to
// hang it under) plus the timing the IO profiler needs. A statement is measured even
// when it is not traced — an untraced query still blocks a goroutine.
type sqlOp struct {
	drv   *sqlDriver
	span  *Span
	ctx   context.Context
	verb  string
	start time.Time
}

// target parses connection identity out of a DSN once per distinct DSN. A pool
// opens and reopens connections for the life of the process, and every one of
// those arrives here with the same string.
func (d *sqlDriver) target(dsn string) connTarget {
	d.targetsMu.Lock()
	defer d.targetsMu.Unlock()
	if target, ok := d.targets[dsn]; ok {
		return target
	}
	target := parseDSN(d.system, d.name, dsn)
	if d.targets == nil {
		d.targets = map[string]connTarget{}
	}
	// One entry per DSN, and a process has a handful. The cap is a guard against
	// a caller that mints DSNs per request, not a working limit.
	if len(d.targets) < maxCachedDSNs {
		d.targets[dsn] = target
	}
	return target
}

func (d *sqlDriver) startSQLSpan(ctx context.Context, query string, target connTarget, params paramList) (context.Context, *sqlOp) {
	op := &sqlOp{drv: d, ctx: ctx, verb: sqlVerb(query), start: time.Now()}
	// SQL spans must nest under an HTTP/job (or manual) root. Emitting root
	// traces per statement floods the trace list and hides real entrypoints.
	if SpanFromContext(ctx) == nil {
		return ctx, op
	}
	client := d.client
	if client == nil {
		client = Default()
	}
	ctx, span := client.StartSpan(ctx, "SQL "+op.verb)
	span.SetAttribute("span.kind", "client")
	span.SetAttribute("db.system", d.system)
	span.SetAttribute("db.statement.verb", op.verb)
	span.SetAttribute("db.operation", op.verb)
	stmt := truncateSQL(query)
	span.SetAttribute("db.statement", stmt)
	span.SetAttribute("db.query.text", stmt)
	setTargetAttributes(span, target)
	setParameterAttributes(span, params)
	op.span = span
	op.ctx = ctx
	return ctx, op
}

func finishSQLSpan(op *sqlOp, err error) {
	if op == nil {
		return
	}
	op.recordIO()
	span := op.span
	if span == nil {
		return
	}
	if err != nil {
		span.SetStatus("error")
		span.SetAttribute("error", "true")
		span.SetAttribute("exception.message", err.Error())
	}
	span.End()
}

// recordIO banks the statement as an exact PROFILE_SAMPLE_TYPE_IO sample. The stack is
// captured here, at the call site, so the flame chart shows the application code that
// issued the query rather than the driver internals below it.
func (op *sqlOp) recordIO() {
	client := op.drv.client
	if client == nil {
		client = Default()
	}
	// skip: runtime.Callers, recordIOWait, recordIO, finishSQLSpan → the driver method.
	client.recordIOWait(op.ctx, "SQL "+op.verb, op.drv.system, time.Since(op.start), 5)
}

// abandonSpan marks a statement ended without writing (driver.ErrSkip fallback path):
// the call did not run, so it is neither a span nor measured IO.
func abandonSpan(op *sqlOp) {
	if op == nil || op.span == nil {
		return
	}
	op.span.mu.Lock()
	defer op.span.mu.Unlock()
	op.span.ended = true
}

// setTargetAttributes stamps connection identity on a statement span. These are
// the keys the engine's data-source detection sweep reads — `db.host` / `db.name`
// / `db.user` mint the source, `server.port` / `net.peer.port` give it a port —
// so without them a Go service's queries can never get an execution plan.
//
// `server.address` mirrors `db.host` because the service map keys datastore nodes
// off whichever of the two is present, and `net.peer.port` mirrors `server.port`
// for the same reason on the port. Empty fields are omitted rather than written
// blank: a missing attribute reads as unknown, an empty one reads as known-empty.
func setTargetAttributes(span *Span, target connTarget) {
	if target.empty() {
		return
	}
	if target.host != "" {
		span.SetAttribute("db.host", target.host)
		span.SetAttribute("server.address", target.host)
	}
	if target.port != "" {
		span.SetAttribute("server.port", target.port)
		span.SetAttribute("net.peer.port", target.port)
	}
	if target.name != "" {
		span.SetAttribute("db.name", target.name)
	}
	if target.user != "" {
		span.SetAttribute("db.user", target.user)
	}
	if target.driver != "" {
		span.SetAttribute("db.driver", target.driver)
	}
}

// setParameterAttributes stamps the values the statement actually ran with. The
// count is written even when the rendered list is empty so a reader can tell a
// statement that bound nothing from one whose values were dropped.
func setParameterAttributes(span *Span, params paramList) {
	count := params.len()
	if count == 0 {
		return
	}
	span.SetAttribute("db.parameters.count", strconv.Itoa(count))
	if rendered := boundedParametersJSON(params); rendered != "" {
		span.SetAttribute("db.parameters", rendered)
	}
}
