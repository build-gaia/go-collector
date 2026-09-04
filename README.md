# Chronos Go SDK

Go telemetry SDK for Chronos. Applications write spool files conforming to
[`proto/SPOOL_CONTRACT.md`](../../proto/SPOOL_CONTRACT.md); the collector sidecar
ships them to `engine-ingest`.

Status: **implemented** (first consumer: `service-stock-analytics`).

## Signals

| Signal | Source | Series / schema |
| --- | --- | --- |
| Traces | HTTP middleware + manual spans | `chronos.tracing.span-batch.v1` → `.trace` |
| Logs | `Client.Log`, `slog` bridge, logrus hook | `chronos.tracing.log-batch.v1` → `.log` |
| Profiles | CPU, wall, off-CPU, allocation, lock, I/O — **process-wide** (workers included) | `chronos.profiling.sample-batch.v1` → `.profile` (see [Profile types](#profile-types)) |
| Metrics | Go runtime memstats / goroutines | `chronos.runtime.metrics.v1` → `.metrics` (see [Runtime metrics](#runtime-metrics)) |

DST recording is not supported in v1 (`CHRONOS_DST_ENABLED` is ignored).

## Install (PHP-like: env + one Start call)

Go has no Zend extension equivalent. The non-intrusive surface is:

1. Set `CHRONOS_*` identity + spool (same idea as PHP `.chronos` / env).
2. Call `chronos.Start()` once at process boot (enables **process-wide**
   profiles/metrics; workers are included automatically via `runtime/pprof`).
3. Wrap the root HTTP handler once (`client.Handler`) — traces plus `route` /
   `http.method` pprof labels for profile rollups.
4. Open SQL via `chronos.Open(driver, dsn)` instead of `sql.Open` (one call site) —
   `QueryContext` / `ExecContext` emit `SQL <VERB>` child spans with `db.*` attrs
   (see [SQL connection identity](#sql-connection-identity)).
5. Optionally label job dispatch with `chronos.Do` / `client.Run` (sets `job=`).

Do **not** sprinkle Chronos into every business function — continuous profiling
already samples every goroutine; labels only need a few entrypoints.

Runtime / `pkg/mod` / vendor frames are tagged `module=internal` so the desktop
can dim them like PHP builtins. Application frames are grouped by their **package**,
shortened against the main module — a frame in
`service-stock-analytics/internal/service/stock` shows as `internal/service/stock`.
(The pprof mapping file is not used for this: a statically linked Go binary has one
mapping, so every application frame would carry an identical module name.)

## Kafka (and other messaging)

`contrib/sarama` wraps IBM/sarama so a publish and a consume are spans:

```go
// producer — one PUBLISH <topic> span, and the trace stamped onto the message
traced := chronossarama.WrapSyncProducer(producer, nil, brokers)
partition, offset, err := traced.SendMessageContext(ctx, message)

// consumer — one PROCESS <topic> span per message, parented onto that publish
handler := chronossarama.WrapConsumerGroupHandler(myHandler, nil, groupID)
```

A separate module (`chronos.dev/collector/sdk/go/contrib/sarama`), for the reason
dd-trace-go splits its contribs: a service that speaks no Kafka should not compile
sarama to get HTTP tracing.

The spans carry the OTel messaging attributes — `messaging.system`,
`messaging.operation`, `messaging.destination.name`, partition, offset, body size,
consumer group. Those are not decoration: the desktop's **Producers** view joins
`messaging.destination.name` to a stream and reads the direction off
`messaging.operation`, so instrumenting a service is what turns that view's "not
instrumented" into a named writer, and what flips a dependency-graph edge from
uninstrumented to instrumented.

The producer writes the active span as a W3C `traceparent` message header and the
consumer reads it back, so work caused by a message is a child of the publish that
caused it — across processes and across languages, since the PHP collector reads
the same header. A message without the header starts its own trace rather than
being dropped.

For a transport with no wrapper yet, `client.RecordMessaging` / `StartMessagingSpan`
in the core SDK take the same attributes with no Kafka types involved.

## Profile types

The same five sample types the PHP sampler produces, plus lock contention. One series
per type — the desktop resolves a chosen sample type to a single series.

| Series | Type | Source | Attribution |
| --- | --- | --- | --- |
| `go-cpu` | `CPU` | `runtime/pprof` CPU profile | On-CPU nanoseconds |
| `go-wall` | `WALL` | Goroutine snapshots × tick interval | Every goroutine, running or parked |
| `go-offcpu` | `OFF_CPU` | The parked subset of the same snapshots | Existed but did not run |
| `go-heap` | `ALLOCATION` | `runtime/pprof` heap profile | In-use bytes |
| `go-lock` | `LOCK` | mutex + block profiles (`lock.kind` label) | Contention nanoseconds |
| `go-io` | `IO` | Exact, from the SQL driver and HTTP transport | Real duration, real stack |

Every sampled record carries a `wait` label (`running`, `io`, `lock`, `chan`, `sleep`,
`parked`) so a flame chart can be split by why the time was spent.

**Why wall/off-CPU are sampled from goroutines rather than labelled.** A
`runtime/pprof` label set lives on the goroutine that set it, and Go does not carry
labels across a channel handoff. A service that dispatches work to a worker pool
started at boot therefore attributes almost none of its real work: the labelled
goroutine sits in `WaitGroup.Wait` while unlabelled workers do the job. Sampling the
goroutine set observes every goroutine wherever it is, so attribution never depends on
where the goroutine was created — and it needs no application change.

**Idle workers are excluded by default.** A pool sized for peak spends most of its wall
time parked on its job channel, and counting that buries every real stack. Samples
classified `wait=chan` are dropped from `go-wall`, `go-offcpu` and `go-lock`; set
`CHRONOS_GO_WALL_EXCLUDE_IDLE=false` to measure queue wait instead.

**I/O is measured, not sampled.** The driver `chronos.Open` installs brackets every
statement, so an I/O sample carries the real duration and the real stack at the
blocking call — the same trade Zend's observer makes for PHP's `PDO::` / `curl_exec`.
The cost is coverage: only clients the SDK wraps are seen. Cover another one yourself
with `client.RecordIOWait(ctx, "REDIS GET", elapsed)`.

## Runtime metrics

The metric point names **quantities, not languages** (ADR 0024 §5). A heap, a GC
and a request count are not Go's or PHP's, and until that change this SDK emitted
`process.runtime.php.request.count` from a Go process because that was the name
the engine already knew.

| Key | From |
| --- | --- |
| `process.runtime.request.count` / `.duration_ms` / `.errors` | The HTTP middleware's counters, drained per point |
| `process.runtime.memory.heap_alloc_bytes` / `.heap_inuse_bytes` / `.heap_sys_bytes` / `.stack_inuse_bytes` | `runtime.MemStats` |
| `process.runtime.gc.collections` / `.gc.pause_ms` | `MemStats.NumGC` / `PauseTotalNs`, **as a delta** |
| `process.runtime.threads` | `runtime.NumGoroutine()` |

Two of those need their reading spelled out:

**GC is a delta, not a total.** Go reports `NumGC` and `PauseTotalNs` since
process start; the PHP extension reports collections and pause time per request.
Those are different quantities and cannot share a key, so this SDK subtracts its
own previous reading and publishes the work done *in this window* — which is what
the PHP numbers already meant. The first point after boot reports the whole
history, correctly: that is the window since the process started.

**`threads` counts goroutines.** A goroutine is not a thread, but the question
asked of the series — how many concurrent things is this process carrying, and is
it climbing — is the same one asked of a thread count or an FPM child count, and a
Go-only key answers it for one language only.

## Process identity on every span

Every span carries the `app.*` family the PHP extension has always sent, so a Go
service is not language-unknown in the desktop:

| Attribute | Value |
| --- | --- |
| `app.language` | `go` |
| `app.language.version` | `runtime.Version()` with the `go` prefix removed (`1.24.1`) |
| `app.version`, `service.version` | `CHRONOS_SERVICE_VERSION`, when set |
| `app.commit` | `vcs.revision` from the linker's build info |
| `app.dirty` | `true` when the build had uncommitted changes |

`app.commit` needs no build flag and no `.git` directory in the container: the Go
linker stamps the revision into any binary built inside a checkout (it is absent
when built from a module cache or with `-buildvcs=false`). `app.branch` has no Go
equivalent — build info records the revision, not the branch.

## SQL connection identity

A statement span carries the statement (`db.system`, `db.statement`,
`db.query.text`, `db.statement.verb`, `db.operation`) **and the connection it ran
on**:

| Attribute | From |
| --- | --- |
| `db.host`, `server.address` | DSN host, or the socket path for a unix DSN |
| `server.port`, `net.peer.port` | DSN port, or the driver's default when the DSN omits it |
| `db.name` | DSN database / schema |
| `db.user` | DSN user |
| `db.driver` | The driver name passed to `chronos.Open` |

This is not decoration. The engine's data-source detection sweep reads `db.host`
/ `db.name` / `db.user` (plus a port) off recent spans to mint the connection
coordinates it later provisions a read-only account against and runs `EXPLAIN`
through. A statement span without them can be timed and fingerprinted but can
never get an execution plan.

The DSN is parsed once per distinct DSN, at `Open` / `OpenConnector`. Recognised
forms: go-sql-driver MySQL (`user:pass@tcp(host:port)/db`, including `unix(...)`),
libpq/pgx URL and `key=value` forms, `sqlserver://` and its ADO `;`-separated
form, SQLite paths (file name only), and any other `scheme://` DSN. An
unrecognised DSN yields **no** connection attributes rather than a guess — a
wrong host mints a data source an operator then has to disown.

**The password is never held.** It is read only to find where the user name ends,
and is asserted absent from every emitted field in `sql_dsn_test.go`.

## SQL bound parameters

A statement span also carries the values it ran with, on the same two attributes
the PHP collector writes:

| Attribute | Value |
| --- | --- |
| `db.parameters` | JSON array of the bound values as strings, in ordinal order |
| `db.parameters.count` | How many were bound, **before** capping |

Connection identity gets a statement an `EXPLAIN`; the parameters get it a
*useful* one. The desktop's DB screen binds this array by ordinal to turn a
fingerprint back into the exact query that was slow, and the engine samples
parameters across the latency distribution so the pathological run can be
explained beside a healthy one.

At most **32** values are rendered, each capped to **64 bytes**, so a statement
bound with a large blob or a long `IN` list cannot turn one span into an
unbounded payload. `db.parameters.count` is the true count, so a capped list is
visible as such. A statement that bound nothing writes neither attribute.

Values are rendered like PHP's bindings: `nil` as the empty string, `[]byte` as
its text, `time.Time` as RFC 3339. Capping is by byte and may split a rune;
the result is still valid JSON.

## Correlating CPU samples with traces

`client.Run` and `client.Handler` stamp `trace_id` / `span_id` as pprof labels, which
`NormalizePprof` lifts into each sample's `correlation` field — so a CPU flame frame
links back to the trace that produced it.

Those labels cannot cross a worker-pool channel (see above), so CPU samples taken on a
pool goroutine are uncorrelated unless the handoff is instrumented. `chronos.Bind`
makes that one line at the dispatch site:

```go
run := chronos.Bind(ctx, "chunk-"+id)          // capture on the dispatching goroutine
pool.Enqueue(func() {                           // …restore inside the worker
    _ = run(func(ctx context.Context) error { return job.Run(ctx) })
})
```

This is only needed for CPU correlation and for giving pool work a child span. Wall,
off-CPU and I/O attribution already work without it.

## Configuration

Shared variables (see spool contract):

| Variable | Required | Default |
| --- | --- | --- |
| `CHRONOS_ENABLED` | no | `true` |
| `CHRONOS_ORGANISATION_ID` | yes | — |
| `CHRONOS_PROJECT_ID` | yes | — |
| `CHRONOS_APPLICATION_ID` | yes | — |
| `CHRONOS_SPOOL_DIRECTORY` | yes | — |
| `CHRONOS_APM_ENABLED` | no | `true` |
| `CHRONOS_LOGS_ENABLED` | no | `true` |
| `CHRONOS_PROFILER_ENABLED` | no | `false` |
| `CHRONOS_METRICS_ENABLED` | no | `true` |
| `CHRONOS_SERVICE_NAME` | no | application id |
| `CHRONOS_SERVICE_VERSION` | no | — |

Go-specific:

| Variable | Default | Description |
| --- | --- | --- |
| `CHRONOS_GO_PROFILE_INTERVAL` | `90s` | CPU/heap sample cadence, and the flush cadence for wall/off-CPU/IO (must exceed duration) |
| `CHRONOS_GO_PROFILE_DURATION` | `60s` | CPU sample window |
| `CHRONOS_GO_PROFILE_MAX_STACK_DEPTH` | `64` | Frames captured per wall / IO stack |
| `CHRONOS_GO_PROFILE_BATCH_SIZE` | `500` | Samples per spool document |
| `CHRONOS_GO_WALL_ENABLED` | `true` | Goroutine-snapshot sampler behind `WALL` / `OFF_CPU` |
| `CHRONOS_GO_WALL_INTERVAL` | `100ms` | Snapshot period. The overhead knob — a snapshot briefly stops the world |
| `CHRONOS_GO_WALL_EXCLUDE_IDLE` | `true` | Drop goroutines parked on a channel/select |
| `CHRONOS_GO_WALL_MAX_STACKS` | `4096` | Distinct stacks tracked per flush window |
| `CHRONOS_GO_IO_ENABLED` | `true` | Exact IO samples from instrumented clients |
| `CHRONOS_GO_IO_MIN_WAIT` | `1ms` | Floor below which a call is not worth a stack walk |
| `CHRONOS_GO_IO_MAX_SAMPLES` | `4096` | IO samples buffered per flush window |
| `CHRONOS_GO_SPAN_BATCH_SIZE` | `200` | Finished spans per `.trace` document |
| `CHRONOS_GO_SPAN_FLUSH_INTERVAL` | `5s` | How long a partly-filled span batch waits |
| `CHRONOS_GO_SPAN_MAX_BUFFERED` | `10000` | Spans held in memory before shedding (see `Client.DroppedSpans`) |
| `CHRONOS_GO_METRICS_INTERVAL` | `15s` | Runtime metrics cadence |
| `CHRONOS_GO_HTTP_CAPTURE` | `true` | Capture HTTP headers on server spans |
| `CHRONOS_GO_HTTP_CAPTURE_BODIES` | `false` | Capture request/response bodies |
| `CHRONOS_GO_HTTP_CAPTURE_MAX_BODY` | `65536` | Per-body cap (`text/html` responses use 100 MiB) |
| `CHRONOS_GO_HTTP_SKIP_PATHS` | `/status,/health,/healthz,/ready,/readyz,/livez,/ws,/job/status,/api/stats,/api/running-jobs,/api/failed-jobs-paginated` | Exact URL paths that skip APM spans (probes / UI polls) |

Without organisation/project/application/spool, the SDK stays **inert** (no spans, no profiles, no disk writes).

## Span batching

Finished spans accumulate into one `.trace` document instead of one file per span. A
client job that dispatches 5,000 chunks, each issuing queries, would otherwise produce
tens of thousands of files for the sidecar to stat, read, ship and unlink — the per-file
cost dominates long before the ingest route does.

A batch is written when it reaches `CHRONOS_GO_SPAN_BATCH_SIZE`, on the flush ticker, and
on `Shutdown`. The size-triggered flush runs inline on whichever goroutine ended the span
that filled the buffer, which keeps backpressure visible rather than hiding an unbounded
queue behind a channel.

Spans live in memory until flushed, so an abrupt kill loses at most one window. If the
spool stops accepting writes, the unwritten batch goes back into the buffer and is retried
— a spool that recovers loses nothing. Past `CHRONOS_GO_SPAN_MAX_BUFFERED` spans are shed
and counted; a non-zero `client.DroppedSpans()` means the spool is not draining.

Short-lived processes and tests that read the spool directly should call
`client.FlushSpans()` (or `client.Flush()` for spans plus profiles) before asserting.

## Design notes

- **No Go equivalent of `chronos.so`.** Go already exposes `runtime/pprof`; this SDK normalizes pprof into Chronos `ProfileSampleBatch` JSON (root-first stacks).
- Stack order matches the PHP sampler so desktop flame graphs stay consistent.
- Redaction happens before spool write.
- Atomic spool writes: `tmp-{uuid}` → `{sha256}.{signal}`.

## Shipping

Telemetry lands as files in `CHRONOS_SPOOL_DIRECTORY`. Ship them with the same
polyglot forwarder PHP uses — no Go-specific agent:

```bash
# local chronos-local stack (operator/deploy/local)
./operator/deploy/local/start-forwarders.sh
# starts chronos-fwd-stock-analytics → data/chronos-spool → engine-ingest
```

Or run `chronos-collector` / `chronos-engine-agent` yourself against the spool
directory with `CHRONOS_INGEST_URL` + `CHRONOS_INGEST_TOKEN`.
