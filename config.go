package chronos

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is resolved from environment variables (see collector/proto/SPOOL_CONTRACT.md).
// Missing identity or spool directory leaves Enabled=false so the SDK is inert.
type Config struct {
	Enabled        bool
	Organisation   string
	Project        string
	Application    string
	SpoolDir       string
	ServiceName    string
	ServiceVersion string

	APMEnabled      bool
	LogsEnabled     bool
	ProfilerEnabled bool
	MetricsEnabled  bool
	DSTEnabled      bool

	ProfileInterval time.Duration
	ProfileDuration time.Duration
	MetricsInterval time.Duration

	// ProfileMaxStackDepth caps frames captured per stack (wall / IO samplers).
	ProfileMaxStackDepth int
	// ProfileBatchSize caps samples per spool document.
	ProfileBatchSize int

	// WallProfileEnabled turns on the goroutine-snapshot sampler behind WALL / OFF_CPU.
	WallProfileEnabled bool
	// WallSampleInterval is the snapshot period; each live goroutine is charged this
	// much wall time per tick. Shorter is more faithful and more expensive — a
	// goroutine snapshot briefly stops the world.
	WallSampleInterval time.Duration
	// WallExcludeIdle drops goroutines parked on a channel or select from WALL. A pool
	// sized for peak spends most of its wall time idle, and counting that buries every
	// real stack. Turn it off to measure queue wait.
	WallExcludeIdle bool
	// WallMaxStacks caps distinct stacks tracked per flush window.
	WallMaxStacks int

	// IOProfileEnabled records exact IO samples from instrumented clients (SQL, HTTP).
	IOProfileEnabled bool
	// IOMinWait is the floor below which a call is not worth a stack walk.
	IOMinWait time.Duration
	// IOMaxSamples caps IO samples buffered per flush window.
	IOMaxSamples int

	// SpanBatchSize is how many finished spans accumulate before a batch is spooled.
	SpanBatchSize int
	// SpanFlushInterval bounds how long a partly-filled batch waits.
	SpanFlushInterval time.Duration
	// SpanMaxBuffered caps spans held in memory; past it, spans are dropped and counted
	// (see Client.DroppedSpans) rather than growing the heap without limit.
	SpanMaxBuffered int

	HTTPCapture       bool
	HTTPCaptureBodies bool
	HTTPMaxBody       int
	HTTPRedact        bool
	RedactPatterns    []string
	// HTTPSkipPaths are URL paths that skip APM spans (probes / UI polls). Exact match.
	HTTPSkipPaths []string

	// Background starts profiler/metrics loops. Start() sets this true; tests leave it false.
	Background bool
}

// LoadConfig reads Chronos settings from the process environment.
func LoadConfig() Config {
	cfg := Config{
		Enabled:         envBool("CHRONOS_ENABLED", true),
		Organisation:    firstNonEmpty(os.Getenv("CHRONOS_ORGANISATION_ID"), os.Getenv("CHRONOS_ORGANISATION")),
		Project:         firstNonEmpty(os.Getenv("CHRONOS_PROJECT_ID"), os.Getenv("CHRONOS_PROJECT")),
		Application:     firstNonEmpty(os.Getenv("CHRONOS_APPLICATION_ID"), os.Getenv("CHRONOS_APPLICATION")),
		SpoolDir:        os.Getenv("CHRONOS_SPOOL_DIRECTORY"),
		ServiceName:     firstNonEmpty(os.Getenv("CHRONOS_SERVICE_NAME"), os.Getenv("CHRONOS_APPLICATION"), os.Getenv("CHRONOS_APPLICATION_ID")),
		ServiceVersion:  os.Getenv("CHRONOS_SERVICE_VERSION"),
		APMEnabled:      envBool("CHRONOS_APM_ENABLED", true),
		LogsEnabled:     envBool("CHRONOS_LOGS_ENABLED", true),
		ProfilerEnabled: envBool("CHRONOS_PROFILER_ENABLED", false),
		MetricsEnabled:  envBool("CHRONOS_METRICS_ENABLED", true),
		DSTEnabled:      envBool("CHRONOS_DST_ENABLED", false),
		// CPU profiling is process-global and exclusive: interval must exceed duration.
		ProfileInterval:      envDuration("CHRONOS_GO_PROFILE_INTERVAL", 90*time.Second),
		ProfileDuration:      envDuration("CHRONOS_GO_PROFILE_DURATION", 60*time.Second),
		MetricsInterval:      envDuration("CHRONOS_GO_METRICS_INTERVAL", 15*time.Second),
		ProfileMaxStackDepth: envInt("CHRONOS_GO_PROFILE_MAX_STACK_DEPTH", 64),
		ProfileBatchSize:     envInt("CHRONOS_GO_PROFILE_BATCH_SIZE", defaultProfileBatchSize),
		WallProfileEnabled:   envBool("CHRONOS_GO_WALL_ENABLED", true),
		WallSampleInterval:   envDuration("CHRONOS_GO_WALL_INTERVAL", 100*time.Millisecond),
		WallExcludeIdle:      envBool("CHRONOS_GO_WALL_EXCLUDE_IDLE", true),
		WallMaxStacks:        envInt("CHRONOS_GO_WALL_MAX_STACKS", 4096),
		IOProfileEnabled:     envBool("CHRONOS_GO_IO_ENABLED", true),
		IOMinWait:            envDuration("CHRONOS_GO_IO_MIN_WAIT", time.Millisecond),
		IOMaxSamples:         envInt("CHRONOS_GO_IO_MAX_SAMPLES", 4096),
		SpanBatchSize:        envInt("CHRONOS_GO_SPAN_BATCH_SIZE", defaultSpanBatchSize),
		SpanFlushInterval:    envDuration("CHRONOS_GO_SPAN_FLUSH_INTERVAL", defaultSpanFlushInterval),
		SpanMaxBuffered:      envInt("CHRONOS_GO_SPAN_MAX_BUFFERED", 10000),
		HTTPCapture:          envBool("CHRONOS_GO_HTTP_CAPTURE", true),
		HTTPCaptureBodies:    envBool("CHRONOS_GO_HTTP_CAPTURE_BODIES", false),
		HTTPMaxBody:          envInt("CHRONOS_GO_HTTP_CAPTURE_MAX_BODY", 65536),
		HTTPRedact:           envBool("CHRONOS_GO_HTTP_CAPTURE_REDACT", true),
		RedactPatterns:       envCSV("CHRONOS_GO_REDACT_PATTERNS", defaultRedactPatterns),
		HTTPSkipPaths:        envCSV("CHRONOS_GO_HTTP_SKIP_PATHS", defaultHTTPSkipPaths),
	}
	if !cfg.Enabled || cfg.Organisation == "" || cfg.Project == "" || cfg.Application == "" || cfg.SpoolDir == "" {
		cfg.Enabled = false
	}
	if cfg.ServiceName == "" {
		cfg.ServiceName = cfg.Application
	}
	return cfg
}

var defaultRedactPatterns = []string{
	"password", "passwd", "secret", "token", "authorization", "api_key", "apikey",
	"access_key", "private_key", "cookie", "set-cookie", "session", "credit", "card",
	"ssn", "cvv", "bearer",
}

// Health probes and common dashboard poll routes — keep Traces for real work.
var defaultHTTPSkipPaths = []string{
	"/status",
	"/health",
	"/healthz",
	"/ready",
	"/readyz",
	"/livez",
	"/ws",
	"/job/status",
	"/api/stats",
	"/api/running-jobs",
	"/api/failed-jobs-paginated",
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func envBool(key string, fallback bool) bool {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	switch strings.ToLower(raw) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}

func envInt(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return n
}

func envDuration(key string, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return fallback
	}
	return d
}

func envCSV(key string, fallback []string) []string {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return append([]string(nil), fallback...)
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return append([]string(nil), fallback...)
	}
	return out
}

// applyProfileDefaults fills profiling knobs left at their zero value. LoadConfig always
// sets them; a Config built by hand (tests, embedders) would otherwise disable stack
// capture by asking for a zero-depth stack.
func (c *Config) applyProfileDefaults() {
	if c.ProfileMaxStackDepth <= 0 {
		c.ProfileMaxStackDepth = 64
	}
	if c.ProfileBatchSize <= 0 {
		c.ProfileBatchSize = defaultProfileBatchSize
	}
	if c.WallSampleInterval <= 0 {
		c.WallSampleInterval = 100 * time.Millisecond
	}
	if c.WallMaxStacks <= 0 {
		c.WallMaxStacks = 4096
	}
	if c.IOMaxSamples <= 0 {
		c.IOMaxSamples = 4096
	}
}

// Span batching defaults. The batch size trades spool file count against how much a
// hard kill can lose; 200 turns a 5,000-chunk job from 5,000 files into 25.
const (
	defaultSpanBatchSize     = 200
	defaultSpanFlushInterval = 5 * time.Second
)
