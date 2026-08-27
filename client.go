package chronos

import (
	"context"
	"log/slog"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"chronos.dev/collector/sdk/go/spool"
)

// Client is the Chronos Go SDK entrypoint. Safe for concurrent use after Start.
type Client struct {
	cfg    Config
	writer spool.Writer

	cancel context.CancelFunc
	wg     sync.WaitGroup

	wall  *wallSampler
	io    *ioRecorder
	spans *spanBuffer

	httpRequests atomic.Uint64
	httpErrors   atomic.Uint64
	httpDuration atomic.Uint64 // nanoseconds, summed

	// Previous cumulative GC counters, so the shared gc.* keys can report the
	// per-window delta the PHP extension already reports. See Client.gcDelta.
	gcNum     atomic.Uint32
	gcPauseNs atomic.Uint64
}

// Start constructs a client from environment config and starts background
// profilers/metrics when enabled. Returns a no-op client when Chronos is disabled.
func Start() *Client {
	cfg := LoadConfig()
	cfg.Background = true
	return StartWithConfig(cfg)
}

// StartWithConfig starts a client with an explicit config (tests / custom wiring).
// Background loops run only when cfg.Background is true.
func StartWithConfig(cfg Config) *Client {
	c := &Client{
		cfg:    cfg,
		writer: spool.Writer{Dir: cfg.SpoolDir},
	}
	if !cfg.Enabled {
		return c
	}
	if err := os.MkdirAll(cfg.SpoolDir, 0o750); err != nil {
		slog.Error("chronos: spool directory not writable; disabling SDK",
			"spool", cfg.SpoolDir, "err", err)
		c.cfg.Enabled = false
		return c
	}
	if cfg.APMEnabled {
		c.spans = newSpanBuffer(cfg.SpanBatchSize, cfg.SpanMaxBuffered)
	}
	if cfg.ProfilerEnabled {
		c.cfg.applyProfileDefaults()
		cfg = c.cfg
		if cfg.IOProfileEnabled {
			c.io = newIORecorder(cfg.IOMaxSamples, cfg.ProfileMaxStackDepth)
		}
		if cfg.WallProfileEnabled {
			c.wall = newWallSampler(cfg.WallMaxStacks)
		}
	}
	setDefault(c)
	if !cfg.Background {
		return c
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	if cfg.ProfilerEnabled {
		runtime.SetMutexProfileFraction(5)
		runtime.SetBlockProfileRate(1000)
		c.wg.Add(1)
		go c.profileLoop(ctx)
		// Wall / off-CPU and IO run on their own loop: CPU profiling blocks for most
		// of its interval, and wall sampling must keep ticking while it does.
		c.wg.Add(1)
		go c.wallLoop(ctx)
	}
	if cfg.MetricsEnabled {
		c.wg.Add(1)
		go c.metricsLoop(ctx)
	}
	if c.spans != nil {
		c.wg.Add(1)
		go c.spanLoop(ctx)
	}
	return c
}

// Config returns a copy of the active configuration.
func (c *Client) Config() Config {
	if c == nil {
		return Config{}
	}
	return c.cfg
}

// Enabled reports whether the client will emit telemetry.
func (c *Client) Enabled() bool {
	return c != nil && c.cfg.Enabled
}

// Shutdown stops background loops, waits for them to exit, and flushes whatever they
// left buffered. Signals are flushed even when no loop was running, so a client built
// with Background=false still lands its spans on the way out.
func (c *Client) Shutdown(ctx context.Context) {
	if c == nil {
		return
	}
	if c.cancel != nil {
		c.cancel()
		done := make(chan struct{})
		go func() {
			c.wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-ctx.Done():
			// The deadline expired; flush what is already buffered rather than
			// returning with spans still in memory.
		}
	}
	if err := c.Flush(); err != nil {
		slog.Debug("chronos: shutdown flush failed", "err", err)
	}
}

func (c *Client) profileLoop(ctx context.Context) {
	defer c.wg.Done()
	interval := c.cfg.ProfileInterval
	if interval <= 0 {
		interval = 90 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.CollectCPUProfile(c.cfg.ProfileDuration); err != nil {
				slog.Debug("chronos: cpu profile failed", "err", err)
			}
			if err := c.CollectHeapProfile(); err != nil {
				slog.Debug("chronos: heap profile failed", "err", err)
			}
			if err := c.CollectLockProfile(); err != nil {
				slog.Debug("chronos: lock profile failed", "err", err)
			}
		}
	}
}

// wallLoop ticks the goroutine sampler and periodically spools WALL / OFF_CPU / IO.
//
// Sampling and flushing are deliberately separate clocks: the sample interval sets
// attribution fidelity (and overhead), the profile interval sets how often a batch
// reaches the spool.
func (c *Client) wallLoop(ctx context.Context) {
	defer c.wg.Done()
	defer func() {
		// A shutdown mid-window still has measured IO worth keeping.
		_ = c.flushWallProfiles()
		_ = c.flushIOProfiles()
	}()

	sampleEvery := c.cfg.WallSampleInterval
	if sampleEvery <= 0 {
		sampleEvery = 100 * time.Millisecond
	}
	flushEvery := c.cfg.ProfileInterval
	if flushEvery <= 0 {
		flushEvery = 90 * time.Second
	}

	var sampleC <-chan time.Time
	if c.wall != nil {
		ticker := time.NewTicker(sampleEvery)
		defer ticker.Stop()
		sampleC = ticker.C
	}
	flush := time.NewTicker(flushEvery)
	defer flush.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-sampleC:
			c.wall.sample()
		case <-flush.C:
			if err := c.flushWallProfiles(); err != nil {
				slog.Debug("chronos: wall profile flush failed", "err", err)
			}
			if err := c.flushIOProfiles(); err != nil {
				slog.Debug("chronos: io profile flush failed", "err", err)
			}
		}
	}
}

func (c *Client) metricsLoop(ctx context.Context) {
	defer c.wg.Done()
	interval := c.cfg.MetricsInterval
	if interval <= 0 {
		interval = 15 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	// Emit once at start so a short-lived process still produces a point.
	_ = c.EmitRuntimeMetrics()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.EmitRuntimeMetrics(); err != nil {
				slog.Debug("chronos: metrics emit failed", "err", err)
			}
		}
	}
}

func (c *Client) recordHTTP(status int, duration time.Duration) {
	if c == nil {
		return
	}
	c.httpRequests.Add(1)
	if status >= 500 {
		c.httpErrors.Add(1)
	}
	if duration > 0 {
		c.httpDuration.Add(uint64(duration.Nanoseconds()))
	}
}

func trimGoVersion() string {
	return runtime.Version()
}
