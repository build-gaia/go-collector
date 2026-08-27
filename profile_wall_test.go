package chronos

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// frames builds a root-first stack from leaf-last function names.
func frames(names ...string) []ProfileFrame {
	out := make([]ProfileFrame, 0, len(names))
	for _, n := range names {
		out = append(out, ProfileFrame{Function: n, Module: frameModule(n, "", "")})
	}
	return out
}

func TestClassifyWaitRunningWhenLeafIsNotAPark(t *testing.T) {
	// A goroutine deep inside the driver but not parked is running, even though its
	// stack contains the net frames an IO wait would also show.
	require.Equal(t, waitRunning, classifyWait(frames(
		"acme/app/job.Run",
		"net.(*conn).Read",
		"acme/app/parse.Decode",
	)))
}

func TestClassifyWaitDistinguishesParkReasons(t *testing.T) {
	require.Equal(t, waitIO, classifyWait(frames(
		"acme/app/job.Run",
		"database/sql.(*DB).QueryContext",
		"net.(*netFD).Read",
		"internal/poll.runtime_pollWait",
		"runtime.netpollblock",
		"runtime.gopark",
	)))
	require.Equal(t, waitLock, classifyWait(frames(
		"acme/app/job.Run",
		"sync.(*WaitGroup).Wait",
		"sync.runtime_SemacquireWaitGroup",
		"runtime.semacquire1",
		"runtime.gopark",
	)))
	require.Equal(t, waitChan, classifyWait(frames(
		"acme/app/worker.(*Pool).Start.func1",
		"runtime.chanrecv1",
		"runtime.chanrecv",
		"runtime.gopark",
	)))
	require.Equal(t, waitGC, classifyWait(frames(
		"runtime.gcBgMarkWorker",
		"runtime.gopark",
	)))
}

// An IO wait nested inside a lock-holding call is IO: the innermost wait is the one
// the goroutine is actually blocked on.
func TestClassifyWaitPrefersInnermostReason(t *testing.T) {
	require.Equal(t, waitIO, classifyWait(frames(
		"acme/app/job.Run",
		"sync.(*Mutex).Lock",
		"acme/app/pool.Acquire",
		"internal/poll.runtime_pollWait",
		"runtime.gopark",
	)))
}

func TestWallSamplesExcludeIdleAndRuntimeStacks(t *testing.T) {
	c := &Client{cfg: Config{
		Organisation: "org", Project: "proj", Application: "app",
		ServiceName: "svc", WallExcludeIdle: true,
	}}
	idle := frames("acme/app/worker.(*Pool).Start.func1", "runtime.chanrecv", "runtime.gopark")
	blocked := frames("acme/app/job.Run", "internal/poll.runtime_pollWait", "runtime.gopark")
	running := frames("acme/app/job.Run", "acme/app/calc.Sum")
	gc := frames("runtime.gcBgMarkWorker", "runtime.gopark")

	got := map[string][]ProfileSample{}
	for i, stack := range [][]ProfileFrame{idle, blocked, running, gc} {
		wall, off := c.samplesForFrames(stack, int64(i+1), 100*time.Millisecond)
		got["wall"] = append(got["wall"], wall...)
		got["off"] = append(got["off"], off...)
	}

	// Idle workers and runtime goroutines contribute nothing; blocked and running do.
	require.Len(t, got["wall"], 2)
	require.Len(t, got["off"], 1)
	require.Equal(t, "io", got["off"][0].Labels["wait"])
	require.Equal(t, "PROFILE_SAMPLE_TYPE_OFF_CPU", got["off"][0].SampleType)

	// 2 ticks × 100ms of blocked time.
	require.Equal(t, strconv.FormatInt((200*time.Millisecond).Nanoseconds(), 10), got["off"][0].Value)
}

func TestWallSamplerAggregatesIdenticalStacks(t *testing.T) {
	w := newWallSampler(8)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); w.sample(); w.sample() }()
	wg.Wait()

	buckets, ticks, dropped := w.drain()
	require.EqualValues(t, 2, ticks)
	require.EqualValues(t, 0, dropped)
	require.NotEmpty(t, buckets)
	// Two snapshots of a stable process must produce repeat ticks, not only new stacks.
	total := int64(0)
	for _, b := range buckets {
		total += b.ticks
	}
	require.Greater(t, total, int64(len(buckets)))

	// drain resets the window.
	after, ticksAfter, _ := w.drain()
	require.Empty(t, after)
	require.EqualValues(t, 0, ticksAfter)
}

func TestWallSamplerCapsDistinctStacks(t *testing.T) {
	w := newWallSampler(1)
	w.sample()
	_, _, dropped := w.drain()
	require.Greater(t, dropped, int64(0))
}

// Block- and mutex-profile stacks are recorded at the blocking call, so they lack the
// runtime.gopark leaf a goroutine snapshot has. An idle pool worker must still classify
// as a channel wait from that shorter stack.
func TestClassifyWaitHandlesBlockProfileStacks(t *testing.T) {
	require.Equal(t, waitChan, classifyWait(frames(
		"acme/app/worker.(*Pool).Start.func1",
		"runtime.chanrecv2",
	)))
}

// An idle accept loop parks in netpoll, so the generic I/O test would claim it. It is
// waiting for work to arrive, not doing work, and an idle server accumulates a full
// second of it per wall second per listener — enough to bury every real query.
func TestClassifyWaitTreatsAcceptLoopAsIdle(t *testing.T) {
	accept := frames(
		"acme/app/internal/http.Start",
		"net/http.(*Server).ListenAndServe",
		"net/http.(*Server).Serve",
		"net.(*TCPListener).Accept",
		"net.(*netFD).accept",
		"internal/poll.(*FD).Accept",
		"internal/poll.runtime_pollWait",
		"runtime.netpollblock",
		"runtime.gopark",
	)
	kind := classifyWait(accept)
	require.Equal(t, waitAccept, kind)
	require.True(t, kind.idle())

	// A real query blocked in netpoll must stay I/O, not be swept up as idle.
	query := classifyWait(frames(
		"acme/app/job.Run",
		"database/sql.(*DB).QueryContext",
		"net.(*netFD).Read",
		"internal/poll.runtime_pollWait",
		"runtime.gopark",
	))
	require.Equal(t, waitIO, query)
	require.False(t, query.idle())
}

func TestAcceptLoopIsExcludedFromWall(t *testing.T) {
	c := &Client{cfg: Config{
		Organisation: "org", Project: "p", Application: "a",
		ServiceName: "svc", WallExcludeIdle: true,
	}}
	wall, off := c.samplesForFrames(frames(
		"acme/app/internal/http.Start",
		"net/http.(*Server).Serve",
		"internal/poll.(*FD).Accept",
		"runtime.gopark",
	), 100, 100*time.Millisecond)
	require.Empty(t, wall)
	require.Empty(t, off)
}

// The profiler must never bill the application for the cost of profiling.
func TestSelfProfilingStacksAreDropped(t *testing.T) {
	self := frames(
		"chronos.dev/collector/sdk/go.(*Client).wallLoop",
		"chronos.dev/collector/sdk/go.(*wallSampler).sample",
		"runtime.GoroutineProfile",
		"runtime.startTheWorld",
	)
	require.True(t, isSelfProfilingStack(self))

	// Pure-runtime stacks are GC / scheduler cost — a real finding, kept.
	require.False(t, isSelfProfilingStack(frames("runtime.gcBgMarkWorker", "runtime.gopark")))
	// An SDK frame under application code is normal instrumentation, kept.
	require.False(t, isSelfProfilingStack(frames(
		"acme/app/job.Run",
		"chronos.dev/collector/sdk/go.(*Client).Run",
		"acme/app/job.work",
	)))
}

// Binaries built from a file argument (`go build ./main.go`, `go run main.go` — what air
// and most Dockerfiles do) record no module path, so the root has to be inferred or every
// frame carries its full import path.
func TestInferModuleRootCoversFileBuiltBinaries(t *testing.T) {
	require.Equal(t, "service-stock-analytics", inferModuleRoot("service-stock-analytics/internal/service/stock"))
	require.Equal(t, "github.com/org/repo", inferModuleRoot("github.com/org/repo/internal/thing"))
	require.Equal(t, "main", inferModuleRoot("main"))
}
