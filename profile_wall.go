package chronos

import (
	"hash/maphash"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"
)

// Wall-clock and off-CPU profiling for Go, with no application changes.
//
// WHY NOT runtime/pprof LABELS. A pprof label set lives on the goroutine that called
// pprof.Do, and Go does not carry labels across a channel handoff. A service that hands
// work to a worker pool started at boot therefore attributes almost none of its real
// work: the labelled goroutine sits in WaitGroup.Wait while unlabelled workers do the
// job. Sampling the goroutine set instead observes every goroutine wherever it is, so
// attribution never depends on where the goroutine was created.
//
// WHAT IT MEASURES. Each tick, runtime.GoroutineProfile yields one stack per live
// goroutine. Charging every goroutine one tick interval of nanoseconds gives wall time
// per stack — the Go analogue of the PHP sampler's monotonic drain window. OFF_CPU is
// the subset whose leaf is a park primitive: the goroutine existed but did not run.
//
// COST. The hot path copies program counters and hashes them; nothing is symbolised
// until flush. GoroutineProfile does briefly stop the world, so the tick interval is the
// overhead knob (CHRONOS_GO_WALL_INTERVAL) and the sampler backs off on its own if a
// snapshot costs more than a tenth of the interval.

// waitKind classifies why a goroutine was not running when it was sampled.
type waitKind uint8

const (
	waitRunning waitKind = iota // on-CPU or runnable
	waitIO                      // parked in netpoll / file descriptor read
	waitLock                    // parked on a mutex, WaitGroup or Cond
	waitChan                    // parked on a channel or select — an idle pool worker looks like this
	waitAccept                  // parked waiting for a connection to arrive — an idle server loop
	waitSleep                   // timer sleep
	waitGC                      // runtime background worker
	waitOther                   // parked for a reason we do not name
)

func (k waitKind) label() string {
	switch k {
	case waitIO:
		return "io"
	case waitLock:
		return "lock"
	case waitChan:
		return "chan"
	case waitAccept:
		return "accept"
	case waitSleep:
		return "sleep"
	case waitGC:
		return "gc"
	case waitOther:
		return "parked"
	default:
		return "running"
	}
}

// blocked reports whether the goroutine was off-CPU when sampled.
func (k waitKind) blocked() bool { return k != waitRunning && k != waitGC }

// idle reports waiting for work to arrive rather than waiting on work in progress.
//
// An accept loop parked in netpoll is technically an I/O wait, and counting it as one
// swamps the profile: an idle server accumulates one full second of "I/O" per wall
// second per listener, dwarfing every real query. The distinction that matters to a
// reader is not blocked-vs-running, it is waiting-for-work vs doing-work.
func (k waitKind) idle() bool { return k == waitChan || k == waitAccept }

// wallSampler accumulates goroutine-ticks per unique stack between flushes.
type wallSampler struct {
	mu        sync.Mutex
	buckets   map[uint64]*wallBucket
	buf       []runtime.StackRecord
	seed      maphash.Seed
	maxStacks int
	dropped   int64
	ticks     int64
}

type wallBucket struct {
	pcs   []uintptr
	ticks int64
}

func newWallSampler(maxStacks int) *wallSampler {
	if maxStacks <= 0 {
		maxStacks = 4096
	}
	return &wallSampler{
		buckets:   make(map[uint64]*wallBucket),
		seed:      maphash.MakeSeed(),
		maxStacks: maxStacks,
	}
}

// sample takes one goroutine snapshot and banks a tick against each live stack.
func (w *wallSampler) sample() {
	if w == nil {
		return
	}
	records, ok := w.snapshot()
	if !ok {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.ticks++
	for i := range records {
		pcs := records[i].Stack()
		if len(pcs) == 0 {
			continue
		}
		key := w.hash(pcs)
		if bucket, exists := w.buckets[key]; exists {
			bucket.ticks++
			continue
		}
		if len(w.buckets) >= w.maxStacks {
			w.dropped++
			continue
		}
		w.buckets[key] = &wallBucket{pcs: append([]uintptr(nil), pcs...), ticks: 1}
	}
}

// snapshot reads every live goroutine's stack, growing the reusable buffer when the
// goroutine count has outgrown it. GoroutineProfile reports the size it needs when it
// refuses, so this converges in one extra call under a steady goroutine count.
func (w *wallSampler) snapshot() ([]runtime.StackRecord, bool) {
	w.mu.Lock()
	buf := w.buf
	w.mu.Unlock()
	for attempt := 0; attempt < 3; attempt++ {
		if len(buf) == 0 {
			buf = make([]runtime.StackRecord, runtime.NumGoroutine()+16)
		}
		n, ok := runtime.GoroutineProfile(buf)
		if ok {
			w.mu.Lock()
			w.buf = buf
			w.mu.Unlock()
			return buf[:n], true
		}
		buf = make([]runtime.StackRecord, n+16)
	}
	return nil, false
}

func (w *wallSampler) hash(pcs []uintptr) uint64 {
	var h maphash.Hash
	h.SetSeed(w.seed)
	// A []uintptr is a contiguous run of machine words; hashing its bytes avoids a
	// per-frame write on a path that runs for every goroutine on every tick.
	bytes := unsafe.Slice((*byte)(unsafe.Pointer(&pcs[0])), len(pcs)*int(unsafe.Sizeof(pcs[0])))
	_, _ = h.Write(bytes)
	return h.Sum64()
}

// drain returns the accumulated buckets and resets the accumulator.
func (w *wallSampler) drain() (buckets []*wallBucket, ticks int64, dropped int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	buckets = make([]*wallBucket, 0, len(w.buckets))
	for _, b := range w.buckets {
		buckets = append(buckets, b)
	}
	ticks, dropped = w.ticks, w.dropped
	w.buckets = make(map[uint64]*wallBucket, len(w.buckets))
	w.ticks, w.dropped = 0, 0
	sort.Slice(buckets, func(i, j int) bool { return buckets[i].ticks > buckets[j].ticks })
	return buckets, ticks, dropped
}

// symbolizeRootFirst resolves program counters to Chronos frames, root-first to match
// the PHP sampler and the desktop flame builder.
func symbolizeRootFirst(pcs []uintptr) []ProfileFrame {
	if len(pcs) == 0 {
		return nil
	}
	leafFirst := make([]ProfileFrame, 0, len(pcs))
	iter := runtime.CallersFrames(pcs)
	for {
		frame, more := iter.Next()
		if frame.Function != "" || frame.PC != 0 {
			leafFirst = append(leafFirst, ProfileFrame{
				Module:   frameModule(frame.Function, frame.File, ""),
				Function: frame.Function,
				File:     frame.File,
				Line:     lineNumber(frame.Line),
			})
		}
		if !more {
			break
		}
	}
	for i, j := 0, len(leafFirst)-1; i < j; i, j = i+1, j-1 {
		leafFirst[i], leafFirst[j] = leafFirst[j], leafFirst[i]
	}
	return leafFirst
}

func lineNumber(line int) uint32 {
	if line <= 0 || line >= 1<<31 {
		return 0
	}
	return uint32(line)
}

// classifyWait decides whether a sampled goroutine was running, and if not, why it was
// parked. Frames are root-first, so the leaf is last.
//
// The leaf decides running-vs-parked: only the runtime's park primitives appear as the
// leaf of a goroutine that is off-CPU. The reason then comes from the frames just above
// the primitive, scanned leaf-ward so that the innermost wait wins — a goroutine blocked
// in netpoll inside a driver that also uses mutexes is an IO wait, not a lock wait.
func classifyWait(frames []ProfileFrame) waitKind {
	if len(frames) == 0 {
		return waitRunning
	}
	leaf := frames[len(frames)-1].Function
	if !isParkPrimitive(leaf) {
		return waitRunning
	}
	// Whole-stack test, not part of the leaf-ward scan below: an accept loop parks in
	// netpoll, so internal/poll.runtime_pollWait sits nearer the leaf than the accept
	// frame and would be classified as ordinary I/O.
	if isAcceptStack(frames) {
		return waitAccept
	}
	for i := len(frames) - 1; i >= 0; i-- {
		fn := frames[i].Function
		switch {
		case strings.HasPrefix(fn, "internal/poll.runtime_pollWait"),
			strings.HasPrefix(fn, "internal/poll.(*FD)"),
			strings.HasPrefix(fn, "internal/poll.(*pollDesc)"),
			strings.HasPrefix(fn, "net.(*netFD)"),
			strings.HasPrefix(fn, "net.(*conn)"),
			strings.HasPrefix(fn, "os.(*File)"):
			return waitIO
		case strings.HasPrefix(fn, "sync.runtime_Semacquire"),
			strings.HasPrefix(fn, "sync.runtime_SemacquireMutex"),
			strings.HasPrefix(fn, "sync.runtime_SemacquireWaitGroup"),
			strings.HasPrefix(fn, "sync.runtime_notifyListWait"),
			strings.HasPrefix(fn, "sync.(*Mutex)"),
			strings.HasPrefix(fn, "sync.(*RWMutex)"),
			strings.HasPrefix(fn, "sync.(*WaitGroup)"),
			strings.HasPrefix(fn, "sync.(*Cond)"):
			return waitLock
		case strings.HasPrefix(fn, "runtime.chanrecv"),
			strings.HasPrefix(fn, "runtime.chansend"),
			strings.HasPrefix(fn, "runtime.selectgo"):
			return waitChan
		case strings.HasPrefix(fn, "time.Sleep"),
			strings.HasPrefix(fn, "runtime.timeSleep"):
			return waitSleep
		case strings.HasPrefix(fn, "runtime.gcBgMarkWorker"),
			strings.HasPrefix(fn, "runtime.bgsweep"),
			strings.HasPrefix(fn, "runtime.bgscavenge"),
			strings.HasPrefix(fn, "runtime.forcegchelper"),
			strings.HasPrefix(fn, "runtime.runfinq"):
			return waitGC
		}
	}
	return waitOther
}

// isParkPrimitive reports whether a leaf frame is the runtime entering a wait.
//
// Goroutine-profile stacks bottom out in runtime.gopark. Block- and mutex-profile stacks
// do not: they are recorded at the blocking operation itself, so an idle worker appears
// as a bare runtime.chanrecv2 leaf. Both spellings have to count, or contention profiles
// classify every idle worker as running.
// isAcceptStack reports a goroutine parked waiting for a new connection.
func isAcceptStack(frames []ProfileFrame) bool {
	for _, f := range frames {
		switch {
		case strings.HasPrefix(f.Function, "internal/poll.(*FD).Accept"),
			strings.HasPrefix(f.Function, "net.(*netFD).accept"),
			strings.HasPrefix(f.Function, "net.(*TCPListener).Accept"),
			strings.HasPrefix(f.Function, "net.(*TCPListener).accept"),
			strings.HasPrefix(f.Function, "net.(*UnixListener).accept"),
			strings.HasPrefix(f.Function, "net/http.(*Server).Serve"):
			return true
		}
	}
	return false
}

func isParkPrimitive(function string) bool {
	switch {
	case strings.HasPrefix(function, "runtime.gopark"),
		strings.HasPrefix(function, "runtime.goparkunlock"),
		strings.HasPrefix(function, "runtime.selectgo"),
		strings.HasPrefix(function, "runtime.notetsleep"),
		strings.HasPrefix(function, "runtime.semacquire"),
		strings.HasPrefix(function, "runtime.chanrecv"),
		strings.HasPrefix(function, "runtime.chansend"),
		strings.HasPrefix(function, "runtime.block"):
		return true
	}
	return false
}

// wallSamplesFrom converts accumulated buckets into WALL and OFF_CPU samples.
//
// Every non-runtime goroutine contributes wall time. Off-CPU is the parked subset —
// wall minus running, which is the same accounting the PHP sampler does as wall minus
// CPU, arrived at per goroutine instead of per window.
//
// Idle pool workers (parked on a channel with no work) are excluded from WALL by
// default: a pool sized for peak spends most of its wall time waiting for a job, and
// counting that swamps every real stack in the flame chart. Set
// CHRONOS_GO_WALL_EXCLUDE_IDLE=false to measure queue wait instead.
func (c *Client) wallSamplesFrom(buckets []*wallBucket, interval time.Duration) (wall, offCPU []ProfileSample) {
	if interval <= 0 {
		return nil, nil
	}
	for _, bucket := range buckets {
		w, o := c.samplesForFrames(symbolizeRootFirst(bucket.pcs), bucket.ticks, interval)
		wall = append(wall, w...)
		offCPU = append(offCPU, o...)
	}
	for i := range wall {
		wall[i].Sequence = strconv.Itoa(i + 1)
	}
	for i := range offCPU {
		offCPU[i].Sequence = strconv.Itoa(i + 1)
	}
	return wall, offCPU
}

// samplesForFrames turns one aggregated stack into its WALL and OFF_CPU records.
func (c *Client) samplesForFrames(frames []ProfileFrame, ticks int64, interval time.Duration) (wall, offCPU []ProfileSample) {
	periodNanos := interval.Nanoseconds()
	if periodNanos <= 0 || ticks <= 0 || !hasUserlandFrame(frames) || isSelfProfilingStack(frames) {
		return nil, nil
	}
	kind := classifyWait(frames)
	if kind == waitGC {
		return nil, nil
	}
	if kind.idle() && c.cfg.WallExcludeIdle {
		return nil, nil
	}
	now := time.Now().UTC()
	nanos := ticks * periodNanos
	labels := c.baseProfileLabels(map[string]string{"wait": kind.label()})
	wall = append(wall, c.profileSample(wallSeriesID, "PROFILE_SAMPLE_TYPE_WALL", "nanoseconds",
		1, now, periodNanos, nanos, frames, labels, nil))
	if kind.blocked() {
		offCPU = append(offCPU, c.profileSample(offCPUSeriesID, "PROFILE_SAMPLE_TYPE_OFF_CPU", "nanoseconds",
			1, now, periodNanos, nanos, frames, labels, nil))
	}
	return wall, offCPU
}

// profileSample builds one sample-batch record from an already-symbolised stack.
func (c *Client) profileSample(
	seriesID, sampleType, unit string,
	sequence int,
	at time.Time,
	periodNanos, value int64,
	frames []ProfileFrame,
	labels map[string]string,
	correlation *ProfileCorrelation,
) ProfileSample {
	return ProfileSample{
		Organisation:      c.cfg.organisationRef(),
		Application:       c.cfg.applicationRef(),
		ProfileSeriesID:   seriesID,
		Sequence:          strconv.Itoa(sequence),
		SampledAt:         formatRFC3339Nano(at),
		SampleType:        sampleType,
		PeriodNanoseconds: strconv.FormatInt(periodNanos, 10),
		Value:             strconv.FormatInt(value, 10),
		Unit:              unit,
		Stack:             frames,
		Correlation:       correlation,
		Labels:            labels,
	}
}

// flushWallProfiles drains the sampler and spools WALL / OFF_CPU batches.
func (c *Client) flushWallProfiles() error {
	if c == nil || c.wall == nil {
		return nil
	}
	buckets, ticks, _ := c.wall.drain()
	if ticks == 0 || len(buckets) == 0 {
		return nil
	}
	wall, offCPU := c.wallSamplesFrom(buckets, c.cfg.WallSampleInterval)
	if err := c.writeProfileSamples(wallSeriesID, wall); err != nil {
		return err
	}
	return c.writeProfileSamples(offCPUSeriesID, offCPU)
}
