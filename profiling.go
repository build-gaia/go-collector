package chronos

import (
	"bytes"
	"fmt"
	"runtime/pprof"
	"strconv"
	"time"

	gprofile "github.com/google/pprof/profile"

	"chronos.dev/collector/sdk/go/spool"
)

// Series IDs. One series per sample type: the desktop resolves a chosen sample type to
// a single series, so two series sharing a type would leave one of them unreachable.
const (
	cpuSeriesID    = "go-cpu"
	heapSeriesID   = "go-heap"
	lockSeriesID   = "go-lock"
	wallSeriesID   = "go-wall"
	offCPUSeriesID = "go-offcpu"
	ioSeriesID     = "go-io"
)

// defaultProfileBatchSize caps samples per spool document.
const defaultProfileBatchSize = 500

// CollectCPUProfile samples CPU for duration and writes a Chronos .profile batch.
func (c *Client) CollectCPUProfile(duration time.Duration) error {
	if c == nil || !c.cfg.Enabled || !c.cfg.ProfilerEnabled {
		return nil
	}
	if duration <= 0 {
		duration = c.cfg.ProfileDuration
	}
	var buf bytes.Buffer
	if err := pprof.StartCPUProfile(&buf); err != nil {
		return fmt.Errorf("chronos: start cpu profile: %w", err)
	}
	timer := time.NewTimer(duration)
	<-timer.C
	pprof.StopCPUProfile()

	prof, err := gprofile.Parse(&buf)
	if err != nil {
		return fmt.Errorf("chronos: parse cpu profile: %w", err)
	}
	return c.writeNormalizedProfile(prof, cpuSeriesID, "PROFILE_SAMPLE_TYPE_CPU", "nanoseconds", nil, normalizeFilters{})
}

// CollectHeapProfile captures an in-use heap profile and writes a Chronos .profile batch.
func (c *Client) CollectHeapProfile() error {
	if c == nil || !c.cfg.Enabled || !c.cfg.ProfilerEnabled {
		return nil
	}
	var buf bytes.Buffer
	if err := pprof.WriteHeapProfile(&buf); err != nil {
		return fmt.Errorf("chronos: write heap profile: %w", err)
	}
	prof, err := gprofile.Parse(&buf)
	if err != nil {
		return fmt.Errorf("chronos: parse heap profile: %w", err)
	}
	return c.writeNormalizedProfile(prof, heapSeriesID, "PROFILE_SAMPLE_TYPE_ALLOCATION", "bytes", nil, normalizeFilters{})
}

// CollectLockProfile snapshots contention stacks: the mutex profile (time held by a
// contended lock) and the block profile (time parked waiting on a lock, channel or
// WaitGroup). Both are contention, so both land in one PROFILE_SAMPLE_TYPE_LOCK series
// distinguished by the `lock.kind` label — the desktop resolves a sample type to a
// single series, so shipping them separately would hide one of them.
func (c *Client) CollectLockProfile() error {
	if err := c.collectLookupProfile("mutex", lockSeriesID, "PROFILE_SAMPLE_TYPE_LOCK", "nanoseconds", "mutex"); err != nil {
		return err
	}
	return c.collectLookupProfile("block", lockSeriesID, "PROFILE_SAMPLE_TYPE_LOCK", "nanoseconds", "block")
}

func (c *Client) collectLookupProfile(lookupName, seriesID, sampleType, unit, lockKind string) error {
	if c == nil || !c.cfg.Enabled || !c.cfg.ProfilerEnabled {
		return nil
	}
	p := pprof.Lookup(lookupName)
	if p == nil {
		return nil
	}
	var buf bytes.Buffer
	// debug=0 → protobuf, same as CPU/heap writers.
	if err := p.WriteTo(&buf, 0); err != nil {
		return fmt.Errorf("chronos: write %s profile: %w", lookupName, err)
	}
	if buf.Len() == 0 {
		return nil
	}
	prof, err := gprofile.Parse(&buf)
	if err != nil {
		return fmt.Errorf("chronos: parse %s profile: %w", lookupName, err)
	}
	extra := map[string]string{}
	if lockKind != "" {
		extra["lock.kind"] = lockKind
	}
	return c.writeNormalizedProfile(prof, seriesID, sampleType, unit, extra, normalizeFilters{
		dropNonUserland: true,
		dropIdleWaits:   c.cfg.WallExcludeIdle,
	})
}

// normalizeFilters selects which samples a series keeps. CPU and heap keep everything —
// a stack sitting entirely in the runtime is GC cost, which is a real finding. Contention
// profiles do not, because their runtime-only stacks are scheduler bookkeeping.
type normalizeFilters struct {
	dropNonUserland bool
	dropIdleWaits   bool
}

func (c *Client) writeNormalizedProfile(prof *gprofile.Profile, seriesID, sampleType, unit string, extra map[string]string, filters normalizeFilters) error {
	samples := NormalizePprof(prof, NormalizeOptions{
		Organisation:    c.cfg.Organisation,
		Project:         c.cfg.Project,
		Application:     c.cfg.Application,
		SeriesID:        seriesID,
		SampleType:      sampleType,
		Unit:            unit,
		Labels:          c.baseProfileLabels(extra),
		DropNonUserland: filters.dropNonUserland,
		DropIdleWaits:   filters.dropIdleWaits,
	})
	return c.writeProfileSamples(seriesID, samples)
}

// baseProfileLabels are the labels every Go profile sample carries, plus per-series extras.
func (c *Client) baseProfileLabels(extra map[string]string) map[string]string {
	labels := map[string]string{
		"service":    c.cfg.ServiceName,
		"go.version": goVersionLabel(),
	}
	for k, v := range extra {
		if k != "" && v != "" {
			labels[k] = v
		}
	}
	return labels
}

// writeProfileSamples spools one sample batch, chunked so a burst of samples never
// exceeds the ingest document cap in a single file.
func (c *Client) writeProfileSamples(seriesID string, samples []ProfileSample) error {
	if len(samples) == 0 {
		return nil
	}
	limit := c.cfg.ProfileBatchSize
	if limit <= 0 {
		limit = defaultProfileBatchSize
	}
	for start := 0; start < len(samples); start += limit {
		end := start + limit
		if end > len(samples) {
			end = len(samples)
		}
		chunk := samples[start:end]
		batch := profileBatch{
			Schema:      "chronos.profiling.sample-batch.v1",
			Processing:  c.cfg.processing(seedNow() + seriesID + strconv.Itoa(start)),
			Samples:     chunk,
			SampleCount: strconv.Itoa(len(chunk)),
		}
		if _, err := c.writer.WriteJSON(spool.SignalProfile, batch); err != nil {
			return err
		}
	}
	return nil
}

// NormalizeOptions controls pprof → Chronos sample conversion.
type NormalizeOptions struct {
	Organisation string
	Project      string
	Application  string
	SeriesID     string
	SampleType   string
	Unit         string
	Labels       map[string]string

	// DropNonUserland discards samples with no application frame. The mutex and block
	// profiles are full of scheduler-internal contention (runtime.findRunnable taking
	// the scheduler lock) that no application change can act on.
	DropNonUserland bool
	// DropIdleWaits discards samples parked on a channel or select. A pool sized for
	// peak blocks its idle workers on the job channel, and that contention would
	// otherwise outweigh every real lock in the profile.
	DropIdleWaits bool
}

// NormalizePprof converts a Google pprof Profile into Chronos sample-batch records.
// Stacks are emitted root-first to match the PHP sampler / flame builder convention
// (pprof stores leaf-first). Library / runtime frames are tagged module=internal so the
// desktop can dim them the same way it dims PHP builtins.
func NormalizePprof(prof *gprofile.Profile, opt NormalizeOptions) []ProfileSample {
	if prof == nil {
		return nil
	}
	period := prof.Period
	if period <= 0 {
		period = 10_000_000 // 10ms default CPU period
	}
	cfg := Config{
		Organisation: opt.Organisation,
		Project:      opt.Project,
		Application:  opt.Application,
	}
	out := make([]ProfileSample, 0, len(prof.Sample))
	now := time.Now().UTC()
	for i, sample := range prof.Sample {
		if sample == nil || len(sample.Location) == 0 {
			continue
		}
		value := int64(0)
		if len(sample.Value) > 0 {
			value = sample.Value[len(sample.Value)-1]
		}
		if value == 0 && len(sample.Value) > 0 {
			value = sample.Value[0]
		}
		if value == 0 {
			continue
		}
		stack := framesRootFirst(sample)
		if opt.DropNonUserland && !hasUserlandFrame(stack) {
			continue
		}
		if opt.DropIdleWaits && classifyWait(stack).idle() {
			continue
		}
		// Applies to every series, including CPU: the sampler must never bill the
		// application for the cost of sampling.
		if isSelfProfilingStack(stack) {
			continue
		}
		labels := map[string]string{}
		for k, v := range opt.Labels {
			labels[k] = v
		}
		mergePprofSampleLabels(labels, sample)
		correlation := takeCorrelation(labels)
		out = append(out, ProfileSample{
			Organisation:      cfg.organisationRef(),
			Application:       cfg.applicationRef(),
			ProfileSeriesID:   opt.SeriesID,
			Sequence:          strconv.FormatInt(int64(i+1), 10),
			SampledAt:         formatRFC3339Nano(now),
			SampleType:        opt.SampleType,
			PeriodNanoseconds: strconv.FormatInt(period, 10),
			Value:             strconv.FormatInt(value, 10),
			Unit:              opt.Unit,
			Stack:             stack,
			Correlation:       correlation,
			Labels:            labels,
		})
	}
	return out
}

// mergePprofSampleLabels copies runtime/pprof Labels (route, job, …) onto Chronos labels.
func mergePprofSampleLabels(dst map[string]string, sample *gprofile.Sample) {
	if sample == nil {
		return
	}
	for key, values := range sample.Label {
		if len(values) == 0 || values[0] == "" {
			continue
		}
		dst[key] = values[0]
	}
}

// takeCorrelation moves trace/span identifiers out of the free-form label bag into the
// typed correlation field, so the desktop can jump from a flame frame to its trace.
// Client.Run stamps them as runtime/pprof labels; they arrive here via sample.Label.
func takeCorrelation(labels map[string]string) *ProfileCorrelation {
	traceID := labels["trace_id"]
	spanID := labels["span_id"]
	if traceID == "" && spanID == "" {
		return nil
	}
	delete(labels, "trace_id")
	delete(labels, "span_id")
	return &ProfileCorrelation{TraceID: traceID, SpanID: spanID}
}

func framesRootFirst(sample *gprofile.Sample) []ProfileFrame {
	// pprof locations are leaf → root; reverse to root → leaf.
	locs := sample.Location
	frames := make([]ProfileFrame, 0, len(locs)*2)
	for i := len(locs) - 1; i >= 0; i-- {
		loc := locs[i]
		if loc == nil {
			continue
		}
		mappingModule := ""
		if loc.Mapping != nil {
			mappingModule = baseName(loc.Mapping.File)
		}
		if len(loc.Line) == 0 {
			frames = append(frames, ProfileFrame{
				Module:   frameModule("", "", mappingModule),
				Function: fmt.Sprintf("0x%x", loc.Address),
			})
			continue
		}
		// Line entries are inline-outer → inner; keep order after reverse of locations.
		for _, line := range loc.Line {
			fn := ""
			file := ""
			var lineno uint32
			if line.Function != nil {
				fn = line.Function.Name
				file = line.Function.Filename
			}
			if line.Line > 0 && line.Line < 1<<32 {
				lineno = uint32(line.Line)
			}
			frames = append(frames, ProfileFrame{
				Module:   frameModule(fn, file, mappingModule),
				Function: fn,
				File:     file,
				Line:     lineno,
			})
		}
	}
	return frames
}

func goVersionLabel() string {
	return trimGoVersion()
}

type profileBatch struct {
	Schema      string             `json:"schema"`
	Processing  ProcessingIdentity `json:"processing"`
	Samples     []ProfileSample    `json:"samples"`
	SampleCount string             `json:"sampleCount"`
}

// ProfileSample is one Chronos profiling sample-batch record.
type ProfileSample struct {
	Organisation      OrganisationRef     `json:"organisation"`
	Application       ApplicationRef      `json:"application"`
	ProfileSeriesID   string              `json:"profileSeriesId"`
	Sequence          string              `json:"sequence"`
	SampledAt         string              `json:"sampledAt"`
	SampleType        string              `json:"sampleType"`
	PeriodNanoseconds string              `json:"periodNanoseconds"`
	Value             string              `json:"value"`
	Unit              string              `json:"unit"`
	Stack             []ProfileFrame      `json:"stack"`
	Correlation       *ProfileCorrelation `json:"correlation,omitempty"`
	Labels            map[string]string   `json:"labels,omitempty"`
}

// ProfileFrame is one stack frame in a Chronos profile sample.
type ProfileFrame struct {
	Module   string `json:"module,omitempty"`
	Function string `json:"function,omitempty"`
	File     string `json:"file,omitempty"`
	Line     uint32 `json:"line,omitempty"`
}

// ProfileCorrelation links a sample to a trace/session.
type ProfileCorrelation struct {
	TraceID   string `json:"traceId,omitempty"`
	SpanID    string `json:"spanId,omitempty"`
	SessionID string `json:"sessionId,omitempty"`
}
