package chronos

import (
	"context"
	"strconv"
	"sync"
	"time"

	"chronos.dev/collector/sdk/go/redact"
	"chronos.dev/collector/sdk/go/spool"
)

type spanContextKey struct{}

// Span is an in-flight Chronos span buffered until End.
type Span struct {
	cfg         Config
	writer      spool.Writer
	buffer      *spanBuffer
	client      *Client
	mu          sync.Mutex
	ended       bool
	TraceID     string
	SpanID      string
	ParentID    string
	Name        string
	StartedAt   time.Time
	Attributes  map[string]string
	Status      string
	ServiceName string
	// Sampled is the trace's sampling decision as it arrived, or true for a trace
	// this process started: there is no sampler on the Go side, so a locally born
	// trace is always sampled and an inherited flag is passed on untouched.
	Sampled bool
	// Tracestate and Baggage are the inbound companion headers, held so every
	// outbound carrier this span fathers can forward them. Empty when absent.
	Tracestate string
	Baggage    string
	HTTPMethod string
	HTTPRoute  string
	HTTPStatus uint32
}

// StartSpan begins a child span under the ambient context span (if any).
func (c *Client) StartSpan(ctx context.Context, name string) (context.Context, *Span) {
	if c == nil || !c.cfg.Enabled || !c.cfg.APMEnabled {
		return ctx, &Span{Name: name, StartedAt: time.Now().UTC(), Attributes: map[string]string{}, Sampled: true}
	}
	parent := SpanFromContext(ctx)
	traceID := newTraceID()
	parentID := ""
	// A trace this process starts is sampled; one it joins keeps whatever decision
	// the originator made, along with the context it is obliged to forward.
	sampled := true
	tracestate := ""
	baggage := ""
	if parent != nil && parent.TraceID != "" {
		traceID = parent.TraceID
		parentID = parent.SpanID
		sampled = parent.Sampled
		tracestate = parent.Tracestate
		baggage = parent.Baggage
	}
	span := &Span{
		cfg:         c.cfg,
		writer:      c.writer,
		buffer:      c.spans,
		client:      c,
		TraceID:     traceID,
		SpanID:      newSpanID(),
		ParentID:    parentID,
		Name:        name,
		StartedAt:   time.Now().UTC(),
		Attributes:  map[string]string{},
		Status:      "ok",
		ServiceName: c.cfg.ServiceName,
		Sampled:     sampled,
		Tracestate:  tracestate,
		Baggage:     baggage,
	}
	return context.WithValue(ctx, spanContextKey{}, span), span
}

// SpanFromContext returns the active span, or nil.
func SpanFromContext(ctx context.Context) *Span {
	if ctx == nil {
		return nil
	}
	span, _ := ctx.Value(spanContextKey{}).(*Span)
	return span
}

// SetAttribute sets a single attribute (stringified).
func (s *Span) SetAttribute(key, value string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Attributes == nil {
		s.Attributes = map[string]string{}
	}
	if len(s.Attributes) >= 64 {
		return
	}
	s.Attributes[key] = value
}

// SetStatus sets the span status string (ok, error, unset).
func (s *Span) SetStatus(status string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Status = status
}

// End records the span. It is buffered into a batch rather than written immediately —
// see span_buffer.go. Fail-open on write errors.
func (s *Span) End() {
	if s == nil {
		return
	}
	record, ok := s.finish()
	if !ok {
		return
	}
	// The spool write happens outside the span lock: the inline batch flush takes the
	// buffer lock and does file IO, and sibling spans ending on other goroutines should
	// not serialise behind it.
	if s.client == nil || s.buffer == nil {
		// A span built by hand, with no client behind it, still writes as a batch of one.
		batch := spanBatch{
			Schema:     "chronos.tracing.span-batch.v1",
			Processing: s.cfg.processing(seedNow() + record.SpanID),
			Spans:      []spanRecord{record},
			SpanCount:  "1",
		}
		_, _ = s.writer.WriteJSON(spool.SignalTrace, batch)
		return
	}
	s.client.enqueueSpan(record)
}

// finish marks the span ended and snapshots it as a wire record. ok is false when the
// span was already ended or is not recordable.
func (s *Span) finish() (spanRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended {
		return spanRecord{}, false
	}
	s.ended = true
	if !s.cfg.Enabled || !s.cfg.APMEnabled || s.writer.Dir == "" {
		return spanRecord{}, false
	}
	attrs := s.Attributes
	if s.cfg.HTTPRedact {
		attrs = redact.Map(attrs, s.cfg.RedactPatterns)
	}
	// After redaction: the resource family is the SDK's own, carries no
	// application data, and must not be reshaped by an application's patterns.
	attrs = withResourceAttributes(attrs, s.cfg)
	return spanRecord{
		Organisation:   s.cfg.organisationRef(),
		Application:    s.cfg.applicationRef(),
		TraceID:        s.TraceID,
		SpanID:         s.SpanID,
		ParentSpanID:   s.ParentID,
		Name:           s.Name,
		StartedAt:      formatRFC3339Nano(s.StartedAt),
		EndedAt:        formatRFC3339Nano(time.Now().UTC()),
		Status:         s.Status,
		Attributes:     attrs,
		ServiceName:    s.ServiceName,
		HTTPMethod:     s.HTTPMethod,
		HTTPRoute:      s.HTTPRoute,
		HTTPStatusCode: s.HTTPStatus,
	}, true
}

// Traceparent returns a W3C traceparent header value for outbound propagation.
//
// The flags byte is the span's own Sampled decision rather than a hardcoded `01`:
// the sampling decision belongs to whoever started the trace, and re-rendering an
// inherited `00` as `01` resurrects downstream half of a trace whose other half was
// deliberately dropped. See RemoteContext.
func (s *Span) Traceparent() string {
	if s == nil {
		return ""
	}
	return FormatTraceparent(s.TraceID, s.SpanID, s.Sampled)
}

// RemoteContext renders the span as the context to hand the next hop: the
// traceparent naming THIS span, plus the tracestate and baggage it inherited, which
// W3C requires a participant to forward unchanged.
func (s *Span) RemoteContext() RemoteContext {
	if s == nil {
		return RemoteContext{}
	}
	return RemoteContext{
		TraceID:    s.TraceID,
		SpanID:     s.SpanID,
		Sampled:    s.Sampled,
		Tracestate: s.Tracestate,
		Baggage:    s.Baggage,
	}
}

// Adopt reparents the span onto an extracted remote context, carrying its sampling
// flag and companion headers. For the server-side case where the span has to exist
// before the inbound headers are read.
func (s *Span) Adopt(remote RemoteContext) {
	if s == nil || !remote.Valid() {
		return
	}
	s.TraceID = remote.TraceID
	s.ParentID = remote.SpanID
	s.Sampled = remote.Sampled
	s.Tracestate = remote.Tracestate
	s.Baggage = remote.Baggage
}

type spanBatch struct {
	Schema     string             `json:"schema"`
	Processing ProcessingIdentity `json:"processing"`
	Spans      []spanRecord       `json:"spans"`
	SpanCount  string             `json:"spanCount"`
}

type spanRecord struct {
	Organisation   OrganisationRef   `json:"organisation"`
	Application    ApplicationRef    `json:"application"`
	TraceID        string            `json:"traceId"`
	SpanID         string            `json:"spanId"`
	ParentSpanID   string            `json:"parentSpanId"`
	Name           string            `json:"name"`
	StartedAt      string            `json:"startedAt"`
	EndedAt        string            `json:"endedAt"`
	Status         string            `json:"status"`
	Attributes     map[string]string `json:"attributes"`
	ServiceName    string            `json:"serviceName,omitempty"`
	HTTPMethod     string            `json:"httpMethod,omitempty"`
	HTTPRoute      string            `json:"httpRoute,omitempty"`
	HTTPStatusCode uint32            `json:"httpStatusCode,omitempty"`
}

func statusFromCode(code int) string {
	if code >= 500 {
		return "error"
	}
	return "ok"
}

func uint32Status(code int) uint32 {
	if code < 0 {
		return 0
	}
	return uint32(code)
}

func attrInt(n int) string {
	return strconv.Itoa(n)
}
