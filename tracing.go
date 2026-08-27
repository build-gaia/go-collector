package chronos

import (
	"context"
	"fmt"
	"strconv"
	"strings"
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
	HTTPMethod  string
	HTTPRoute   string
	HTTPStatus  uint32
}

// StartSpan begins a child span under the ambient context span (if any).
func (c *Client) StartSpan(ctx context.Context, name string) (context.Context, *Span) {
	if c == nil || !c.cfg.Enabled || !c.cfg.APMEnabled {
		return ctx, &Span{Name: name, StartedAt: time.Now().UTC(), Attributes: map[string]string{}}
	}
	parent := SpanFromContext(ctx)
	traceID := newTraceID()
	parentID := ""
	if parent != nil && parent.TraceID != "" {
		traceID = parent.TraceID
		parentID = parent.SpanID
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
func (s *Span) Traceparent() string {
	if s == nil || s.TraceID == "" || s.SpanID == "" {
		return ""
	}
	return fmt.Sprintf("00-%s-%s-01", s.TraceID, s.SpanID)
}

// ParseTraceparent extracts trace-id and parent-id from a W3C traceparent header.
func ParseTraceparent(header string) (traceID, spanID string, ok bool) {
	parts := strings.Split(strings.TrimSpace(header), "-")
	if len(parts) != 4 {
		return "", "", false
	}
	if len(parts[1]) != 32 || len(parts[2]) != 16 {
		return "", "", false
	}
	return parts[1], parts[2], true
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
