package chronos

import (
	"context"
	"log/slog"
	"time"

	"chronos.dev/collector/sdk/go/spool"
)

// Log emits a single log record as a one-record .log batch, correlated to the ambient span.
func (c *Client) Log(ctx context.Context, severity, body string, attrs map[string]string) {
	if c == nil || !c.cfg.Enabled || !c.cfg.LogsEnabled {
		return
	}
	traceID, spanID := "", ""
	if span := SpanFromContext(ctx); span != nil {
		traceID = span.TraceID
		spanID = span.SpanID
	}
	if attrs == nil {
		attrs = map[string]string{}
	}
	if len(attrs) > 64 {
		trimmed := make(map[string]string, 64)
		i := 0
		for k, v := range attrs {
			if i >= 64 {
				break
			}
			trimmed[k] = v
			i++
		}
		attrs = trimmed
	}
	batch := logBatch{
		Schema:     "chronos.tracing.log-batch.v1",
		Processing: c.cfg.processing(seedNow()),
		Logs: []logRecord{{
			Organisation:   c.cfg.organisationRef(),
			Application:    c.cfg.applicationRef(),
			TraceID:        traceID,
			SpanID:         spanID,
			ObservedAt:     formatRFC3339Nano(time.Now().UTC()),
			Severity:       severity,
			SeverityNumber: severityNumber(severity),
			Body:           body,
			Attributes:     attrs,
		}},
		LogCount: "1",
	}
	_, _ = c.writer.WriteJSON(spool.SignalLog, batch)
}

// SlogHandler returns an slog.Handler that mirrors records into Chronos .log batches
// while optionally forwarding to next (may be nil).
func (c *Client) SlogHandler(next slog.Handler) slog.Handler {
	return &slogBridge{client: c, next: next}
}

type slogBridge struct {
	client *Client
	next   slog.Handler
}

func (h *slogBridge) Enabled(ctx context.Context, level slog.Level) bool {
	if h.next != nil {
		return h.next.Enabled(ctx, level)
	}
	return h.client != nil && h.client.cfg.Enabled && h.client.cfg.LogsEnabled
}

func (h *slogBridge) Handle(ctx context.Context, record slog.Record) error {
	if h.client != nil && h.client.cfg.Enabled && h.client.cfg.LogsEnabled {
		attrs := map[string]string{}
		record.Attrs(func(a slog.Attr) bool {
			if len(attrs) < 64 {
				attrs[a.Key] = a.Value.String()
			}
			return true
		})
		h.client.Log(ctx, slogLevelName(record.Level), record.Message, attrs)
	}
	if h.next != nil {
		return h.next.Handle(ctx, record)
	}
	return nil
}

func (h *slogBridge) WithAttrs(attrs []slog.Attr) slog.Handler {
	var next slog.Handler
	if h.next != nil {
		next = h.next.WithAttrs(attrs)
	}
	return &slogBridge{client: h.client, next: next}
}

func (h *slogBridge) WithGroup(name string) slog.Handler {
	var next slog.Handler
	if h.next != nil {
		next = h.next.WithGroup(name)
	}
	return &slogBridge{client: h.client, next: next}
}

func slogLevelName(level slog.Level) string {
	switch {
	case level < slog.LevelInfo:
		return "debug"
	case level < slog.LevelWarn:
		return "info"
	case level < slog.LevelError:
		return "warn"
	default:
		return "error"
	}
}

func severityNumber(name string) int {
	switch name {
	case "trace":
		return 1
	case "debug":
		return 5
	case "info", "notice":
		return 9
	case "warn", "warning":
		return 13
	case "error":
		return 17
	case "fatal", "critical":
		return 21
	default:
		return 0
	}
}

type logBatch struct {
	Schema     string             `json:"schema"`
	Processing ProcessingIdentity `json:"processing"`
	Logs       []logRecord        `json:"logs"`
	LogCount   string             `json:"logCount"`
}

type logRecord struct {
	Organisation   OrganisationRef   `json:"organisation"`
	Application    ApplicationRef    `json:"application"`
	TraceID        string            `json:"traceId,omitempty"`
	SpanID         string            `json:"spanId,omitempty"`
	ObservedAt     string            `json:"observedAt"`
	Severity       string            `json:"severity"`
	SeverityNumber int               `json:"severityNumber"`
	Body           string            `json:"body"`
	Attributes     map[string]string `json:"attributes"`
}
