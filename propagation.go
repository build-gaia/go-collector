package chronos

import (
	"context"
	"strconv"
	"strings"
	"time"
)

// W3C trace context on the wire, and the one Chronos header that rides beside it.
//
// This file is the single place that decides what a Chronos process WRITES as
// context and what it is willing to BELIEVE when it reads context back. It exists
// because the two SDKs have to agree byte for byte: a trace that crosses PHP -> Go
// -> PHP is one trace only if both sides render the same traceparent grammar, both
// forward the same companion headers, and both refuse the same malformed input. The
// PHP side's authority is Service\TraceContext and Dto\SpanReservation; every rule
// below is that behaviour restated in Go, with the PHP location named where the
// rule is not obvious from the spec alone.
//
// Header names are W3C, not Chronos-specific, so a broker header set by any OTel
// SDK is readable here and vice versa. The one exception is spelled x-chronos-*
// because no standard covers it.
const (
	// TraceparentHeader carries trace id, parent span id and flags.
	TraceparentHeader = "traceparent"
	// TracestateHeader carries other vendors' state. A participant that forwards
	// traceparent MUST forward tracestate it does not understand
	// (https://www.w3.org/TR/trace-context/#mutating-the-tracestate-field), which
	// is the obligation the PHP bridges already honour in Service\Propagation.
	TracestateHeader = "tracestate"
	// BaggageHeader carries application key/value context, forwarded as-is.
	BaggageHeader = "baggage"
	// EnqueuedAtHeader is the publisher's wall clock at the moment of the send,
	// as seconds with six decimals. Spelled and formatted exactly as PHP's
	// Service\MessagingSpan::ENQUEUED_AT_HEADER, because queue wait is a
	// subtraction across two processes: the producer and the consumer have to
	// agree on the field name AND on the number format before either can measure
	// anything, and they are frequently not the same language.
	EnqueuedAtHeader = "x-chronos-enqueued-at"
)

// W3C caps, mirrored from PHP Service\TraceContext so a value one side keeps is a
// value the other side keeps.
const (
	maxTracestateEntries  = 32
	maxBaggageEntries     = 64
	maxBaggageHeaderBytes = 8192
	maxBaggageValueBytes  = 4096
	maxBaggageKeyBytes    = 256
)

// RemoteContext is the trace context extracted from an inbound carrier: a request's
// headers, a broker record's headers, a message's property table.
//
// Sampled is the PUBLISHER's flag, unmodified — including false. Carrying it is the
// whole point: the previous Go SDK hardcoded `-01` on everything it wrote, so a PHP
// service that decided NOT to sample had that decision overwritten the moment a Go
// hop forwarded the context, and half a dropped trace reappeared downstream.
type RemoteContext struct {
	TraceID    string
	SpanID     string
	Sampled    bool
	Tracestate string
	Baggage    string
}

// Valid reports whether there is a parent worth adopting.
func (r RemoteContext) Valid() bool {
	return r.TraceID != "" && r.SpanID != ""
}

// ParseTraceparentContext parses a W3C traceparent, strictly.
//
// Strict means the exact grammar PHP enforces in TraceContext::fromHeader
// (`/^00-([a-f0-9]{32})-([a-f0-9]{16})-([a-f0-9]{2})$/D`, all-zero ids refused):
// version `00` only, LOWERCASE hex, and neither id all zeroes. This SDK used to
// check lengths alone, and each thing that let through was a real defect rather
// than a pedantic one:
//
//   - Uppercase hex was accepted and then re-propagated verbatim, so a Go hop
//     between two PHP hops emitted a trace id that would not string-match the
//     lowercase one either side had written — one logical trace, two trace ids.
//   - An all-zero id is forbidden by W3C precisely because it names nothing; a
//     span parented onto it is an orphan that looks connected.
//   - Version `ff` is invalid by definition, and a future version's field layout
//     is not knowable, so guessing at it invents a parent.
//
// Refusing is not data loss: the caller starts a fresh trace instead, which is the
// honest answer and the same one PHP reaches (its fromHeader throws, and the native
// request start mints a new context).
func ParseTraceparentContext(header string) (RemoteContext, bool) {
	fields := strings.Split(strings.TrimSpace(header), "-")
	if len(fields) != 4 {
		return RemoteContext{}, false
	}
	if fields[0] != "00" {
		return RemoteContext{}, false
	}
	if !lowerHex(fields[1], 32) || !lowerHex(fields[2], 16) || !lowerHex(fields[3], 2) {
		return RemoteContext{}, false
	}
	if allZero(fields[1]) || allZero(fields[2]) {
		return RemoteContext{}, false
	}
	flags, err := strconv.ParseUint(fields[3], 16, 8)
	if err != nil {
		return RemoteContext{}, false
	}
	return RemoteContext{
		TraceID: fields[1],
		SpanID:  fields[2],
		Sampled: flags&1 == 1,
	}, true
}

// ParseTraceparent is ParseTraceparentContext for callers that only want the ids.
func ParseTraceparent(header string) (traceID, spanID string, ok bool) {
	remote, ok := ParseTraceparentContext(header)
	return remote.TraceID, remote.SpanID, ok
}

// FormatTraceparent renders the wire value, byte-identical to PHP's
// TraceContext::header() and SpanReservation::header().
func FormatTraceparent(traceID, spanID string, sampled bool) string {
	if traceID == "" || spanID == "" {
		return ""
	}
	flags := "00"
	if sampled {
		flags = "01"
	}
	return "00-" + traceID + "-" + spanID + "-" + flags
}

func lowerHex(value string, length int) bool {
	if len(value) != length {
		return false
	}
	for i := 0; i < len(value); i++ {
		c := value[i]
		if (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') {
			continue
		}
		return false
	}
	return true
}

func allZero(value string) bool {
	for i := 0; i < len(value); i++ {
		if value[i] != '0' {
			return false
		}
	}
	return len(value) > 0
}

// ContextWithRemoteContext makes an extracted context the parent of everything
// started under the returned context, carrying the companion headers along so the
// next hop out of this process forwards what came in.
//
// The stub span is never ended and never recorded; it exists only to hold the
// remote ids where StartSpan looks for a parent. An invalid context returns ctx
// unchanged, so a consumer handed an uninstrumented (or malformed) message starts
// its own trace rather than losing its telemetry.
func ContextWithRemoteContext(ctx context.Context, remote RemoteContext) context.Context {
	if !remote.Valid() {
		return ctx
	}
	return context.WithValue(ctx, spanContextKey{}, &Span{
		TraceID:    remote.TraceID,
		SpanID:     remote.SpanID,
		Sampled:    remote.Sampled,
		Tracestate: remote.Tracestate,
		Baggage:    remote.Baggage,
		StartedAt:  time.Now().UTC(),
		Attributes: map[string]string{},
		ended:      true,
	})
}

// ContextWithRemoteSpan is ContextWithRemoteContext for a bare traceparent, kept
// for callers that have no companion headers to pass.
func ContextWithRemoteSpan(ctx context.Context, traceparent string) context.Context {
	remote, ok := ParseTraceparentContext(traceparent)
	if !ok {
		return ctx
	}
	return ContextWithRemoteContext(ctx, remote)
}

// TraceparentFromContext renders the active span as a W3C traceparent, or "" when
// there is no span to propagate.
//
// This is what makes a consumer span a CHILD of the publish that caused it, across
// process and language boundaries: the producer writes it as a message header and
// the consumer parses it back.
func TraceparentFromContext(ctx context.Context) string {
	return SpanFromContext(ctx).Traceparent()
}

// TracestateFromContext is the tracestate this process received, or "" — the value
// to forward next to an outbound traceparent.
func TracestateFromContext(ctx context.Context) string {
	span := SpanFromContext(ctx)
	if span == nil {
		return ""
	}
	return span.Tracestate
}

// BaggageFromContext is the baggage this process received, or "".
func BaggageFromContext(ctx context.Context) string {
	span := SpanFromContext(ctx)
	if span == nil {
		return ""
	}
	return span.Baggage
}

// NormalizeTracestate parses and re-renders a tracestate header the way PHP's
// TraceContext does: order-preserving, first occurrence of a key wins, members that
// do not match the ABNF dropped, at most 32 members. "" means "nothing worth
// forwarding".
//
// Re-rendering rather than echoing the bytes is what makes the two SDKs agree on
// what a forwarded tracestate LOOKS like — and it is also the only thing standing
// between an attacker-supplied header and a CRLF injected into a broker frame or an
// outbound request.
func NormalizeTracestate(header string) string {
	header = strings.TrimSpace(header)
	if header == "" {
		return ""
	}
	seen := make(map[string]struct{}, 8)
	members := make([]string, 0, 8)
	for _, raw := range strings.Split(header, ",") {
		if len(members) >= maxTracestateEntries {
			break
		}
		member := strings.TrimSpace(raw)
		if member == "" {
			continue
		}
		equals := strings.Index(member, "=")
		if equals <= 0 {
			continue
		}
		key, value := member[:equals], member[equals+1:]
		if !validTracestateKey(key) || !validTracestateValue(value) {
			continue
		}
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		seen[key] = struct{}{}
		members = append(members, key+"="+value)
	}
	return strings.Join(members, ",")
}

// validTracestateKey is the tracestate ABNF's simple-key or tenant@system form.
func validTracestateKey(key string) bool {
	if tenant, system, multi := strings.Cut(key, "@"); multi {
		return len(tenant) >= 1 && len(tenant) <= 241 && len(system) >= 1 && len(system) <= 14 &&
			tracestateKeyStart(tenant[0], true) && tracestateKeyTail(tenant[1:]) &&
			tracestateKeyStart(system[0], false) && tracestateKeyTail(system[1:])
	}
	if len(key) < 1 || len(key) > 256 {
		return false
	}
	return tracestateKeyStart(key[0], false) && tracestateKeyTail(key[1:])
}

func tracestateKeyStart(c byte, digitsAllowed bool) bool {
	if c >= 'a' && c <= 'z' {
		return true
	}
	return digitsAllowed && c >= '0' && c <= '9'
}

func tracestateKeyTail(tail string) bool {
	for i := 0; i < len(tail); i++ {
		c := tail[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case c == '_' || c == '-' || c == '*' || c == '/':
		default:
			return false
		}
	}
	return true
}

func validTracestateValue(value string) bool {
	if len(value) < 1 || len(value) > 256 {
		return false
	}
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c < 0x20 || c > 0x7e || c == ',' || c == '=' {
			return false
		}
	}
	last := value[len(value)-1]
	return last != ' ' && last != '\t'
}

// NormalizeBaggage parses and re-renders a baggage header the way PHP's
// TraceContext does: properties after the first ';' dropped, values percent-decoded
// and re-encoded, order preserved, first key wins, bounded in members and bytes.
func NormalizeBaggage(header string) string {
	header = strings.TrimSpace(header)
	if header == "" || len(header) > maxBaggageHeaderBytes {
		return ""
	}
	seen := make(map[string]struct{}, 8)
	members := make([]string, 0, 8)
	for _, raw := range strings.Split(header, ",") {
		if len(members) >= maxBaggageEntries {
			break
		}
		member := strings.TrimSpace(raw)
		if member == "" {
			continue
		}
		if semi := strings.Index(member, ";"); semi >= 0 {
			member = strings.TrimSpace(member[:semi])
		}
		equals := strings.Index(member, "=")
		if equals <= 0 {
			continue
		}
		key := strings.TrimSpace(member[:equals])
		encoded := strings.TrimSpace(member[equals+1:])
		if key == "" || encoded == "" || len(key) > maxBaggageKeyBytes || !validBaggageKey(key) {
			continue
		}
		value := percentDecode(encoded)
		if len(value) > maxBaggageValueBytes {
			continue
		}
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		seen[key] = struct{}{}
		members = append(members, key+"="+percentEncode(value))
	}
	return strings.Join(members, ",")
}

// validBaggageKey is an RFC 7230 token, as PHP spells it.
func validBaggageKey(key string) bool {
	for i := 0; i < len(key); i++ {
		c := key[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case strings.IndexByte("!#$%&'*+-.^_`|~", c) >= 0:
		default:
			return false
		}
	}
	return len(key) > 0
}

// percentDecode is PHP rawurldecode: %XX only, '+' is a literal plus (which is what
// separates it from form decoding, and why net/url's query helpers are wrong here).
// An invalid escape is left as written rather than dropped, so a value that was
// never encoded survives intact.
func percentDecode(value string) string {
	if !strings.Contains(value, "%") {
		return value
	}
	out := make([]byte, 0, len(value))
	for i := 0; i < len(value); i++ {
		if value[i] == '%' && i+2 < len(value) {
			hi, hiOK := hexNibble(value[i+1])
			lo, loOK := hexNibble(value[i+2])
			if hiOK && loOK {
				out = append(out, hi<<4|lo)
				i += 2
				continue
			}
		}
		out = append(out, value[i])
	}
	return string(out)
}

// percentEncode is PHP rawurlencode: RFC 3986 unreserved bytes pass, everything
// else becomes %XX in UPPERCASE hex, which is the casing PHP emits.
func percentEncode(value string) string {
	out := make([]byte, 0, len(value))
	const hex = "0123456789ABCDEF"
	for i := 0; i < len(value); i++ {
		c := value[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '-', c == '_', c == '.', c == '~':
			out = append(out, c)
		default:
			out = append(out, '%', hex[c>>4], hex[c&0x0f])
		}
	}
	return string(out)
}

func hexNibble(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}

// FormatEnqueuedAt renders a publish instant for EnqueuedAtHeader, in PHP's
// `%.6F` of microtime(true): seconds since the epoch, six decimals, no exponent.
func FormatEnqueuedAt(at time.Time) string {
	return strconv.FormatFloat(float64(at.UnixNano())/1e9, 'f', 6, 64)
}

// WaitMilliseconds is how long a message waited between the publisher's stamp and
// the consumer picking it up, and false when that cannot be said honestly.
//
// Port of PHP Service\MessagingWait, decision for decision, because the number is
// only comparable if both languages discard the same readings. A wall clock is the
// worse clock and the only possible one: a monotonic reading means nothing in
// another process. So a missing, unparseable or non-positive stamp is unknown, and
// a NEGATIVE reading is discarded whole rather than clamped — skew of a second in
// one direction is skew of a second in the other, and clamping would hide exactly
// the readings that reveal it while letting an unmeasured queue report the
// healthiest possible value, zero.
func WaitMilliseconds(enqueuedAt string, startedAt time.Time) (int64, bool) {
	stamp := strings.TrimSpace(enqueuedAt)
	if stamp == "" {
		return 0, false
	}
	seconds, err := strconv.ParseFloat(stamp, 64)
	if err != nil || seconds <= 0 {
		return 0, false
	}
	waited := (float64(startedAt.UnixNano())/1e9 - seconds) * 1000.0
	if waited < 0 {
		return 0, false
	}
	return int64(waited + 0.5), true
}
