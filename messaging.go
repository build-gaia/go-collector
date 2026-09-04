package chronos

import (
	"context"
	"strconv"
	"time"
)

// Messaging spans: the half of the producer/consumer topology no broker can supply.
//
// A broker records that a connection published to a destination. It does not record
// which SERVICE that connection belonged to - Kafka comes closest with client.id and
// that is a client-library string, not a service name. So the writer half of a
// messaging topology can only come from the application, which is what this file
// emits: one span per publish and one per consumed message, carrying the OTel
// messaging attributes the engine indexes and the desktop's Producers view joins on
// (messaging.destination.name matched to a stream, direction read off
// messaging.operation).
//
// Deliberately transport-agnostic. No Kafka, AMQP or Redis type appears here, so the
// core SDK keeps its four dependencies and a second transport is a wrapper rather
// than a change to this file. The Sarama wrapper lives in contrib/sarama, which is
// its own module for the same reason.

// The OTel messaging operation values. `process` is the one to reach for in a
// consumer: `receive` means the message arrived, `process` means the handler ran,
// and only the second one has a duration worth looking at.
const (
	OperationPublish = "publish"
	OperationReceive = "receive"
	OperationProcess = "process"
)

// MessagingSpan describes one message crossing the boundary.
//
// Only System, Operation and Destination are required - they are the three the
// join needs. Everything else is context, and an absent field writes no attribute
// rather than an empty one, because "" on messaging.kafka.partition would sort and
// group as a real partition.
type MessagingSpan struct {
	// `kafka`, `redis`, `rabbitmq` - the transport's own name, not a closed
	// vocabulary (ADR 0024 §5).
	System string
	// One of the Operation* constants.
	Operation string
	// The topic, queue, exchange or channel. THE join key: it is matched against a
	// stream's name to attribute the write.
	Destination string
	// The broker address, when the caller knows it. Lets two clusters with a topic
	// of the same name stay apart.
	Server string
	// Broker coordinates, when known. Zero values are written; a negative partition
	// or offset is treated as unknown, which is how Sarama spells "not assigned".
	Partition int32
	Offset    int64
	// Payload size in bytes, when known. -1 is unknown.
	BodySize int
	// The message key / id, when the caller has one that is safe to record.
	MessageID string
	// Consumer group, on a consume span.
	ConsumerGroup string
}

// spanName is what the desktop shows in a trace. `PUBLISH orders` reads the way
// `SQL SELECT` and `HTTP GET` already do, so a messaging span is recognisable in a
// waterfall without reading its attributes.
func (m MessagingSpan) spanName() string {
	operation := m.Operation
	if operation == "" {
		operation = OperationPublish
	}
	name := upperASCII(operation)
	if m.Destination == "" {
		return name
	}
	return name + " " + m.Destination
}

func upperASCII(value string) string {
	out := []byte(value)
	for i, c := range out {
		if c >= 'a' && c <= 'z' {
			out[i] = c - 32
		}
	}
	return string(out)
}

// StartMessagingSpan begins a messaging span and returns it with the derived
// context, exactly like StartSpan. The caller ends it.
//
// Attributes are set at start rather than at end so a span that is never ended
// (a panicking handler, a cancelled context) still carries what it was for.
func (c *Client) StartMessagingSpan(
	ctx context.Context,
	message MessagingSpan,
) (context.Context, *Span) {
	ctx, span := c.StartSpan(ctx, message.spanName())
	span.SetAttribute("span.kind", spanKindFor(message.Operation))
	if message.System != "" {
		span.SetAttribute("messaging.system", message.System)
	}
	if message.Operation != "" {
		span.SetAttribute("messaging.operation", message.Operation)
	}
	if message.Destination != "" {
		span.SetAttribute("messaging.destination.name", message.Destination)
	}
	if message.Server != "" {
		span.SetAttribute("server.address", message.Server)
	}
	if message.ConsumerGroup != "" {
		span.SetAttribute("messaging.consumer.group.name", message.ConsumerGroup)
	}
	if message.MessageID != "" {
		span.SetAttribute("messaging.message.id", message.MessageID)
	}
	if message.Partition >= 0 {
		span.SetAttribute("messaging.kafka.partition", attrInt(int(message.Partition)))
	}
	if message.Offset >= 0 {
		span.SetAttribute("messaging.kafka.offset", attrInt64(message.Offset))
	}
	if message.BodySize >= 0 {
		span.SetAttribute("messaging.message.body.size", attrInt(message.BodySize))
	}
	return ctx, span
}

// A publish is a client call; a receive or process is server-side work. The same
// distinction span.kind already carries for HTTP, and what lets a consumer span be
// the root of its own trace without looking like an outbound request.
func spanKindFor(operation string) string {
	if operation == OperationPublish {
		return "client"
	}
	return "consumer"
}

// RecordMessaging runs fn inside a messaging span, ending it with the right status.
//
// The convenience form, for a wrapper that has a natural function boundary. A
// returned error marks the span errored and is passed through untouched: telemetry
// never changes what the caller sees.
func (c *Client) RecordMessaging(
	ctx context.Context,
	message MessagingSpan,
	fn func(context.Context) error,
) error {
	ctx, span := c.StartMessagingSpan(ctx, message)
	defer span.End()
	err := fn(ctx)
	if err != nil {
		span.SetStatus("error")
		span.SetAttribute("error.message", err.Error())
	}
	return err
}

// TraceparentFromContext renders the active span as a W3C traceparent, or "" when
// there is no span to propagate.
//
// This is what makes a consumer span a CHILD of the publish that caused it, across
// process and language boundaries: the producer writes it as a message header and
// the consumer parses it back. Chronos's PHP collector reads the same header, so a
// PHP publisher and a Go consumer land in one trace.
func TraceparentFromContext(ctx context.Context) string {
	span := SpanFromContext(ctx)
	if span == nil {
		return ""
	}
	return span.Traceparent()
}

// ContextWithRemoteSpan makes an extracted trace context the parent of everything
// started under the returned context.
//
// The returned span is a stub: it is never ended and never recorded, it exists only
// to carry the remote ids so StartSpan parents onto them. A malformed or absent
// header returns ctx unchanged, so a consumer that receives an uninstrumented
// message starts its own trace rather than dropping the message's telemetry.
func ContextWithRemoteSpan(ctx context.Context, traceparent string) context.Context {
	traceID, spanID, ok := ParseTraceparent(traceparent)
	if !ok {
		return ctx
	}
	return context.WithValue(ctx, spanContextKey{}, &Span{
		TraceID:    traceID,
		SpanID:     spanID,
		StartedAt:  time.Now().UTC(),
		Attributes: map[string]string{},
		ended:      true,
	})
}

// TraceparentHeader is the header name both ends use. W3C, not a Chronos-specific
// name: a broker header set by any OTel SDK is readable here and vice versa.
const TraceparentHeader = "traceparent"

func attrInt64(n int64) string {
	return strconv.FormatInt(n, 10)
}
