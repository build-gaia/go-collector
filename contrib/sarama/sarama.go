// Package sarama instruments IBM/sarama Kafka clients with Chronos messaging spans.
//
// Its own module, not part of the core SDK, for the reason dd-trace-go splits its
// contribs: the core SDK has four dependencies and a service that speaks no Kafka
// should not compile sarama to get HTTP tracing. A service that does speak Kafka
// adds one more require line.
//
// # What it produces
//
// One span per published message (`PUBLISH orders`) and one per consumed message
// (`PROCESS orders`), each carrying the OTel messaging attributes: system,
// operation, destination, partition, offset, body size, consumer group. Those are
// exactly the attributes the engine indexes and the desktop's Producers view joins
// on - `messaging.destination.name` matched to a stream, direction read off
// `messaging.operation` - so instrumenting a service here is what turns that view's
// "not instrumented" into a named writer.
//
// # Batch consumers
//
// A consumer that accumulates messages and flushes them in groups is a different
// shape, and the per-message wrapper measures it wrongly: the span closes as the
// message is appended to a slice, long before the flush does the work. batch.go has
// the API for that case - a span over the flush, and a per-message context the
// handler can hand to whatever one message causes.
//
// # Trace continuity across the broker
//
// The producer writes the active span as a `traceparent` message header and the
// consumer parses it back, so the work a message causes is a child of the publish
// that caused it - across processes and across languages, since the PHP collector
// reads the same header. Alongside it go the `tracestate` and `baggage` this
// process received (W3C obliges a forwarder to pass on state it does not
// understand) and `x-chronos-enqueued-at`, the publish instant a consumer needs to
// say how long the message waited. The set, the spelling and the number format are
// PHP's MessagingSpan::contextHeaders(), so a mixed-language topic is one topic.
//
// A message with no header, or one whose traceparent does not satisfy the W3C
// grammar both SDKs enforce, starts its own trace rather than being dropped or
// parented onto an id nobody minted: an uninstrumented publisher must not cost the
// consumer its telemetry, and a believed-but-wrong parent is worse than none.
//
// # Fail-open, always
//
// Every wrapper returns the underlying value's result unchanged. A nil client, a
// disabled SDK or a spool that will not accept writes costs telemetry and nothing
// else; no wrapper here can fail a publish or lose a message.
package sarama

import (
	"sync"

	"context"
	"strconv"
	"strings"
	"time"

	chronos "chronos.dev/collector/sdk/go"
	"github.com/IBM/sarama"
)

// System is the `messaging.system` value every span here carries.
const System = "kafka"

// client resolves to the process default when the caller passes nil, which is the
// common case: chronos.Start() has already set it.
func client(c *chronos.Client) *chronos.Client {
	if c != nil {
		return c
	}
	return chronos.Default()
}

// --- Producer ---------------------------------------------------------------

// SyncProducer wraps a sarama.SyncProducer so every send is a span.
//
// It implements sarama.SyncProducer, so it drops into an existing call site
// without touching the code that uses it.
type SyncProducer struct {
	sarama.SyncProducer
	client *chronos.Client
	// Brokers is written to `server.address`, so two clusters holding a topic of
	// the same name stay apart. Optional.
	brokers string
}

// WrapSyncProducer returns a producer that records a publish span per message.
//
// `ctx` is not a parameter because sarama's SendMessage carries none. Use
// SendMessageContext to parent a publish onto the work that caused it; plain
// SendMessage still records a span, it is simply a root.
func WrapSyncProducer(
	producer sarama.SyncProducer,
	c *chronos.Client,
	brokers []string,
) *SyncProducer {
	return &SyncProducer{
		SyncProducer: producer,
		client:       client(c),
		brokers:      strings.Join(brokers, ","),
	}
}

// SendMessage records a span around the underlying send.
func (p *SyncProducer) SendMessage(message *sarama.ProducerMessage) (int32, int64, error) {
	return p.SendMessageContext(context.Background(), message)
}

// SendMessageContext is SendMessage with a parent context, so the publish is a
// child of the request or job that caused it and the trace survives the broker.
func (p *SyncProducer) SendMessageContext(
	ctx context.Context,
	message *sarama.ProducerMessage,
) (int32, int64, error) {
	if p == nil || message == nil {
		return p.SyncProducer.SendMessage(message)
	}
	ctx, span := p.client.StartMessagingSpan(ctx, chronos.MessagingSpan{
		System:      System,
		Operation:   chronos.OperationPublish,
		Destination: message.Topic,
		Server:      p.brokers,
		// Partition and offset are the broker's answer, not the caller's ask, so
		// they are unknown until the send returns and are set below.
		Partition: -1,
		Offset:    -1,
		BodySize:  encoderLength(message.Value),
		Body:      encoderBytes(p.client, message.Value),
		MessageID: keyOf(message.Key),
	})
	defer span.End()

	inject(ctx, message)

	partition, offset, err := p.SyncProducer.SendMessage(message)
	if err != nil {
		span.SetStatus("error")
		span.SetAttribute("error.message", err.Error())
		return partition, offset, err
	}
	span.SetAttribute("messaging.kafka.partition", itoa(int64(partition)))
	span.SetAttribute("messaging.kafka.offset", itoa(offset))
	return partition, offset, nil
}

// SendMessages records one span per message in the batch.
//
// Per message rather than per batch: a topic is a per-message property, so a batch
// spanning three topics has to attribute three destinations or the join it feeds is
// wrong. The underlying call is still one round trip.
func (p *SyncProducer) SendMessages(messages []*sarama.ProducerMessage) error {
	return p.SendMessagesContext(context.Background(), messages)
}

// SendMessagesContext is SendMessages with a parent context.
func (p *SyncProducer) SendMessagesContext(
	ctx context.Context,
	messages []*sarama.ProducerMessage,
) error {
	if p == nil {
		return p.SyncProducer.SendMessages(messages)
	}
	spans := make([]*chronos.Span, 0, len(messages))
	for _, message := range messages {
		if message == nil {
			continue
		}
		messageCtx, span := p.client.StartMessagingSpan(ctx, chronos.MessagingSpan{
			System:      System,
			Operation:   chronos.OperationPublish,
			Destination: message.Topic,
			Server:      p.brokers,
			Partition:   -1,
			Offset:      -1,
			BodySize:    encoderLength(message.Value),
			Body:        encoderBytes(p.client, message.Value),
			MessageID:   keyOf(message.Key),
		})
		inject(messageCtx, message)
		spans = append(spans, span)
	}
	err := p.SyncProducer.SendMessages(messages)
	for _, span := range spans {
		if err != nil {
			span.SetStatus("error")
			span.SetAttribute("error.message", err.Error())
		}
		span.End()
	}
	return err
}

// inject writes the outbound context onto the record headers: the W3C traceparent,
// the tracestate and baggage this process is forwarding, and the publish instant.
//
// The set and the spelling are PHP's MessagingSpan::contextHeaders(), because a
// mixed-language topic only works if both producers write the same fields — a Go
// publish that omits tracestate ends another vendor's context at the broker, and
// one that omits the enqueued-at stamp makes queue wait unmeasurable for every PHP
// consumer of that topic (MessagingWait has nothing to subtract from).
//
// A header the caller already set is left alone, per key. That is PHP's
// `$headers + contextHeaders()` precedence: they propagated it deliberately, and
// overwriting would reparent their trace or restate their clock.
//
// The stamp rides even when there is no span to propagate. A message published from
// a cron with no trace to continue has still waited just as long, and dropping the
// stamp would throw that measurement away too.
func inject(ctx context.Context, message *sarama.ProducerMessage) {
	span := chronos.SpanFromContext(ctx)
	setHeader(message, chronos.TraceparentHeader, span.Traceparent())
	setHeader(message, chronos.TracestateHeader, chronos.TracestateFromContext(ctx))
	setHeader(message, chronos.BaggageHeader, chronos.BaggageFromContext(ctx))
	setHeader(message, chronos.EnqueuedAtHeader, chronos.FormatEnqueuedAt(time.Now()))
}

// setHeader appends one record header unless it is empty or the caller already
// wrote that key. Kafka record header keys are raw bytes under whatever case the
// producer chose, so the comparison is case-insensitive in both directions.
func setHeader(message *sarama.ProducerMessage, key, value string) {
	if value == "" {
		return
	}
	for _, existing := range message.Headers {
		if strings.EqualFold(string(existing.Key), key) {
			return
		}
	}
	message.Headers = append(message.Headers, sarama.RecordHeader{
		Key:   []byte(key),
		Value: []byte(value),
	})
}

// --- Consumer ---------------------------------------------------------------

// ConsumerGroupHandler wraps a sarama.ConsumerGroupHandler so every message
// handled becomes a span, parented onto the publish that produced it.
type ConsumerGroupHandler struct {
	sarama.ConsumerGroupHandler
	client *chronos.Client
	group  string
}

// WrapConsumerGroupHandler returns a handler that records a process span per
// message. `group` is the consumer group id, written to
// `messaging.consumer.group.name` - the same identity the broker reports as a
// consumer group, so a span joins to the group the metrics side already knows.
func WrapConsumerGroupHandler(
	handler sarama.ConsumerGroupHandler,
	c *chronos.Client,
	group string,
) *ConsumerGroupHandler {
	return &ConsumerGroupHandler{ConsumerGroupHandler: handler, client: client(c), group: group}
}

// ConsumeClaim wraps the claim's message channel so the wrapped handler reads
// instrumented messages.
//
// The span cannot wrap ConsumeClaim itself: that call lives as long as the
// partition assignment, which is minutes to hours, and one span over it would
// report a duration that is a rebalance interval rather than any unit of work. So
// the channel is proxied and each message gets its own span, ended when the
// handler asks for the next one - which is exactly the handler's own processing
// time for that message.
func (h *ConsumerGroupHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	return h.ConsumerGroupHandler.ConsumeClaim(session, &instrumentedClaim{
		ConsumerGroupClaim: claim,
		handler:            h,
		session:            session,
	})
}

type instrumentedClaim struct {
	sarama.ConsumerGroupClaim
	handler *ConsumerGroupHandler
	session sarama.ConsumerGroupSession

	// once guards the proxy so that Messages() is idempotent, the way
	// sarama's own is.
	once sync.Once
	out  <-chan *sarama.ConsumerMessage
}

// Messages proxies the claim channel, opening a span as each message is handed to
// the handler and closing it when the next one is taken (or the channel closes).
//
// It returns the SAME channel on every call. sarama's ConsumerGroupClaim does,
// and handlers rely on it: `for { select { case m := <-claim.Messages(): } }` is
// an ordinary way to write a claim loop. Spawning a proxy per call turns that
// into a goroutine leak where every orphan races the others for the source and
// then blocks forever writing into a channel nobody holds, so messages are
// consumed from the broker and silently dropped.
func (c *instrumentedClaim) Messages() <-chan *sarama.ConsumerMessage {
	c.once.Do(func() {
		source := c.ConsumerGroupClaim.Messages()
		out := make(chan *sarama.ConsumerMessage)
		c.out = out
		go func() {
			defer close(out)
			var open *chronos.Span
			defer func() { open.End() }()
			for message := range source {
				// The previous message is done the moment the handler asks for
				// the next one.
				open.End()
				open = c.handler.start(c.session.Context(), message)
				out <- message
			}
		}()
	})
	return c.out
}

// start opens the span for one consumed message, parented onto the publish when the
// message carries a traceparent.
func (h *ConsumerGroupHandler) start(
	ctx context.Context,
	message *sarama.ConsumerMessage,
) *chronos.Span {
	_, span := startProcessSpan(ctx, h.client, h.group, message)
	return span
}

// startProcessSpan is THE definition of what a consumed message looks like, used by
// both the per-message wrapper above and the batch API in batch.go.
//
// One definition rather than two because the attribute set is a contract with the
// engine, not a local choice: `messaging.destination.name` is what attributes the
// span to a stream and `messaging.operation` is what gives it a direction, so a
// second copy that drifted by one key would make half this service's consume spans
// silently unjoinable. It returns the derived context as well as the span, because
// the batch API's whole purpose is handing that context to the handler.
func startProcessSpan(
	ctx context.Context,
	c *chronos.Client,
	group string,
	message *sarama.ConsumerMessage,
) (context.Context, *chronos.Span) {
	if message == nil {
		return ctx, nil
	}
	remote, enqueuedAt := extract(message)
	ctx = chronos.ContextWithRemoteContext(ctx, remote)
	started := time.Now()
	ctx, span := c.StartMessagingSpan(ctx, chronos.MessagingSpan{
		System:        System,
		Operation:     chronos.OperationProcess,
		Destination:   message.Topic,
		ConsumerGroup: group,
		Partition:     message.Partition,
		Offset:        message.Offset,
		BodySize:      len(message.Value),
		Body:          consumeBody(c, message.Value),
		MessageID:     string(message.Key),
	})
	// How long the message sat between the publish and this handler. Only the
	// producer's stamp can supply it, and only when it is there and believable: a
	// backed-up queue and a slow handler produce the same handler duration and are
	// told apart by nothing else.
	if waited, ok := chronos.WaitMilliseconds(enqueuedAt, started); ok {
		span.SetAttribute("messaging.message.queue_time_ms", itoa(waited))
	}
	return ctx, span
}

// extract reads the context a producer wrote: the W3C trio, plus the enqueued-at
// stamp returned separately because it is a measurement rather than a parent.
//
// Case-insensitively keyed, matching PHP's MessagingSpan::inboundContext: Kafka
// record header keys are raw bytes under whatever case the producer used, and an
// OTel SDK on the other end is under no obligation to pick ours. A malformed
// traceparent yields no parent at all, which roots the consume span in its own
// trace — honest, and better than parenting onto an id nobody can join to.
func extract(message *sarama.ConsumerMessage) (chronos.RemoteContext, string) {
	var traceparent, tracestate, baggage, enqueuedAt string
	for _, header := range message.Headers {
		if header == nil {
			continue
		}
		value := string(header.Value)
		switch {
		case strings.EqualFold(string(header.Key), chronos.TraceparentHeader):
			traceparent = value
		case strings.EqualFold(string(header.Key), chronos.TracestateHeader):
			tracestate = value
		case strings.EqualFold(string(header.Key), chronos.BaggageHeader):
			baggage = value
		case strings.EqualFold(string(header.Key), chronos.EnqueuedAtHeader):
			enqueuedAt = value
		}
	}
	remote, ok := chronos.ParseTraceparentContext(traceparent)
	if !ok {
		return chronos.RemoteContext{}, enqueuedAt
	}
	remote.Tracestate = chronos.NormalizeTracestate(tracestate)
	remote.Baggage = chronos.NormalizeBaggage(baggage)
	return remote, enqueuedAt
}

// --- helpers -----------------------------------------------------------------

// encoderLength is the message size when sarama can give one without re-encoding,
// and -1 otherwise. Never encodes to measure: that would double the CPU cost of a
// publish to fill in one attribute.
func encoderLength(encoder sarama.Encoder) int {
	if encoder == nil {
		return -1
	}
	return encoder.Length()
}

func encoderBytes(c *chronos.Client, encoder sarama.Encoder) []byte {
	if encoder == nil || !c.CaptureMessagingBodies() {
		return nil
	}
	body, err := encoder.Encode()
	if err != nil {
		return nil
	}
	return body
}

func consumeBody(c *chronos.Client, value []byte) []byte {
	if !c.CaptureMessagingBodies() {
		return nil
	}
	return value
}

// keyOf renders a message key as an id, and only when it is already text. A binary
// key (an Avro-encoded id, a protobuf) is not rendered: a mangled string in
// `messaging.message.id` is worse than no id at all.
func keyOf(key sarama.Encoder) string {
	text, ok := key.(sarama.StringEncoder)
	if !ok {
		return ""
	}
	return string(text)
}

func itoa(n int64) string {
	return strconv.FormatInt(n, 10)
}
