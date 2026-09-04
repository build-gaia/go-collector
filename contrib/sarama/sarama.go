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
// # Trace continuity across the broker
//
// The producer writes the active span as a `traceparent` message header and the
// consumer parses it back, so the work a message causes is a child of the publish
// that caused it - across processes and across languages, since the PHP collector
// reads the same header. A message with no header starts its own trace rather than
// being dropped: an uninstrumented publisher must not cost the consumer its
// telemetry.
//
// # Fail-open, always
//
// Every wrapper returns the underlying value's result unchanged. A nil client, a
// disabled SDK or a spool that will not accept writes costs telemetry and nothing
// else; no wrapper here can fail a publish or lose a message.
package sarama

import (
	"context"
	"strconv"
	"strings"

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

// inject writes the active span onto the message as a W3C traceparent header. A
// message that already carries one is left alone - the caller propagated it
// deliberately and overwriting would reparent their trace.
func inject(ctx context.Context, message *sarama.ProducerMessage) {
	header := chronos.TraceparentFromContext(ctx)
	if header == "" {
		return
	}
	for _, existing := range message.Headers {
		if strings.EqualFold(string(existing.Key), chronos.TraceparentHeader) {
			return
		}
	}
	message.Headers = append(message.Headers, sarama.RecordHeader{
		Key:   []byte(chronos.TraceparentHeader),
		Value: []byte(header),
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
}

// Messages proxies the claim channel, opening a span as each message is handed to
// the handler and closing it when the next one is taken (or the channel closes).
func (c *instrumentedClaim) Messages() <-chan *sarama.ConsumerMessage {
	source := c.ConsumerGroupClaim.Messages()
	out := make(chan *sarama.ConsumerMessage)
	go func() {
		defer close(out)
		var open *chronos.Span
		defer func() { open.End() }()
		for message := range source {
			// The previous message is done the moment the handler asks for the
			// next one.
			open.End()
			open = c.handler.start(c.session.Context(), message)
			out <- message
		}
	}()
	return out
}

// start opens the span for one consumed message, parented onto the publish when the
// message carries a traceparent.
func (h *ConsumerGroupHandler) start(
	ctx context.Context,
	message *sarama.ConsumerMessage,
) *chronos.Span {
	if message == nil {
		return nil
	}
	ctx = chronos.ContextWithRemoteSpan(ctx, extract(message))
	_, span := h.client.StartMessagingSpan(ctx, chronos.MessagingSpan{
		System:        System,
		Operation:     chronos.OperationProcess,
		Destination:   message.Topic,
		ConsumerGroup: h.group,
		Partition:     message.Partition,
		Offset:        message.Offset,
		BodySize:      len(message.Value),
		MessageID:     string(message.Key),
	})
	return span
}

// extract reads the W3C traceparent a producer wrote, if any.
func extract(message *sarama.ConsumerMessage) string {
	for _, header := range message.Headers {
		if header == nil {
			continue
		}
		if strings.EqualFold(string(header.Key), chronos.TraceparentHeader) {
			return string(header.Value)
		}
	}
	return ""
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
