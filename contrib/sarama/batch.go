package sarama

import (
	"context"
	"fmt"
	"sync"
	"time"

	chronos "chronos.dev/collector/sdk/go"
	"github.com/IBM/sarama"
)

// Batch consumption: what a consumer that flushes in groups needs that the
// per-message wrapper cannot give it.
//
// # The two things the per-message wrapper gets wrong here
//
// WrapConsumerGroupHandler opens a span as the handler takes a message off the
// channel and ends it when the handler takes the next one. For a handler that does
// the work right there, that IS the work. For a handler that accumulates - appends
// to a slice and returns to the select, flushing on size or age - it is the time
// spent appending to a slice. Both failures follow from that one fact:
//
//  1. Every PROCESS span has a duration of approximately zero, because the handler
//     really did take microseconds to append. The flush, which is where the seconds
//     are, is timed by nothing.
//  2. Whatever the flush publishes uses the context it has to hand, which is the
//     session's - a context that outlives every message in the batch and belongs to
//     none of them. So the publish is a root, the causal thread from the message
//     that caused it is cut, and a pipeline that really runs
//     `asn_item -> 1326-asn-items -> 1326-asn-items-sink` shows up as a scatter of
//     disconnected two-span traces instead of one story.
//
// The second is the expensive one. A duration nobody can trust is annoying; a trace
// that stops at every intermediary topic makes the whole graph unreadable.
//
// # The model
//
// A batch is TWO kinds of unit of work at once and the model carries both.
//
// The BATCH SPAN is the flush: one span, opened when the flush begins and ended when
// it returns, carrying the count and the offset range. It is what you look at to ask
// "how long is this consumer taking per batch, and how big are the batches". It is a
// root in this service's own trace, because it has 500 causes and no parent: see
// the note on links below.
//
// The MESSAGE SPAN is one message's share of that work, and it lives in the
// UPSTREAM trace, parented onto the publish that produced the message, exactly as
// the per-message wrapper's span does. That parentage is the entire point. Anything
// the handler does with that message's context - a publish to the next topic, a
// query, an HTTP call - lands in the producer's trace, so the chain through the
// intermediary topics is one trace from end to end.
//
// # Links, and why there are none
//
// OTel would express "this batch consumed 500 messages from 500 different traces" as
// 500 span links. The Chronos span model has no links: a span is a trace id, a span
// id, one parent id and a flat map of string attributes (see sdk/go/tracing.go -
// there is RemoteContext and Adopt, both of which set the single parent, and nothing
// that records a second relationship). Rather than invent a parallel link mechanism
// nothing downstream reads, the relationship is written as attributes on the message
// span - `messaging.batch.trace_id` and `messaging.batch.span_id` - and the batch
// span counts what pointed at it.
//
// That is genuinely second best and the tradeoff is worth stating: the reference
// points one way only (message -> batch), the engine cannot traverse it, and nothing
// in the desktop renders it today. What it buys is that the pointer survives in the
// attribute index, so "which batch handled this message" is answerable by query
// rather than not at all, and the day the span model grows links this becomes a
// mechanical rewrite of two SetAttribute calls. The alternative - parenting the
// batch span onto the first message's trace - was rejected outright: it would claim
// a causal relationship that does not exist and would drag 499 unrelated messages'
// work into one arbitrary producer's trace.
//
// # Cardinality
//
// 500 message spans per flush is not free, and this estate already emits tens of
// thousands of spans a minute from these consumers. So the default is
// MessageSpansOnDemand: a message span is minted only when the handler ASKS for that
// message's context, which is precisely when a causal thread would otherwise be cut.
// A handler that bulk-inserts a batch and asks for nothing pays one span. A handler
// that republishes per message pays one span per message and gets the pipeline it
// could not see before. You pay for causality where you use it.
//
// Sampling one in N was considered and rejected as the default. It would cost the
// same on average and give worse answers: the N-1 unsampled messages still produce
// publishes, those publishes still become roots, and you cannot tell by looking at a
// trace whether it is complete or a sampling casualty. Opt in with MessageSpansNever
// on a hot path where causality is not worth the volume, or MessageSpansAlways when
// you want the full picture while debugging.

// MessageSpans is when the batch API mints a span for an individual message.
type MessageSpans int

const (
	// MessageSpansOnDemand mints a message span the first time the handler asks for
	// that message's context, and never for a message it does not ask about. The
	// default: it is the setting under which a span exists exactly where a causal
	// thread would otherwise break.
	MessageSpansOnDemand MessageSpans = iota
	// MessageSpansAlways mints one for every message in the batch, whether the
	// handler asks or not. For when you want the per-message durations themselves,
	// and are willing to pay batch-size spans per flush for them.
	MessageSpansAlways
	// MessageSpansNever mints none. Everything the handler does hangs off the batch
	// span, so the flush is still timed and still attributed - only the per-message
	// causal thread is given up.
	MessageSpansNever
)

// BatchOptions configures a batch-aware consumer.
//
// The zero value is usable: the default client, no consumer group recorded,
// on-demand message spans, and - for WrapBatchConsumerGroupHandler - the
// accumulation limits below fall back to defaults.
type BatchOptions struct {
	// Group is the consumer group id, written to `messaging.consumer.group.name`.
	// The same identity the broker reports, so a span joins to the group the metrics
	// side already knows.
	Group string
	// MessageSpans is the cardinality policy. Zero is MessageSpansOnDemand.
	MessageSpans MessageSpans

	// The accumulation limits, read only by WrapBatchConsumerGroupHandler. A
	// handler that does its own batching sets none of these and calls ProcessBatch
	// at its own flush point.

	// MaxRecords flushes once the batch holds this many messages. Zero means no
	// record limit.
	MaxRecords int
	// MaxBytes flushes once the accumulated message values reach this many bytes.
	// Zero means no byte limit.
	MaxBytes int
	// MaxWait flushes a non-empty batch this long after the last flush, so a quiet
	// partition does not hold messages indefinitely. Zero means DefaultMaxWait.
	MaxWait time.Duration
}

// DefaultMaxWait is the flush age used when BatchOptions.MaxWait is zero. A second
// is long enough to batch usefully on a busy partition and short enough that a quiet
// one does not sit on a message for a human-noticeable time.
const DefaultMaxWait = time.Second

func (o BatchOptions) maxWait() time.Duration {
	if o.MaxWait > 0 {
		return o.MaxWait
	}
	return DefaultMaxWait
}

func (o BatchOptions) full(records, bytes int) bool {
	if o.MaxRecords > 0 && records >= o.MaxRecords {
		return true
	}
	return o.MaxBytes > 0 && bytes >= o.MaxBytes
}

// Batch is one flush, handed to the handler.
//
// It owns the batch span and every message span minted under it, and it is the only
// way to get a per-message context. Not safe for concurrent use: a handler that
// fans its messages out across goroutines must take a context per message on the
// owning goroutine first.
type Batch struct {
	client   *chronos.Client
	options  BatchOptions
	session  sarama.ConsumerGroupSession
	messages []*sarama.ConsumerMessage

	// span and ctx are the flush itself. span is nil when there is nothing to
	// record; every Span method tolerates that, and ctx is then the caller's own.
	span *chronos.Span
	ctx  context.Context
	// parent is the context the flush was handed, kept unmodified: a message span
	// derives from it so that cancellation and values still reach it, but the batch
	// span - which is in another trace - does not become its parent.
	parent context.Context

	// open is the message span currently running under MessageSpansOnDemand, closed
	// when the handler moves on to another message. See ContextFor.
	open        *chronos.Span
	openMessage *sarama.ConsumerMessage
	// contexts is the context handed back for each message, kept so a second ask
	// for the same message returns the same span rather than minting another.
	contexts map[*sarama.ConsumerMessage]context.Context
	// spans is every message span minted, so the batch can close any the handler
	// left running. Ending twice is a no-op, so the ones Each already closed cost
	// nothing here.
	spans  []*chronos.Span
	traced int

	ended sync.Once
}

// Messages are the messages in this flush, in the order they were consumed.
func (b *Batch) Messages() []*sarama.ConsumerMessage {
	if b == nil {
		return nil
	}
	return b.messages
}

// Len is the number of messages in this flush.
func (b *Batch) Len() int {
	if b == nil {
		return 0
	}
	return len(b.messages)
}

// Session is the consumer group session this flush came from, so a handler can mark
// offsets. Nil when the batch was not driven by a consumer group.
func (b *Batch) Session() sarama.ConsumerGroupSession {
	if b == nil {
		return nil
	}
	return b.session
}

// Context is the batch span's context: the right parent for work that belongs to the
// flush as a whole rather than to one message - the bulk insert, the object written
// to S3, the single HTTP call carrying all 500 records.
func (b *Batch) Context() context.Context {
	if b == nil || b.ctx == nil {
		return context.Background()
	}
	return b.ctx
}

// SetAttribute records an attribute on the batch span. For the facts only the
// handler knows: which feed, which destination, why the batch closed.
func (b *Batch) SetAttribute(key, value string) {
	if b == nil {
		return
	}
	b.span.SetAttribute(key, value)
}

// ContextFor is the answer to "how does a message keep its own context through the
// flush": it returns a context whose active span is THAT message's process span,
// parented onto the publish that produced it. A publish made with this context is a
// child of message N, not of the batch and not of the session, so the chain through
// the intermediary topic is one trace.
//
// It is also the low-friction retrofit. An existing flush loop already reads
// `for _, message := range messages`; adopting this is replacing the ctx it passes
// down with batch.ContextFor(message).
//
// Under MessageSpansOnDemand the span opens here and closes when the handler asks
// about a DIFFERENT message, which for the ordinary sequential loop makes its
// duration that message's own share of the flush - the same trick the streaming
// claim proxy uses, and for the same reason: the handler tells us it is finished
// with one message by asking for the next. Asking again about a message already
// closed returns the same context rather than minting a second span; its children
// then extend past their parent's end, which is untidy and still correctly
// parented.
//
// Under MessageSpansNever it returns the batch context, so the call site is written
// once and the policy stays a configuration decision.
func (b *Batch) ContextFor(message *sarama.ConsumerMessage) context.Context {
	if b == nil || message == nil {
		return b.Context()
	}
	if existing, ok := b.contexts[message]; ok {
		return existing
	}
	if b.options.MessageSpans == MessageSpansNever {
		return b.Context()
	}
	// The previous message is done the moment the handler moves to another one.
	if b.openMessage != message {
		b.open.End()
		b.open, b.openMessage = nil, nil
	}
	ctx, span := b.start(message)
	b.open, b.openMessage = span, message
	return ctx
}

// Each runs fn once per message with that message's own context, ending each message
// span as fn returns so its duration is exactly that message's work.
//
// The shape to reach for when the handler really is a loop over messages, and the
// only one that gives honest per-message durations: ContextFor can only guess that a
// message is finished when the next one is asked for, whereas fn returning says so.
//
// It stops at the first error and returns it untouched, matching a plain `for` loop
// with an early return. The message span is marked errored, the batch span is left
// to the caller's own error handling, and a panic propagates with the span closed.
func (b *Batch) Each(fn func(context.Context, *sarama.ConsumerMessage) error) error {
	if b == nil {
		return nil
	}
	for _, message := range b.messages {
		if message == nil {
			continue
		}
		if err := b.each(fn, message); err != nil {
			return err
		}
	}
	return nil
}

// each is one iteration, in its own frame so the span closes on a panic too.
func (b *Batch) each(
	fn func(context.Context, *sarama.ConsumerMessage) error,
	message *sarama.ConsumerMessage,
) error {
	if b.options.MessageSpans == MessageSpansNever {
		return fn(b.Context(), message)
	}
	// A message Each visits is a message the handler is working on, so any span
	// ContextFor left open for it is this one and must not outlive the iteration.
	if b.openMessage == message {
		b.open, b.openMessage = nil, nil
	}
	ctx, span := b.start(message)
	defer span.End()
	err := fn(ctx, message)
	if err != nil {
		span.SetStatus("error")
		span.SetAttribute("error.message", err.Error())
	}
	return err
}

// start opens one message span, or returns the one already open for that message.
//
// The parent is the message's own traceparent, NOT the batch span, which is why the
// batch reference has to be written as attributes: the message span is in the
// producer's trace and the batch span is in this service's, and a span has one
// parent in one trace.
func (b *Batch) start(message *sarama.ConsumerMessage) (context.Context, *chronos.Span) {
	if existing, ok := b.contexts[message]; ok {
		return existing, chronos.SpanFromContext(existing)
	}
	// The session context, not the batch context: inheriting the batch span would
	// make it the parent, and the traceparent on the message has the better claim.
	ctx, span := startProcessSpan(b.parentContext(), b.client, b.options.Group, message)
	if span != nil {
		span.SetAttribute("messaging.batch.index", itoa(int64(b.indexOf(message))))
		if b.span != nil {
			span.SetAttribute("messaging.batch.trace_id", b.span.TraceID)
			span.SetAttribute("messaging.batch.span_id", b.span.SpanID)
		}
	}
	if b.contexts == nil {
		b.contexts = map[*sarama.ConsumerMessage]context.Context{}
	}
	b.contexts[message] = ctx
	b.spans = append(b.spans, span)
	b.traced++
	return ctx, span
}

// parentContext is the context a message span derives from.
func (b *Batch) parentContext() context.Context {
	if b.parent == nil {
		return context.Background()
	}
	return b.parent
}

func (b *Batch) indexOf(message *sarama.ConsumerMessage) int {
	for i, candidate := range b.messages {
		if candidate == message {
			return i
		}
	}
	return -1
}

// end closes the batch span and anything still open under it. Idempotent, because it
// runs from a defer that also has to cover the panic path.
func (b *Batch) end() {
	b.ended.Do(func() {
		for _, span := range b.spans {
			span.End()
		}
		b.open, b.openMessage = nil, nil
		// Recorded at the end because it is the answer to "did the on-demand policy
		// actually mint anything here": a batch of 500 with 0 traced messages is a
		// handler that never asked for a message context, which is either fine or
		// the reason a trace stops at this topic.
		b.span.SetAttribute("messaging.batch.traced_message_count", itoa(int64(b.traced)))
		b.span.End()
	})
}

// ProcessBatch runs fn as one flush: a batch span around it, and a Batch through
// which fn can take a context per message.
//
// The primitive. WrapBatchConsumerGroupHandler is this function plus an accumulation
// loop, and a consumer that already accumulates its own batches - most of them do,
// because the policy is theirs - calls this directly at its flush point and keeps
// the loop it has.
//
// `ctx` is the context the flush runs under, ordinarily the session's. The error fn
// returns is passed back untouched and a panic propagates with every span closed:
// instrumenting a flush cannot change what the flush does.
func ProcessBatch(
	ctx context.Context,
	c *chronos.Client,
	options BatchOptions,
	messages []*sarama.ConsumerMessage,
	fn func(*Batch) error,
) error {
	batch := newBatch(ctx, client(c), options, nil, messages)
	return batch.run(fn)
}

// newBatch opens the batch span and assembles the Batch around it.
func newBatch(
	ctx context.Context,
	c *chronos.Client,
	options BatchOptions,
	session sarama.ConsumerGroupSession,
	messages []*sarama.ConsumerMessage,
) *Batch {
	if ctx == nil {
		ctx = context.Background()
	}
	batch := &Batch{
		client:   c,
		options:  options,
		session:  session,
		messages: messages,
		ctx:      ctx,
		parent:   ctx,
	}
	if len(messages) == 0 {
		// Nothing consumed, nothing to time. An empty flush is a no-op in every
		// consumer that has one, and a span saying so is pure volume.
		return batch
	}
	first, last := messages[0], messages[len(messages)-1]
	batch.ctx, batch.span = c.StartMessagingSpan(ctx, chronos.MessagingSpan{
		System:        System,
		Operation:     chronos.OperationProcess,
		Destination:   first.Topic,
		ConsumerGroup: options.Group,
		// Partition is the claim's, shared by every message in a batch that came
		// from one claim. Offset is deliberately absent: the batch spans a range,
		// and writing one of its ends into the singular attribute would read as the
		// batch having been one message at that offset.
		Partition: first.Partition,
		Offset:    -1,
		BodySize:  batchBytes(messages),
	})
	// A distinct name from the per-message `PROCESS <topic>`. The two have wildly
	// different durations and are counted separately - a batch span per flush, a
	// message span per message - so grouping them under one operation name would
	// produce an average that describes neither.
	batch.span.Name = "PROCESS " + first.Topic + " BATCH"
	batch.span.SetAttribute("messaging.batch.message_count", itoa(int64(len(messages))))
	batch.span.SetAttribute("messaging.kafka.offset.first", itoa(first.Offset))
	batch.span.SetAttribute("messaging.kafka.offset.last", itoa(last.Offset))
	return batch
}

// run is ProcessBatch's body: fn under the batch span, with the spans closed on
// every exit including a panic.
func (b *Batch) run(fn func(*Batch) error) (err error) {
	// Under MessageSpansAlways every message is given its span before the handler
	// runs, so a message the handler never asks about still has one. Under the
	// default the mint happens in ContextFor instead, and a message nobody asks
	// about costs nothing.
	if b.options.MessageSpans == MessageSpansAlways {
		for _, message := range b.messages {
			if message != nil {
				b.start(message)
			}
		}
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			b.span.SetStatus("error")
			b.span.SetAttribute("error.message", fmt.Sprintf("panic: %v", recovered))
			b.end()
			// Re-raised, not swallowed: a handler that panics must panic exactly as
			// it would uninstrumented.
			panic(recovered)
		}
		b.end()
	}()
	err = fn(b)
	if err != nil {
		b.span.SetStatus("error")
		b.span.SetAttribute("error.message", err.Error())
	}
	return err
}

func batchBytes(messages []*sarama.ConsumerMessage) int {
	total := 0
	for _, message := range messages {
		if message != nil {
			total += len(message.Value)
		}
	}
	return total
}

// --- consumer group handler ---------------------------------------------------

// BatchConsumerGroupHandler is a consumer group handler that works in batches.
//
// Setup and Cleanup are sarama's own. ConsumeBatch replaces ConsumeClaim: the
// accumulation - fill until full, flush on age, flush on shutdown - is done for you,
// and what arrives is one flush with its span already open.
type BatchConsumerGroupHandler interface {
	Setup(sarama.ConsumerGroupSession) error
	Cleanup(sarama.ConsumerGroupSession) error
	ConsumeBatch(*Batch) error
}

// WrapBatchConsumerGroupHandler adapts a batch handler into the
// sarama.ConsumerGroupHandler a consumer group wants.
//
// Offsets are NOT marked here. Committing is a durability decision - at-least-once
// after the flush, at-most-once before it, and which one is the application's to
// make - so the handler marks what it wants through Batch.Session().
func WrapBatchConsumerGroupHandler(
	handler BatchConsumerGroupHandler,
	c *chronos.Client,
	options BatchOptions,
) sarama.ConsumerGroupHandler {
	return &batchConsumerGroupHandler{handler: handler, client: client(c), options: options}
}

type batchConsumerGroupHandler struct {
	handler BatchConsumerGroupHandler
	client  *chronos.Client
	options BatchOptions
}

func (h *batchConsumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	return h.handler.Setup(session)
}

func (h *batchConsumerGroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	return h.handler.Cleanup(session)
}

// ConsumeClaim accumulates and flushes.
//
// The claim's message channel is read directly rather than through the per-message
// proxy: a span opened as a message is appended to a slice is the zero-duration span
// this whole file exists to replace.
func (h *batchConsumerGroupHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	var (
		batch []*sarama.ConsumerMessage
		bytes int
	)
	wait := h.options.maxWait()
	timer := time.NewTimer(wait)
	defer timer.Stop()

	// trigger says why the batch closed, which is the difference between "this
	// consumer is saturated" and "this partition is idle" when you are reading the
	// spans later.
	flush := func(trigger string) error {
		if len(batch) == 0 {
			return nil
		}
		flushing := batch
		batch, bytes = nil, 0
		return h.flush(session, flushing, trigger)
	}
	// Draining before Reset: a timer that already fired leaves a value in the
	// channel, and resetting without taking it makes the next flush fire at once.
	reset := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(wait)
	}

	messages := claim.Messages()
	for {
		select {
		case message := <-messages:
			if message == nil {
				return flush("channel-closed")
			}
			batch = append(batch, message)
			bytes += len(message.Value)
			if h.options.full(len(batch), bytes) {
				if err := flush("full"); err != nil {
					return err
				}
				reset()
			}
		case <-timer.C:
			if err := flush("max-wait"); err != nil {
				return err
			}
			timer.Reset(wait)
		case <-session.Context().Done():
			// The session context is already cancelled, so the flush runs on
			// borrowed time; it happens anyway, because the alternative is dropping
			// messages the group is about to have someone else replay.
			return flush("shutdown")
		}
	}
}

func (h *batchConsumerGroupHandler) flush(
	session sarama.ConsumerGroupSession,
	messages []*sarama.ConsumerMessage,
	trigger string,
) error {
	batch := newBatch(session.Context(), h.client, h.options, session, messages)
	batch.SetAttribute("messaging.batch.trigger", trigger)
	return batch.run(h.handler.ConsumeBatch)
}
