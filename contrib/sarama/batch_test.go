package sarama

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
)

// What the batch API promises, stated as the two production symptoms it exists to
// remove:
//
//  1. The flush is timed. The old per-message span closed before the work started
//     and reported a duration of zero; the batch span has to cover the real work.
//  2. A message keeps its own context through the flush. A publish caused by
//     message N must be a child of N's span, in N's upstream trace - not of the
//     batch, not of the session. That is the break that turned one pipeline into a
//     scatter of two-span traces, so it is asserted directly.

// batchMessage is one consumed record carrying a producer's traceparent, which is
// what makes "did this land in the right trace" answerable.
func batchMessage(topic string, offset int64, traceID, parentSpanID string) *sarama.ConsumerMessage {
	message := &sarama.ConsumerMessage{
		Topic:     topic,
		Partition: 3,
		Offset:    offset,
		Value:     []byte(fmt.Sprintf(`{"offset":%d}`, offset)),
	}
	if traceID != "" {
		message.Headers = []*sarama.RecordHeader{{
			Key:   []byte("traceparent"),
			Value: []byte("00-" + traceID + "-" + parentSpanID + "-01"),
		}}
	}
	return message
}

// spanWithAttribute finds the one span carrying key=value.
func spanWithAttribute(t *testing.T, spans []spanRecord, key, value string) spanRecord {
	t.Helper()
	var found []spanRecord
	for _, span := range spans {
		if span.Attributes[key] == value {
			found = append(found, span)
		}
	}
	if len(found) != 1 {
		t.Fatalf("want exactly one span with %s=%s, got %d of %d spans", key, value, len(found), len(spans))
	}
	return found[0]
}

func spanDuration(t *testing.T, span spanRecord) time.Duration {
	t.Helper()
	started, err := time.Parse(time.RFC3339Nano, span.StartedAt)
	if err != nil {
		t.Fatalf("unparseable startedAt %q: %v", span.StartedAt, err)
	}
	ended, err := time.Parse(time.RFC3339Nano, span.EndedAt)
	if err != nil {
		t.Fatalf("unparseable endedAt %q: %v", span.EndedAt, err)
	}
	return ended.Sub(started)
}

// The symptom that started this: a PROCESS span of zero duration, because the work
// happens in a flush the span had already closed before. The batch span has to
// cover the flush.
func TestTheBatchSpanTimesTheRealWorkRatherThanZero(t *testing.T) {
	client, dir := spooling(t)
	messages := []*sarama.ConsumerMessage{
		batchMessage("asn-items", 10, "", ""),
		batchMessage("asn-items", 11, "", ""),
	}

	const work = 40 * time.Millisecond
	err := ProcessBatch(context.Background(), client, BatchOptions{Group: "hydrator"}, messages,
		func(batch *Batch) error {
			time.Sleep(work)
			return nil
		})
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	client.FlushSpans()

	spans := spooledSpans(t, dir)
	batchSpan := spanNamed(t, spans, "PROCESS asn-items BATCH")
	if took := spanDuration(t, batchSpan); took < work {
		t.Errorf("batch span lasted %s, want at least the %s the handler spent", took, work)
	}
	for key, want := range map[string]string{
		"span.kind":                     "consumer",
		"messaging.system":              "kafka",
		"messaging.operation":           "process",
		"messaging.destination.name":    "asn-items",
		"messaging.consumer.group.name": "hydrator",
		"messaging.batch.message_count": "2",
		"messaging.kafka.offset.first":  "10",
		"messaging.kafka.offset.last":   "11",
	} {
		if got := batchSpan.Attributes[key]; got != want {
			t.Errorf("batch span %s = %q, want %q", key, got, want)
		}
	}
}

// Each message gets a span that names the message it is: its own offset, its own
// partition, and the upstream trace its producer minted.
func TestEachMessageGetsASpanAttributableToItsOwnMessage(t *testing.T) {
	client, dir := spooling(t)
	traceOne := "11111111111111111111111111111111"
	traceTwo := "22222222222222222222222222222222"
	messages := []*sarama.ConsumerMessage{
		batchMessage("asn-items", 10, traceOne, "aaaaaaaaaaaaaaaa"),
		batchMessage("asn-items", 11, traceTwo, "bbbbbbbbbbbbbbbb"),
	}

	err := ProcessBatch(context.Background(), client, BatchOptions{Group: "hydrator"}, messages,
		func(batch *Batch) error {
			return batch.Each(func(ctx context.Context, message *sarama.ConsumerMessage) error {
				return nil
			})
		})
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	client.FlushSpans()

	spans := spooledSpans(t, dir)
	batchSpan := spanNamed(t, spans, "PROCESS asn-items BATCH")
	if got := batchSpan.Attributes["messaging.batch.traced_message_count"]; got != "2" {
		t.Errorf("batch span traced %s messages, want 2", got)
	}

	for index, expected := range []struct {
		offset  string
		traceID string
		parent  string
	}{
		{"10", traceOne, "aaaaaaaaaaaaaaaa"},
		{"11", traceTwo, "bbbbbbbbbbbbbbbb"},
	} {
		span := spanWithAttribute(t, spans, "messaging.kafka.offset", expected.offset)
		if span.Name != "PROCESS asn-items" {
			t.Errorf("offset %s span is named %q", expected.offset, span.Name)
		}
		if span.Attributes["messaging.kafka.partition"] != "3" {
			t.Errorf("offset %s span has partition %q", expected.offset, span.Attributes["messaging.kafka.partition"])
		}
		if span.Attributes["messaging.consumer.group.name"] != "hydrator" {
			t.Errorf("offset %s span lost the consumer group", expected.offset)
		}
		if span.Attributes["span.kind"] != "consumer" {
			t.Errorf("offset %s span kind is %q, want consumer", expected.offset, span.Attributes["span.kind"])
		}
		if span.Attributes["messaging.batch.index"] != fmt.Sprint(index) {
			t.Errorf("offset %s span has batch index %q, want %d", expected.offset, span.Attributes["messaging.batch.index"], index)
		}
		// The message span lives in the PRODUCER's trace, parented onto the publish.
		if span.TraceID != expected.traceID {
			t.Errorf("offset %s span is in trace %s, want the producer's %s", expected.offset, span.TraceID, expected.traceID)
		}
		if span.ParentSpanID != expected.parent {
			t.Errorf("offset %s span is parented onto %s, want the publish %s", expected.offset, span.ParentSpanID, expected.parent)
		}
		// And it points back at the batch that handled it, since the span model has
		// no links to say so properly.
		if span.Attributes["messaging.batch.span_id"] != batchSpan.SpanID {
			t.Errorf("offset %s span does not reference the batch span", expected.offset)
		}
		if span.Attributes["messaging.batch.trace_id"] != batchSpan.TraceID {
			t.Errorf("offset %s span does not reference the batch trace", expected.offset)
		}
	}

	// The batch span is a root in this service's own trace. It has 500 causes on a
	// real flush and adopting one of them would be a lie.
	if batchSpan.ParentSpanID != "" {
		t.Errorf("batch span is parented onto %s; it should be a root", batchSpan.ParentSpanID)
	}
	if batchSpan.TraceID == traceOne || batchSpan.TraceID == traceTwo {
		t.Errorf("batch span was adopted into a message's trace (%s)", batchSpan.TraceID)
	}
}

// THE regression. A publish made while handling message N must be a child of N's
// span, so `source -> intermediary-topic -> sink` is one trace. Before the batch
// API the flush published with session.Context() and every hop was a new root.
func TestAPublishWhileHandlingAMessageParentsOntoThatMessage(t *testing.T) {
	client, dir := spooling(t)
	traceOne := "11111111111111111111111111111111"
	traceTwo := "22222222222222222222222222222222"
	messages := []*sarama.ConsumerMessage{
		batchMessage("asn-items", 10, traceOne, "aaaaaaaaaaaaaaaa"),
		batchMessage("asn-items", 11, traceTwo, "bbbbbbbbbbbbbbbb"),
	}

	producer := mocks.NewSyncProducer(t, nil)
	producer.ExpectSendMessageAndSucceed()
	producer.ExpectSendMessageAndSucceed()
	traced := WrapSyncProducer(producer, client, []string{"broker-1:9094"})

	err := ProcessBatch(context.Background(), client, BatchOptions{Group: "hydrator"}, messages,
		func(batch *Batch) error {
			// The shape a retrofitted flush loop has: the loop it already had, with
			// the context it passes down changed.
			for _, message := range batch.Messages() {
				ctx := batch.ContextFor(message)
				_, _, err := traced.SendMessageContext(ctx, &sarama.ProducerMessage{
					Topic: "asn-items-sink",
					Key:   sarama.StringEncoder(fmt.Sprint(message.Offset)),
					Value: sarama.StringEncoder(`{}`),
				})
				if err != nil {
					return err
				}
			}
			return nil
		})
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	client.FlushSpans()

	spans := spooledSpans(t, dir)
	batchSpan := spanNamed(t, spans, "PROCESS asn-items BATCH")

	for _, expected := range []struct {
		offset  string
		traceID string
	}{{"10", traceOne}, {"11", traceTwo}} {
		consume := spanWithAttribute(t, spans, "messaging.kafka.offset", expected.offset)
		publish := spanWithAttribute(t, spans, "messaging.message.id", expected.offset)
		if publish.Name != "PUBLISH asn-items-sink" {
			t.Fatalf("expected a publish span for offset %s, got %q", expected.offset, publish.Name)
		}
		if publish.ParentSpanID != consume.SpanID {
			t.Errorf("publish for offset %s is parented onto %s, want the message span %s",
				expected.offset, publish.ParentSpanID, consume.SpanID)
		}
		if publish.ParentSpanID == batchSpan.SpanID {
			t.Errorf("publish for offset %s is parented onto the batch, which is the bug", expected.offset)
		}
		// One trace from the original producer, through this consumer, to the next
		// topic: the pipeline the desktop was showing as disconnected halves.
		if publish.TraceID != expected.traceID {
			t.Errorf("publish for offset %s is in trace %s, want the pipeline's %s",
				expected.offset, publish.TraceID, expected.traceID)
		}
		if publish.Attributes["span.kind"] != "producer" {
			t.Errorf("publish for offset %s has kind %q, want producer", expected.offset, publish.Attributes["span.kind"])
		}
	}
}

// Without a per-message context there is nothing to mint, and nothing is minted: the
// on-demand default is what keeps a bulk-insert consumer at one span per flush
// instead of one per record.
func TestAHandlerThatAsksForNoMessageContextPaysOneSpan(t *testing.T) {
	client, dir := spooling(t)
	messages := []*sarama.ConsumerMessage{
		batchMessage("asn-items", 10, "", ""),
		batchMessage("asn-items", 11, "", ""),
		batchMessage("asn-items", 12, "", ""),
	}

	if err := ProcessBatch(context.Background(), client, BatchOptions{Group: "hydrator"}, messages,
		func(batch *Batch) error { return nil }); err != nil {
		t.Fatalf("batch: %v", err)
	}
	client.FlushSpans()

	spans := spooledSpans(t, dir)
	if len(spans) != 1 {
		t.Fatalf("a batch nobody asked about wrote %d spans, want 1", len(spans))
	}
	if spans[0].Attributes["messaging.batch.traced_message_count"] != "0" {
		t.Errorf("traced count is %q, want 0", spans[0].Attributes["messaging.batch.traced_message_count"])
	}
}

// MessageSpansAlways is the opt-in that pays batch-size spans per flush.
func TestMessageSpansAlwaysMintsOnePerMessageUnasked(t *testing.T) {
	client, dir := spooling(t)
	messages := []*sarama.ConsumerMessage{
		batchMessage("asn-items", 10, "", ""),
		batchMessage("asn-items", 11, "", ""),
	}

	options := BatchOptions{Group: "hydrator", MessageSpans: MessageSpansAlways}
	if err := ProcessBatch(context.Background(), client, options, messages,
		func(batch *Batch) error { return nil }); err != nil {
		t.Fatalf("batch: %v", err)
	}
	client.FlushSpans()

	spans := spooledSpans(t, dir)
	if len(spans) != 3 {
		t.Fatalf("wrote %d spans, want a batch span and two message spans", len(spans))
	}
}

// MessageSpansNever keeps the flush timed and gives up the causal thread, which has
// to mean the handler's publishes land on the batch rather than nowhere.
func TestMessageSpansNeverFallsBackToTheBatchContext(t *testing.T) {
	client, dir := spooling(t)
	messages := []*sarama.ConsumerMessage{batchMessage("asn-items", 10, "", "")}

	options := BatchOptions{Group: "hydrator", MessageSpans: MessageSpansNever}
	if err := ProcessBatch(context.Background(), client, options, messages,
		func(batch *Batch) error {
			ctx := batch.ContextFor(messages[0])
			_, span := client.StartSpan(ctx, "work")
			span.End()
			return nil
		}); err != nil {
		t.Fatalf("batch: %v", err)
	}
	client.FlushSpans()

	spans := spooledSpans(t, dir)
	if len(spans) != 2 {
		t.Fatalf("wrote %d spans, want the batch span and the handler's own", len(spans))
	}
	batchSpan := spanNamed(t, spans, "PROCESS asn-items BATCH")
	work := spanNamed(t, spans, "work")
	if work.ParentSpanID != batchSpan.SpanID {
		t.Errorf("work is parented onto %s, want the batch span %s", work.ParentSpanID, batchSpan.SpanID)
	}
}

// --- fail open ----------------------------------------------------------------

var errHandler = errors.New("handler said no")

func TestTheHandlerErrorIsPassedBackUntouched(t *testing.T) {
	client, dir := spooling(t)
	messages := []*sarama.ConsumerMessage{batchMessage("asn-items", 10, "", "")}

	err := ProcessBatch(context.Background(), client, BatchOptions{}, messages,
		func(batch *Batch) error { return errHandler })
	if !errors.Is(err, errHandler) {
		t.Fatalf("the wrapper changed the error: %v", err)
	}
	client.FlushSpans()

	// Recorded as errored, and still recorded: a failed flush is the one you most
	// want to see.
	if body := spooled(t, dir); !strings.Contains(body, `"status":"error"`) ||
		!strings.Contains(body, errHandler.Error()) {
		t.Errorf("the batch span did not record the failure\n%s", body)
	}
}

func TestAPanickingHandlerPanicsAsItWouldUninstrumented(t *testing.T) {
	client, dir := spooling(t)
	messages := []*sarama.ConsumerMessage{batchMessage("asn-items", 10, "", "")}

	func() {
		defer func() {
			recovered := recover()
			if recovered == nil {
				t.Fatal("the panic was swallowed")
			}
			if fmt.Sprint(recovered) != "boom" {
				t.Fatalf("the panic was changed to %v", recovered)
			}
		}()
		_ = ProcessBatch(context.Background(), client, BatchOptions{}, messages,
			func(batch *Batch) error { panic("boom") })
	}()
	client.FlushSpans()

	// The span still closed on the way out, or a panicking consumer would leak one
	// span per flush and report none of them.
	if body := spooled(t, dir); !strings.Contains(body, "PROCESS asn-items BATCH") ||
		!strings.Contains(body, "panic: boom") {
		t.Errorf("the batch span was not closed on the panic path\n%s", body)
	}
}

func TestANilClientIsAPassThrough(t *testing.T) {
	messages := []*sarama.ConsumerMessage{batchMessage("asn-items", 10, "", "")}
	seen := 0

	err := ProcessBatch(context.Background(), nil, BatchOptions{}, messages, func(batch *Batch) error {
		return batch.Each(func(ctx context.Context, message *sarama.ConsumerMessage) error {
			if ctx == nil {
				t.Error("a message context must be usable even with no client behind it")
			}
			seen++
			return nil
		})
	})
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	if seen != len(messages) {
		t.Fatalf("the handler saw %d of %d messages", seen, len(messages))
	}
	if err := ProcessBatch(context.Background(), nil, BatchOptions{}, messages,
		func(batch *Batch) error { return errHandler }); !errors.Is(err, errHandler) {
		t.Fatalf("the error was changed with no client: %v", err)
	}
}

func TestAnEmptyBatchRecordsNothingAndStillRuns(t *testing.T) {
	client, dir := spooling(t)
	ran := false
	if err := ProcessBatch(context.Background(), client, BatchOptions{}, nil,
		func(batch *Batch) error {
			ran = true
			if batch.Len() != 0 {
				t.Errorf("empty batch reports %d messages", batch.Len())
			}
			return nil
		}); err != nil {
		t.Fatalf("batch: %v", err)
	}
	client.FlushSpans()

	if !ran {
		t.Fatal("the handler did not run")
	}
	if spans := spooledSpans(t, dir); len(spans) != 0 {
		t.Errorf("an empty flush wrote %d spans", len(spans))
	}
}

// --- the consumer group handler -------------------------------------------------

// batchRecorder is guarded because the age test watches it from the test goroutine
// while ConsumeClaim runs on another.
type batchRecorder struct {
	mu      sync.Mutex
	batches [][]*sarama.ConsumerMessage
	flushed chan struct{}
	err     error
}

func (r *batchRecorder) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (r *batchRecorder) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (r *batchRecorder) ConsumeBatch(batch *Batch) error {
	r.mu.Lock()
	r.batches = append(r.batches, batch.Messages())
	r.mu.Unlock()
	if r.flushed != nil {
		select {
		case r.flushed <- struct{}{}:
		default:
		}
	}
	return batch.Each(func(ctx context.Context, message *sarama.ConsumerMessage) error {
		return r.err
	})
}

func (r *batchRecorder) sizes() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	sizes := []int{}
	for _, batch := range r.batches {
		sizes = append(sizes, len(batch))
	}
	return sizes
}

func TestTheWrappedHandlerFlushesOnSizeAndOnChannelClose(t *testing.T) {
	client, dir := spooling(t)
	claim := &stubClaim{messages: make(chan *sarama.ConsumerMessage, 5)}
	for offset := int64(0); offset < 5; offset++ {
		claim.messages <- batchMessage("movements", offset, "", "")
	}
	close(claim.messages)

	recorder := &batchRecorder{}
	handler := WrapBatchConsumerGroupHandler(recorder, client, BatchOptions{
		Group:      "hydrator",
		MaxRecords: 2,
		MaxWait:    time.Hour,
	})
	if err := handler.ConsumeClaim(&stubSession{ctx: context.Background()}, claim); err != nil {
		t.Fatalf("consume: %v", err)
	}
	client.FlushSpans()

	// 2 + 2 on size, then the remaining 1 when the channel closed.
	sizes := recorder.sizes()
	if fmt.Sprint(sizes) != "[2 2 1]" {
		t.Fatalf("batch sizes were %v, want [2 2 1]", sizes)
	}

	spans := spooledSpans(t, dir)
	batches := 0
	for _, span := range spans {
		if span.Name == "PROCESS movements BATCH" {
			batches++
			if span.Attributes["messaging.batch.trigger"] == "" {
				t.Error("a batch span did not record why it flushed")
			}
		}
	}
	if batches != 3 {
		t.Fatalf("wrote %d batch spans, want 3", batches)
	}
}

func TestTheWrappedHandlerFlushesOnAge(t *testing.T) {
	client, _ := spooling(t)
	claim := &stubClaim{messages: make(chan *sarama.ConsumerMessage, 1)}
	claim.messages <- batchMessage("movements", 7, "", "")

	recorder := &batchRecorder{flushed: make(chan struct{}, 1)}
	handler := WrapBatchConsumerGroupHandler(recorder, client, BatchOptions{
		Group: "hydrator",
		// No record limit, so only the clock can close this batch.
		MaxWait: 20 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- handler.ConsumeClaim(&stubSession{ctx: ctx}, claim) }()

	select {
	case <-recorder.flushed:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("the age trigger never fired")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("consume: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ConsumeClaim did not return after shutdown")
	}
}

func TestTheWrappedHandlerPassesTheHandlerErrorOut(t *testing.T) {
	client, _ := spooling(t)
	claim := &stubClaim{messages: make(chan *sarama.ConsumerMessage, 1)}
	claim.messages <- batchMessage("movements", 1, "", "")
	close(claim.messages)

	recorder := &batchRecorder{err: errHandler}
	handler := WrapBatchConsumerGroupHandler(recorder, client, BatchOptions{Group: "hydrator"})
	if err := handler.ConsumeClaim(&stubSession{ctx: context.Background()}, claim); !errors.Is(err, errHandler) {
		t.Fatalf("the wrapper changed the error: %v", err)
	}
}

// The per-message wrapper is in production and must not have moved: a batch API next
// to it is an addition, not a replacement.
func TestThePerMessageWrapperIsUnchanged(t *testing.T) {
	client, dir := spooling(t)
	traceID := "33333333333333333333333333333333"
	claim := &stubClaim{messages: make(chan *sarama.ConsumerMessage, 1)}
	claim.messages <- batchMessage("movements", 42, traceID, "cccccccccccccccc")
	close(claim.messages)

	inner := &recordingHandler{}
	handler := WrapConsumerGroupHandler(inner, client, "vitess-group")
	if err := handler.ConsumeClaim(&stubSession{ctx: context.Background()}, claim); err != nil {
		t.Fatalf("consume: %v", err)
	}
	client.FlushSpans()

	spans := spooledSpans(t, dir)
	if len(spans) != 1 {
		t.Fatalf("the per-message wrapper wrote %d spans, want 1", len(spans))
	}
	span := spans[0]
	if span.Name != "PROCESS movements" {
		t.Errorf("span is named %q", span.Name)
	}
	if span.TraceID != traceID || span.ParentSpanID != "cccccccccccccccc" {
		t.Errorf("span landed at %s/%s, want the publish's %s/cccccccccccccccc",
			span.TraceID, span.ParentSpanID, traceID)
	}
	for key, want := range map[string]string{
		"span.kind":                     "consumer",
		"messaging.system":              "kafka",
		"messaging.operation":           "process",
		"messaging.destination.name":    "movements",
		"messaging.consumer.group.name": "vitess-group",
		"messaging.kafka.partition":     "3",
		"messaging.kafka.offset":        "42",
	} {
		if got := span.Attributes[key]; got != want {
			t.Errorf("%s = %q, want %q", key, got, want)
		}
	}
	// And it carries no batch bookkeeping, because it is not part of a batch.
	for _, key := range []string{"messaging.batch.span_id", "messaging.batch.index"} {
		if _, present := span.Attributes[key]; present {
			t.Errorf("the per-message span grew a %s attribute", key)
		}
	}
}

// A guard on the one thing a chronos.Span exposes that this file writes to directly.
func TestTheBatchSpanNameIsDistinctFromTheMessageSpanName(t *testing.T) {
	client, dir := spooling(t)
	messages := []*sarama.ConsumerMessage{batchMessage("asn-items", 10, "", "")}
	options := BatchOptions{Group: "hydrator", MessageSpans: MessageSpansAlways}
	if err := ProcessBatch(context.Background(), client, options, messages,
		func(batch *Batch) error { return nil }); err != nil {
		t.Fatalf("batch: %v", err)
	}
	client.FlushSpans()

	names := map[string]int{}
	for _, span := range spooledSpans(t, dir) {
		names[span.Name]++
	}
	if names["PROCESS asn-items"] != 1 || names["PROCESS asn-items BATCH"] != 1 {
		t.Fatalf("span names were %v", names)
	}
}
