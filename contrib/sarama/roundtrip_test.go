package sarama

import (
	"context"
	"encoding/json"
	"regexp"
	"strings"
	"testing"

	chronos "chronos.dev/collector/sdk/go"
	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
)

// The whole point of the traceparent header, proven end to end rather than in
// halves: the existing tests check that a publish WRITES a header and that a
// consume given a hand-written header lands in the right trace, which leaves
// the join itself — the produced bytes being the consumed bytes — untested. A
// mismatch in header name casing, value format or span-id choice would pass
// both halves and still produce two disconnected traces in the desktop.

// spanRecord is the subset of the spooled span document these assertions read.
type spanRecord struct {
	TraceID      string            `json:"traceId"`
	SpanID       string            `json:"spanId"`
	ParentSpanID string            `json:"parentSpanId"`
	Name         string            `json:"name"`
	Attributes   map[string]string `json:"attributes"`
}

// spooledSpans decodes every span the client wrote, in write order.
func spooledSpans(t *testing.T, dir string) []spanRecord {
	t.Helper()
	var out []spanRecord
	decoder := json.NewDecoder(stringReader(spooled(t, dir)))
	for {
		var batch struct {
			Spans []spanRecord `json:"spans"`
		}
		if err := decoder.Decode(&batch); err != nil {
			break
		}
		out = append(out, batch.Spans...)
	}
	return out
}

func spanNamed(t *testing.T, spans []spanRecord, name string) spanRecord {
	t.Helper()
	for _, span := range spans {
		if span.Name == name {
			return span
		}
	}
	t.Fatalf("no span named %q among %d spans", name, len(spans))
	return spanRecord{}
}

func TestProducedHeaderParentsTheConsumeSpanOntoThePublishSpan(t *testing.T) {
	client, dir := spooling(t)
	producer := mocks.NewSyncProducer(t, nil)
	producer.ExpectSendMessageAndSucceed()

	ctx, root := client.StartSpan(context.Background(), "job")

	message := &sarama.ProducerMessage{
		Topic: "movements",
		Key:   sarama.StringEncoder("label-7"),
		Value: sarama.StringEncoder(`{"id":1}`),
	}
	traced := WrapSyncProducer(producer, client, []string{"broker-1:9094"})
	if _, _, err := traced.SendMessageContext(ctx, message); err != nil {
		t.Fatalf("send: %v", err)
	}
	root.End()

	// The consumer sees exactly the bytes the producer put on the wire: the
	// headers are copied across verbatim rather than re-stated, so a change to
	// either side's spelling breaks this test rather than production.
	claim := &stubClaim{messages: make(chan *sarama.ConsumerMessage, 1)}
	claim.messages <- &sarama.ConsumerMessage{
		Topic:     "movements",
		Partition: 3,
		Offset:    42,
		Key:       []byte("label-7"),
		Value:     []byte(`{"id":1}`),
		Headers:   carried(message.Headers),
	}
	close(claim.messages)

	handler := WrapConsumerGroupHandler(&recordingHandler{}, client, "vitess-group")
	if err := handler.ConsumeClaim(&stubSession{ctx: context.Background()}, claim); err != nil {
		t.Fatalf("consume: %v", err)
	}
	client.FlushSpans()

	spans := spooledSpans(t, dir)
	publish := spanNamed(t, spans, "PUBLISH movements")
	consume := spanNamed(t, spans, "PROCESS movements")

	if consume.TraceID != publish.TraceID {
		t.Errorf("consume is in trace %s, publish in %s: the broker split the trace",
			consume.TraceID, publish.TraceID)
	}
	if consume.TraceID != root.TraceID {
		t.Errorf("the message left the trace that caused it: %s want %s", consume.TraceID, root.TraceID)
	}
	if consume.ParentSpanID != publish.SpanID {
		t.Errorf("consume parents onto %q, want the publish span %q",
			consume.ParentSpanID, publish.SpanID)
	}
	if publish.ParentSpanID != root.SpanID {
		t.Errorf("publish parents onto %q, want the caller's span %q",
			publish.ParentSpanID, root.SpanID)
	}
}

// phpTraceparent is TraceContext.php's own pattern, character for character. A
// Go-written header that this rejects is a header the PHP collector drops on
// the floor, and the consumer on the other side of the topic roots its own
// trace instead of joining ours.
var phpTraceparent = regexp.MustCompile(`^00-[a-f0-9]{32}-[a-f0-9]{16}-[a-f0-9]{2}$`)

func TestTheHeaderWeWriteIsOneThePHPCollectorAccepts(t *testing.T) {
	client, _ := spooling(t)
	producer := mocks.NewSyncProducer(t, nil)
	producer.ExpectSendMessageAndSucceed()

	ctx, root := client.StartSpan(context.Background(), "job")
	defer root.End()

	message := &sarama.ProducerMessage{Topic: "movements", Value: sarama.StringEncoder("{}")}
	traced := WrapSyncProducer(producer, client, nil)
	if _, _, err := traced.SendMessageContext(ctx, message); err != nil {
		t.Fatalf("send: %v", err)
	}

	var header sarama.RecordHeader
	for _, candidate := range message.Headers {
		if string(candidate.Key) == "traceparent" {
			header = candidate
		}
	}
	// PHP lower-cases inbound header names before reading, but it writes the
	// key in lower case, so an exact match here is what keeps the two wire
	// formats byte-identical rather than merely mutually parseable.
	if string(header.Key) != "traceparent" {
		t.Fatalf("header key is %q, PHP writes %q", header.Key, "traceparent")
	}
	if !phpTraceparent.Match(header.Value) {
		t.Errorf("PHP's TraceContext::fromHeader would reject %q", header.Value)
	}
}

// A PHP publisher's header, byte for byte as SpanReservation::header() renders
// it, is understood here: same version, same lower-case hex, same flags.
func TestAPHPWrittenHeaderParentsAGoConsumer(t *testing.T) {
	client, dir := spooling(t)
	php := "00-4f1a2b3c4d5e6f708192a3b4c5d6e7f8-0123456789abcdef-01"

	claim := &stubClaim{messages: make(chan *sarama.ConsumerMessage, 1)}
	claim.messages <- &sarama.ConsumerMessage{
		Topic: "movements",
		Value: []byte(`{"id":1}`),
		// Upper-cased on purpose: PHP's own bridges forward whatever case the
		// application wrote, and record headers are raw bytes.
		Headers: []*sarama.RecordHeader{{Key: []byte("Traceparent"), Value: []byte(php)}},
	}
	close(claim.messages)

	handler := WrapConsumerGroupHandler(&recordingHandler{}, client, "vitess-group")
	if err := handler.ConsumeClaim(&stubSession{ctx: context.Background()}, claim); err != nil {
		t.Fatalf("consume: %v", err)
	}
	client.FlushSpans()

	consume := spanNamed(t, spooledSpans(t, dir), "PROCESS movements")
	if consume.TraceID != "4f1a2b3c4d5e6f708192a3b4c5d6e7f8" {
		t.Errorf("consume is in trace %s, want the PHP publisher's", consume.TraceID)
	}
	if consume.ParentSpanID != "0123456789abcdef" {
		t.Errorf("consume parents onto %q, want the PHP publish span", consume.ParentSpanID)
	}
}

// carried copies produced headers onto the consumer's shape, which is the same
// bytes under a different pointer-ness — sarama models the two directions with
// two types for no reason a trace cares about.
func carried(headers []sarama.RecordHeader) []*sarama.RecordHeader {
	out := make([]*sarama.RecordHeader, 0, len(headers))
	for i := range headers {
		out = append(out, &headers[i])
	}
	return out
}

func stringReader(s string) *strings.Reader { return strings.NewReader(s) }

// headerValue is one produced record header, case-insensitively, or "".
func headerValue(headers []sarama.RecordHeader, key string) string {
	for _, header := range headers {
		if strings.EqualFold(string(header.Key), key) {
			return string(header.Value)
		}
	}
	return ""
}

// The full W3C trio crosses the broker, not just the traceparent. A Go service
// sitting between two PHP services used to be where another vendor's tracestate
// and the application's baggage died: the consumer read them, the producer wrote
// neither, and the next hop saw context that had quietly lost two thirds of itself.
func TestTheW3CTrioSurvivesAGoConsumeAndRepublish(t *testing.T) {
	client, _ := spooling(t)
	producer := mocks.NewSyncProducer(t, nil)
	producer.ExpectSendMessageAndSucceed()

	// What a PHP publisher put on the wire, byte for byte.
	inbound := &sarama.ConsumerMessage{
		Topic: "movements",
		Value: []byte(`{"id":1}`),
		Headers: []*sarama.RecordHeader{
			{Key: []byte("traceparent"), Value: []byte("00-4f1a2b3c4d5e6f708192a3b4c5d6e7f8-0123456789abcdef-01")},
			{Key: []byte("tracestate"), Value: []byte("rojo=00f067aa0ba902b7,congo=t61rcWkgMzE")},
			{Key: []byte("baggage"), Value: []byte("userId=alice,tenant=acme")},
		},
	}
	remote, _ := extract(inbound)
	ctx := chronos.ContextWithRemoteContext(context.Background(), remote)

	outbound := &sarama.ProducerMessage{Topic: "labels", Value: sarama.StringEncoder("{}")}
	traced := WrapSyncProducer(producer, client, nil)
	if _, _, err := traced.SendMessageContext(ctx, outbound); err != nil {
		t.Fatalf("send: %v", err)
	}

	if got := headerValue(outbound.Headers, chronos.TracestateHeader); got != "rojo=00f067aa0ba902b7,congo=t61rcWkgMzE" {
		t.Errorf("tracestate forwarded as %q, want the inbound value unchanged", got)
	}
	if got := headerValue(outbound.Headers, chronos.BaggageHeader); got != "userId=alice,tenant=acme" {
		t.Errorf("baggage forwarded as %q, want the inbound value unchanged", got)
	}
	tp := headerValue(outbound.Headers, chronos.TraceparentHeader)
	if !strings.HasPrefix(tp, "00-4f1a2b3c4d5e6f708192a3b4c5d6e7f8-") {
		t.Errorf("republished under %q, want the inbound trace", tp)
	}
}

// The publisher's sampling decision is the publisher's. A -00 message consumed and
// republished by Go must still say -00 downstream, or the half of the trace PHP
// deliberately dropped comes back to life past the Go hop.
func TestAnUnsampledMessageStaysUnsampledAcrossTheBroker(t *testing.T) {
	client, _ := spooling(t)
	producer := mocks.NewSyncProducer(t, nil)
	producer.ExpectSendMessageAndSucceed()

	inbound := &sarama.ConsumerMessage{
		Topic: "movements",
		Headers: []*sarama.RecordHeader{{
			Key:   []byte("traceparent"),
			Value: []byte("00-4f1a2b3c4d5e6f708192a3b4c5d6e7f8-0123456789abcdef-00"),
		}},
	}
	remote, _ := extract(inbound)
	ctx := chronos.ContextWithRemoteContext(context.Background(), remote)

	outbound := &sarama.ProducerMessage{Topic: "labels", Value: sarama.StringEncoder("{}")}
	traced := WrapSyncProducer(producer, client, nil)
	if _, _, err := traced.SendMessageContext(ctx, outbound); err != nil {
		t.Fatalf("send: %v", err)
	}

	tp := headerValue(outbound.Headers, chronos.TraceparentHeader)
	if !strings.HasSuffix(tp, "-00") {
		t.Errorf("republished as %q, want the publisher's unsampled flag", tp)
	}
}

// A traceparent Go would once have believed and PHP never would. Accepting it
// spread an upper-case trace id through a chain whose other hops all wrote lower
// case: one logical trace, two ids, and no way to join them. Refusing it roots an
// honest new trace instead.
func TestAnUppercaseTraceparentIsRefusedRatherThanPropagated(t *testing.T) {
	client, dir := spooling(t)

	claim := &stubClaim{messages: make(chan *sarama.ConsumerMessage, 1)}
	claim.messages <- &sarama.ConsumerMessage{
		Topic: "movements",
		Headers: []*sarama.RecordHeader{{
			Key:   []byte("traceparent"),
			Value: []byte("00-4F1A2B3C4D5E6F708192A3B4C5D6E7F8-0123456789ABCDEF-01"),
		}},
	}
	close(claim.messages)

	handler := WrapConsumerGroupHandler(&recordingHandler{}, client, "vitess-group")
	if err := handler.ConsumeClaim(&stubSession{ctx: context.Background()}, claim); err != nil {
		t.Fatalf("consume: %v", err)
	}
	client.FlushSpans()

	consume := spanNamed(t, spooledSpans(t, dir), "PROCESS movements")
	if strings.EqualFold(consume.TraceID, "4f1a2b3c4d5e6f708192a3b4c5d6e7f8") {
		t.Errorf("adopted an upper-case trace id (%s); PHP would have rejected it", consume.TraceID)
	}
	if consume.ParentSpanID != "" {
		t.Errorf("parented onto %q out of a header PHP refuses", consume.ParentSpanID)
	}
	if !phpTraceparent.MatchString("00-" + consume.TraceID + "-" + consume.SpanID + "-01") {
		t.Errorf("the trace we started instead is itself unacceptable to PHP: %s", consume.TraceID)
	}
}

// Queue wait is a subtraction across two processes, so the producer has to stamp
// the instant and the consumer has to find it under the name PHP uses. Go wrote
// neither half before: a Go publish left queue wait unmeasurable for every PHP
// consumer of the topic, and a Go consumer threw away the stamp PHP had sent.
func TestTheEnqueuedStampCrossesTheBrokerAndBecomesQueueTime(t *testing.T) {
	client, dir := spooling(t)
	producer := mocks.NewSyncProducer(t, nil)
	producer.ExpectSendMessageAndSucceed()

	ctx, root := client.StartSpan(context.Background(), "job")
	message := &sarama.ProducerMessage{Topic: "movements", Value: sarama.StringEncoder("{}")}
	traced := WrapSyncProducer(producer, client, nil)
	if _, _, err := traced.SendMessageContext(ctx, message); err != nil {
		t.Fatalf("send: %v", err)
	}
	root.End()

	stamp := headerValue(message.Headers, chronos.EnqueuedAtHeader)
	if stamp == "" {
		t.Fatal("no x-chronos-enqueued-at on the wire; PHP's MessagingWait has nothing to subtract")
	}
	if !phpMicrotime.MatchString(stamp) {
		t.Errorf("stamp %q is not sprintf('%%.6F', microtime(true))", stamp)
	}

	claim := &stubClaim{messages: make(chan *sarama.ConsumerMessage, 1)}
	claim.messages <- &sarama.ConsumerMessage{
		Topic:   "movements",
		Headers: carried(message.Headers),
	}
	close(claim.messages)

	handler := WrapConsumerGroupHandler(&recordingHandler{}, client, "vitess-group")
	if err := handler.ConsumeClaim(&stubSession{ctx: context.Background()}, claim); err != nil {
		t.Fatalf("consume: %v", err)
	}
	client.FlushSpans()

	consume := spanNamed(t, spooledSpans(t, dir), "PROCESS movements")
	if _, measured := consume.Attributes["messaging.message.queue_time_ms"]; !measured {
		t.Error("the consume span reports no queue time from a stamp it was handed")
	}
}

// The stamp's format is PHP's, not Go's default float rendering.
var phpMicrotime = regexp.MustCompile(`^[0-9]+\.[0-9]{6}$`)

// A stamp from a publisher this SDK never met - or none at all - must not produce a
// measurement. Zero here would read as "picked up instantly", the healthiest
// possible answer, from a queue nobody measured.
func TestAnAbsentOrUnusableStampMeasuresNothing(t *testing.T) {
	client, dir := spooling(t)

	claim := &stubClaim{messages: make(chan *sarama.ConsumerMessage, 2)}
	claim.messages <- &sarama.ConsumerMessage{Topic: "movements"}
	claim.messages <- &sarama.ConsumerMessage{
		Topic:   "labels",
		Headers: []*sarama.RecordHeader{{Key: []byte("x-chronos-enqueued-at"), Value: []byte("yesterday")}},
	}
	close(claim.messages)

	handler := WrapConsumerGroupHandler(&recordingHandler{}, client, "vitess-group")
	if err := handler.ConsumeClaim(&stubSession{ctx: context.Background()}, claim); err != nil {
		t.Fatalf("consume: %v", err)
	}
	client.FlushSpans()

	for _, name := range []string{"PROCESS movements", "PROCESS labels"} {
		span := spanNamed(t, spooledSpans(t, dir), name)
		if value, measured := span.Attributes["messaging.message.queue_time_ms"]; measured {
			t.Errorf("%s reports a queue time of %q it cannot know", name, value)
		}
	}
}
