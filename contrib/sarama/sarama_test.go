package sarama

import (
	"context"
	"strings"
	"testing"
	"time"

	chronos "chronos.dev/collector/sdk/go"
	"chronos.dev/collector/sdk/go/spool"
	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
)

// What the Kafka instrumentation promises, and none of it is "a span exists".
//
//  1. The attributes the desktop's producer join reads are on the span. A span
//     without messaging.destination.name cannot be attributed to a stream, so it
//     is telemetry that costs something and answers nothing.
//  2. The trace crosses the broker. A publish stamps a traceparent header and a
//     consume reads it back, or the work a message causes is an orphan trace.
//  3. Nothing here can break a publish. The wrappers are transparent.

// spooling client writes to a temp directory so the spans can be read back.
func spooling(t *testing.T) (*chronos.Client, string) {
	t.Helper()
	dir := t.TempDir()
	client := chronos.StartWithConfig(chronos.Config{
		Enabled:      true,
		APMEnabled:   true,
		Organisation: "org-test",
		Project:      "project-test",
		Application:  "vitess-test",
		SpoolDir:     dir,
		ServiceName:  "service-vitess",
	})
	t.Cleanup(func() { client.Shutdown(context.Background()) })
	return client, dir
}

// spooled returns every trace document written, as raw JSON. Documents are
// frames in a shared segment since ADR 0035, not files named by signal.
func spooled(t *testing.T, dir string) string {
	t.Helper()
	frames, err := spool.Read(dir)
	if err != nil {
		t.Fatalf("read spool: %v", err)
	}
	var all strings.Builder
	for _, frame := range frames {
		if frame.Signal == string(spool.SignalTrace) {
			all.Write(frame.Payload)
		}
	}
	return all.String()
}

func TestPublishSpanCarriesTheJoinAttributes(t *testing.T) {
	client, dir := spooling(t)
	producer := mocks.NewSyncProducer(t, nil)
	producer.ExpectSendMessageAndSucceed()

	traced := WrapSyncProducer(producer, client, []string{"broker-1:9094"})
	if _, _, err := traced.SendMessage(&sarama.ProducerMessage{
		Topic: "movements",
		Value: sarama.StringEncoder(`{"id":1}`),
	}); err != nil {
		t.Fatalf("send: %v", err)
	}
	client.FlushSpans()

	body := spooled(t, dir)
	for _, want := range []string{
		`"messaging.system":"kafka"`,
		`"messaging.operation":"publish"`,
		`"messaging.destination.name":"movements"`,
		`"server.address":"broker-1:9094"`,
		`"PUBLISH movements"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("span is missing %s\n%s", want, body)
		}
	}
}

func TestPublishStampsTheTraceOntoTheMessage(t *testing.T) {
	client, _ := spooling(t)
	producer := mocks.NewSyncProducer(t, nil)
	producer.ExpectSendMessageAndSucceed()

	// A parent span, so there is a trace to propagate.
	ctx, parent := client.StartSpan(context.Background(), "job")
	defer parent.End()

	message := &sarama.ProducerMessage{Topic: "movements", Value: sarama.StringEncoder("{}")}
	traced := WrapSyncProducer(producer, client, nil)
	if _, _, err := traced.SendMessageContext(ctx, message); err != nil {
		t.Fatalf("send: %v", err)
	}

	var header string
	for _, h := range message.Headers {
		if strings.EqualFold(string(h.Key), chronos.TraceparentHeader) {
			header = string(h.Value)
		}
	}
	if header == "" {
		t.Fatal("no traceparent header was written")
	}
	traceID, _, ok := chronos.ParseTraceparent(header)
	if !ok {
		t.Fatalf("unparseable traceparent %q", header)
	}
	if traceID != parent.TraceID {
		t.Errorf("traceparent carries trace %s, want the ambient %s", traceID, parent.TraceID)
	}
}

func TestPublishDoesNotOverwriteAnExistingTraceparent(t *testing.T) {
	client, _ := spooling(t)
	producer := mocks.NewSyncProducer(t, nil)
	producer.ExpectSendMessageAndSucceed()

	existing := "00-11111111111111111111111111111111-2222222222222222-01"
	message := &sarama.ProducerMessage{
		Topic:   "movements",
		Value:   sarama.StringEncoder("{}"),
		Headers: []sarama.RecordHeader{{Key: []byte("traceparent"), Value: []byte(existing)}},
	}
	ctx, parent := client.StartSpan(context.Background(), "job")
	defer parent.End()

	traced := WrapSyncProducer(producer, client, nil)
	if _, _, err := traced.SendMessageContext(ctx, message); err != nil {
		t.Fatalf("send: %v", err)
	}
	// Counting headers is no longer the assertion: a publish also stamps
	// x-chronos-enqueued-at. What must hold is that the caller's traceparent is
	// still there, still theirs, and still the only one.
	var traceparents []string
	for _, header := range message.Headers {
		if strings.EqualFold(string(header.Key), chronos.TraceparentHeader) {
			traceparents = append(traceparents, string(header.Value))
		}
	}
	if len(traceparents) != 1 {
		t.Fatalf("expected exactly one traceparent, got %d", len(traceparents))
	}
	if traceparents[0] != existing {
		t.Errorf("header was overwritten: %s", traceparents[0])
	}
}

func TestPublishPassesTheBrokerErrorThrough(t *testing.T) {
	client, _ := spooling(t)
	producer := mocks.NewSyncProducer(t, nil)
	producer.ExpectSendMessageAndFail(sarama.ErrOutOfBrokers)

	traced := WrapSyncProducer(producer, client, nil)
	_, _, err := traced.SendMessage(&sarama.ProducerMessage{
		Topic: "movements",
		Value: sarama.StringEncoder("{}"),
	})
	if err != sarama.ErrOutOfBrokers {
		t.Fatalf("wrapper changed the error: %v", err)
	}
}

// --- consumer ---------------------------------------------------------------

type recordingHandler struct {
	seen [][]byte
	// The trace the handler observed, captured from the span the wrapper opened.
	traceIDs []string
}

func (h *recordingHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *recordingHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *recordingHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	for message := range claim.Messages() {
		h.seen = append(h.seen, message.Value)
	}
	return nil
}

// stubClaim feeds a fixed set of messages, like a partition assignment would.
type stubClaim struct {
	messages chan *sarama.ConsumerMessage
}

func (c *stubClaim) Topic() string                            { return "movements" }
func (c *stubClaim) Partition() int32                         { return 3 }
func (c *stubClaim) InitialOffset() int64                     { return 0 }
func (c *stubClaim) HighWaterMarkOffset() int64               { return 0 }
func (c *stubClaim) Messages() <-chan *sarama.ConsumerMessage { return c.messages }

type stubSession struct{ ctx context.Context }

func (s *stubSession) Claims() map[string][]int32                  { return nil }
func (s *stubSession) MemberID() string                            { return "member" }
func (s *stubSession) GenerationID() int32                         { return 1 }
func (s *stubSession) MarkOffset(string, int32, int64, string)     {}
func (s *stubSession) Commit()                                     {}
func (s *stubSession) ResetOffset(string, int32, int64, string)    {}
func (s *stubSession) MarkMessage(*sarama.ConsumerMessage, string) {}
func (s *stubSession) Context() context.Context                    { return s.ctx }

func TestConsumeSpansTheHandlerAndJoinsThePublishTrace(t *testing.T) {
	client, dir := spooling(t)
	traceID := "11111111111111111111111111111111"
	claim := &stubClaim{messages: make(chan *sarama.ConsumerMessage, 2)}
	claim.messages <- &sarama.ConsumerMessage{
		Topic:     "movements",
		Partition: 3,
		Offset:    42,
		Value:     []byte(`{"id":1}`),
		Headers: []*sarama.RecordHeader{{
			Key:   []byte("traceparent"),
			Value: []byte("00-" + traceID + "-2222222222222222-01"),
		}},
	}
	close(claim.messages)

	inner := &recordingHandler{}
	handler := WrapConsumerGroupHandler(inner, client, "vitess-group")
	if err := handler.ConsumeClaim(&stubSession{ctx: context.Background()}, claim); err != nil {
		t.Fatalf("consume: %v", err)
	}
	client.FlushSpans()

	if len(inner.seen) != 1 {
		t.Fatalf("the wrapped handler saw %d messages, want 1", len(inner.seen))
	}
	body := spooled(t, dir)
	for _, want := range []string{
		`"PROCESS movements"`,
		`"messaging.operation":"process"`,
		`"messaging.destination.name":"movements"`,
		`"messaging.consumer.group.name":"vitess-group"`,
		`"messaging.kafka.partition":"3"`,
		`"messaging.kafka.offset":"42"`,
		// The publish that caused it: same trace, so the two are one story.
		`"traceId":"` + traceID + `"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("consume span is missing %s\n%s", want, body)
		}
	}
}

func TestConsumeStartsItsOwnTraceWhenThePublisherIsUninstrumented(t *testing.T) {
	client, dir := spooling(t)
	claim := &stubClaim{messages: make(chan *sarama.ConsumerMessage, 1)}
	claim.messages <- &sarama.ConsumerMessage{Topic: "movements", Value: []byte("{}"), Offset: 1}
	close(claim.messages)

	inner := &recordingHandler{}
	handler := WrapConsumerGroupHandler(inner, client, "vitess-group")
	if err := handler.ConsumeClaim(&stubSession{ctx: context.Background()}, claim); err != nil {
		t.Fatalf("consume: %v", err)
	}
	client.FlushSpans()

	// The message still gets through, and it still gets a span: an uninstrumented
	// publisher must not cost the consumer its telemetry.
	if len(inner.seen) != 1 {
		t.Fatalf("the wrapped handler saw %d messages, want 1", len(inner.seen))
	}
	if body := spooled(t, dir); !strings.Contains(body, `"PROCESS movements"`) {
		t.Errorf("no consume span was recorded\n%s", body)
	}
}

// selectLoopHandler writes its claim loop the way sarama's own docs and most
// handlers do, calling claim.Messages() on every iteration of the select.
type selectLoopHandler struct{ seen [][]byte }

func (h *selectLoopHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *selectLoopHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *selectLoopHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	for {
		select {
		case message := <-claim.Messages():
			if message == nil {
				return nil
			}
			h.seen = append(h.seen, message.Value)
		case <-session.Context().Done():
			return nil
		}
	}
}

// A handler that calls Messages() per iteration must still see every message.
// When the proxy was rebuilt on each call, each orphaned goroutine raced the
// others for the source and then blocked forever writing into a channel nobody
// held, so messages were consumed from the broker and silently dropped.
func TestMessagesIsIdempotentSoASelectLoopSeesEveryMessage(t *testing.T) {
	client, _ := spooling(t)

	const count = 50
	claim := &stubClaim{messages: make(chan *sarama.ConsumerMessage, count)}
	for i := 0; i < count; i++ {
		claim.messages <- &sarama.ConsumerMessage{
			Topic:     "movements",
			Partition: 3,
			Offset:    int64(i),
			Value:     []byte{byte(i)},
		}
	}
	close(claim.messages)

	handler := &selectLoopHandler{}
	wrapped := WrapConsumerGroupHandler(handler, client, "group")

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = wrapped.ConsumeClaim(&stubSession{ctx: context.Background()}, claim)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ConsumeClaim did not return: the proxy swallowed messages")
	}

	if len(handler.seen) != count {
		t.Fatalf("handler saw %d of %d messages", len(handler.seen), count)
	}
}

// Messages() must hand back the same channel every time, as sarama's does.
func TestMessagesReturnsTheSameChannel(t *testing.T) {
	client, _ := spooling(t)
	claim := &stubClaim{messages: make(chan *sarama.ConsumerMessage, 1)}
	wrapped := &instrumentedClaim{
		ConsumerGroupClaim: claim,
		handler:            &ConsumerGroupHandler{client: client},
		session:            &stubSession{ctx: context.Background()},
	}
	if wrapped.Messages() != wrapped.Messages() {
		t.Fatal("Messages() returned a different channel on the second call")
	}
}
