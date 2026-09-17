package chronos

import (
	"context"
	"testing"
	"time"
)

// The grammar these tests hold to is not Go's opinion: it is PHP's
// TraceContext::fromHeader regex, which every Chronos PHP service applies to
// everything Go writes. A header this package accepts but PHP rejects (or the
// reverse) is a trace that silently splits at the language boundary.

func TestTraceparentParsingRefusesWhatPHPRefuses(t *testing.T) {
	cases := map[string]string{
		"an unknown version":     "ff-4f1a2b3c4d5e6f708192a3b4c5d6e7f8-0123456789abcdef-01",
		"a future version":       "01-4f1a2b3c4d5e6f708192a3b4c5d6e7f8-0123456789abcdef-01",
		"upper-case hex":         "00-4F1A2B3C4D5E6F708192A3B4C5D6E7F8-0123456789ABCDEF-01",
		"an all-zero trace id":   "00-00000000000000000000000000000000-0123456789abcdef-01",
		"an all-zero span id":    "00-4f1a2b3c4d5e6f708192a3b4c5d6e7f8-0000000000000000-01",
		"a short trace id":       "00-4f1a2b3c4d5e6f708192a3b4c5d6e7-0123456789abcdef-01",
		"non-hex in the span id": "00-4f1a2b3c4d5e6f708192a3b4c5d6e7f8-0123456789abcdeg-01",
		"missing flags":          "00-4f1a2b3c4d5e6f708192a3b4c5d6e7f8-0123456789abcdef",
		"non-hex flags":          "00-4f1a2b3c4d5e6f708192a3b4c5d6e7f8-0123456789abcdef-zz",
		"a trailing list member": "00-4f1a2b3c4d5e6f708192a3b4c5d6e7f8-0123456789abcdef-01-extra",
		"nothing at all":         "",
	}
	for name, header := range cases {
		if _, ok := ParseTraceparentContext(header); ok {
			t.Errorf("accepted %s (%q); PHP's TraceContext::fromHeader throws on it", name, header)
		}
	}
}

func TestTraceparentParsingCarriesTheSamplingDecision(t *testing.T) {
	sampled, ok := ParseTraceparentContext("00-4f1a2b3c4d5e6f708192a3b4c5d6e7f8-0123456789abcdef-01")
	if !ok || !sampled.Sampled {
		t.Fatalf("a -01 header must parse as sampled, got %+v ok=%v", sampled, ok)
	}
	unsampled, ok := ParseTraceparentContext("00-4f1a2b3c4d5e6f708192a3b4c5d6e7f8-0123456789abcdef-00")
	if !ok {
		t.Fatal("a -00 header is valid trace context, not garbage")
	}
	if unsampled.Sampled {
		t.Error("a -00 header claims NOT sampled; reading it as sampled resurrects a dropped trace")
	}
}

// The flag survives the whole hop: inbound -00, every child span, and the header
// the next hop is handed. This is the defect that made a Go service in the middle
// of a PHP chain un-drop a trace the originator had decided not to keep.
func TestTheUnsampledFlagSurvivesAGoHop(t *testing.T) {
	client := StartWithConfig(Config{
		Enabled: true, APMEnabled: true, SpoolDir: t.TempDir(),
		Organisation: "org", Project: "project", Application: "app",
	})
	defer client.Shutdown(context.Background())

	inbound, ok := ParseTraceparentContext("00-4f1a2b3c4d5e6f708192a3b4c5d6e7f8-0123456789abcdef-00")
	if !ok {
		t.Fatal("parse")
	}
	ctx := ContextWithRemoteContext(context.Background(), inbound)
	_, span := client.StartSpan(ctx, "work")
	defer span.End()

	if span.Sampled {
		t.Error("a child of an unsampled parent claims to be sampled")
	}
	header := span.Traceparent()
	if want := "00-4f1a2b3c4d5e6f708192a3b4c5d6e7f8-" + span.SpanID + "-00"; header != want {
		t.Errorf("forwarded %q, want %q", header, want)
	}
}

// A trace this process starts is sampled: there is no sampler on the Go side, so
// the only honest flag for a locally born trace is 01.
func TestALocallyStartedTraceIsSampled(t *testing.T) {
	client := StartWithConfig(Config{
		Enabled: true, APMEnabled: true, SpoolDir: t.TempDir(),
		Organisation: "org", Project: "project", Application: "app",
	})
	defer client.Shutdown(context.Background())

	_, span := client.StartSpan(context.Background(), "job")
	defer span.End()
	if !span.Sampled {
		t.Fatal("a root span must be sampled")
	}
	if got := span.Traceparent(); got[len(got)-3:] != "-01" {
		t.Errorf("root renders %q, want -01 flags", got)
	}
}

// Rendering is PHP's TraceContext::header() / SpanReservation::header(), which is
// string concatenation, not a formatter with its own ideas about case or padding.
func TestTraceparentRenderingMatchesPHP(t *testing.T) {
	const trace = "4f1a2b3c4d5e6f708192a3b4c5d6e7f8"
	const span = "0123456789abcdef"
	if got, want := FormatTraceparent(trace, span, true), "00-"+trace+"-"+span+"-01"; got != want {
		t.Errorf("sampled: %q want %q", got, want)
	}
	if got, want := FormatTraceparent(trace, span, false), "00-"+trace+"-"+span+"-00"; got != want {
		t.Errorf("unsampled: %q want %q", got, want)
	}
	if FormatTraceparent("", span, true) != "" {
		t.Error("an incomplete context must render nothing rather than a header naming no span")
	}
}

// tracestate and baggage are re-rendered rather than echoed, so the bytes a Go hop
// forwards are the bytes a PHP hop would have forwarded — and so a header carrying
// a CRLF cannot be spliced into a broker frame or an outbound request.
func TestTracestateNormalisation(t *testing.T) {
	cases := []struct{ in, want string }{
		{"rojo=00f067aa0ba902b7,congo=t61rcWkgMzE", "rojo=00f067aa0ba902b7,congo=t61rcWkgMzE"},
		{" rojo=1 , congo=2 ", "rojo=1,congo=2"},
		{"rojo=1,rojo=2", "rojo=1"},
		{"tenant@vendor=1", "tenant@vendor=1"},
		{"ROJO=1,rojo=2", "rojo=2"},
		{"rojo=bad\r\nx-injected: 1", ""},
		{"=novalue,rojo=1", "rojo=1"},
		{"", ""},
	}
	for _, c := range cases {
		if got := NormalizeTracestate(c.in); got != c.want {
			t.Errorf("NormalizeTracestate(%q) = %q, want %q", c.in, got, c.want)
		}
	}
	over := ""
	for i := 0; i < 40; i++ {
		over += "k" + string(rune('a'+i%26)) + string(rune('a'+i/26)) + "=1,"
	}
	if members := len(splitMembers(NormalizeTracestate(over))); members != 32 {
		t.Errorf("kept %d members, W3C and PHP both cap at 32", members)
	}
}

func TestBaggageNormalisation(t *testing.T) {
	cases := []struct{ in, want string }{
		{"userId=alice", "userId=alice"},
		{"userId=alice;meta=1,tenant=acme", "userId=alice,tenant=acme"},
		{"key=a%20b", "key=a%20b"},
		{"key=a b", "key=a%20b"},
		{"key=a+b", "key=a%2Bb"},
		{"dup=1,dup=2", "dup=1"},
		{"bad key=1,ok=2", "ok=2"},
		{"", ""},
	}
	for _, c := range cases {
		if got := NormalizeBaggage(c.in); got != c.want {
			t.Errorf("NormalizeBaggage(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func splitMembers(value string) []string {
	if value == "" {
		return nil
	}
	out := []string{""}
	for i := 0; i < len(value); i++ {
		if value[i] == ',' {
			out = append(out, "")
			continue
		}
		out[len(out)-1] += string(value[i])
	}
	return out
}

// The stamp is a number PHP will subtract from, so its format is part of the wire
// contract: sprintf('%.6F', microtime(true)).
func TestEnqueuedAtRendersAsPHPMicrotime(t *testing.T) {
	at := time.Unix(1757000000, 123456000).UTC()
	if got, want := FormatEnqueuedAt(at), "1757000000.123456"; got != want {
		t.Errorf("FormatEnqueuedAt = %q, want %q", got, want)
	}
}

// Port of MessagingWait, including the cases it refuses to answer. Zero means
// "picked up instantly"; an unmeasured queue must not be able to report it.
func TestWaitMillisecondsMatchesMessagingWait(t *testing.T) {
	start := time.Unix(1757000002, 500000000)
	if got, ok := WaitMilliseconds("1757000000.000000", start); !ok || got != 2500 {
		t.Errorf("got %d ok=%v, want 2500", got, ok)
	}
	for _, unknowable := range []string{"", "   ", "not-a-number", "0", "-1", "1757000003.000000"} {
		if got, ok := WaitMilliseconds(unknowable, start); ok {
			t.Errorf("answered %d for %q; MessagingWait returns null", got, unknowable)
		}
	}
}
