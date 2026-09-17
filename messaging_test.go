package chronos_test

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/require"

	chronos "chronos.dev/collector/sdk/go"
)

func messagingClient(t *testing.T, capture bool, maxBody int) *chronos.Client {
	t.Helper()
	return chronos.StartWithConfig(chronos.Config{
		Enabled:                true,
		Organisation:           "org-local",
		Project:                "project-a",
		Application:            "client-stream-producer",
		SpoolDir:               t.TempDir(),
		ServiceName:            "service-client-stream-producer",
		APMEnabled:             true,
		MessagingCaptureBodies: capture,
		MessagingMaxBody:       maxBody,
	})
}

func TestMessagingSpanCapturesJSONBody(t *testing.T) {
	client := messagingClient(t, true, 0)
	defer client.Shutdown(context.Background())

	payload := []byte(`{"sku":"ABC","qty":2}`)
	_, span := client.StartMessagingSpan(context.Background(), chronos.MessagingSpan{
		System:      "kafka",
		Operation:   chronos.OperationProcess,
		Destination: "stock_pool_movement",
		BodySize:    len(payload),
		Body:        payload,
	})
	require.Equal(t, string(payload), span.Attributes["messaging.message.body"])
	require.Equal(t, "21", span.Attributes["messaging.message.body.size"])
	require.Empty(t, span.Attributes["messaging.message.body.encoding"])
}

func TestMessagingSpanBase64sBinaryBody(t *testing.T) {
	client := messagingClient(t, true, 0)
	defer client.Shutdown(context.Background())

	payload := []byte{0x00, 0x01, 0x02, 0xff}
	_, span := client.StartMessagingSpan(context.Background(), chronos.MessagingSpan{
		System:    "kafka",
		Operation: chronos.OperationPublish,
		Body:      payload,
	})
	require.Equal(t, base64.StdEncoding.EncodeToString(payload), span.Attributes["messaging.message.body"])
	require.Equal(t, "base64", span.Attributes["messaging.message.body.encoding"])
	require.Equal(t, "4", span.Attributes["messaging.message.body.size"])
}

func TestMessagingSpanOmitsBodyWhenCaptureOff(t *testing.T) {
	client := messagingClient(t, false, 0)
	defer client.Shutdown(context.Background())

	payload := []byte(`{"sku":"ABC"}`)
	_, span := client.StartMessagingSpan(context.Background(), chronos.MessagingSpan{
		System:    "kafka",
		Operation: chronos.OperationPublish,
		BodySize:  len(payload),
		Body:      payload,
	})
	require.Empty(t, span.Attributes["messaging.message.body"])
	require.Equal(t, "13", span.Attributes["messaging.message.body.size"])
}

func TestMessagingSpanTruncatesToMaxBody(t *testing.T) {
	client := messagingClient(t, true, 4)
	defer client.Shutdown(context.Background())

	_, span := client.StartMessagingSpan(context.Background(), chronos.MessagingSpan{
		System:    "kafka",
		Operation: chronos.OperationPublish,
		BodySize:  8,
		Body:      []byte("abcdefgh"),
	})
	require.Equal(t, "abcd", span.Attributes["messaging.message.body"])
	require.Equal(t, "true", span.Attributes["messaging.message.body.truncated"])
	require.Equal(t, "8", span.Attributes["messaging.message.body.size"])
}

func TestMessagingCaptureBodiesDefaultOn(t *testing.T) {
	t.Setenv("CHRONOS_ENABLED", "1")
	t.Setenv("CHRONOS_ORGANISATION_ID", "org-local")
	t.Setenv("CHRONOS_PROJECT_ID", "project-a")
	t.Setenv("CHRONOS_APPLICATION_ID", "app")
	t.Setenv("CHRONOS_SPOOL_DIRECTORY", t.TempDir())
	t.Setenv("CHRONOS_GO_HTTP_CAPTURE_BODIES", "")
	t.Setenv("CHRONOS_GO_MESSAGING_CAPTURE_BODIES", "")
	cfg := chronos.LoadConfig()
	require.True(t, cfg.HTTPCaptureBodies)
	require.True(t, cfg.MessagingCaptureBodies)
}

// The kind a messaging span wears is a cross-language contract, not a local
// preference: a Go publish and a PHP publish to the same topic must sort the
// same way in the waterfall and on the service map. This drifted once — Go said
// `client` where PHP said `producer` — and nothing caught it, because nothing
// asserted it.
func TestMessagingSpanKindMatchesTheNormalisedContract(t *testing.T) {
	client := messagingClient(t, true, 0)
	defer client.Shutdown(context.Background())

	for operation, want := range map[string]string{
		chronos.OperationPublish: "producer",
		chronos.OperationProcess: "consumer",
		chronos.OperationReceive: "consumer",
	} {
		_, span := client.StartMessagingSpan(context.Background(), chronos.MessagingSpan{
			System:      "kafka",
			Operation:   operation,
			Destination: "orders",
		})
		require.Equal(t, want, span.Attributes["span.kind"],
			"operation %q must be span.kind %q", operation, want)
	}
}
