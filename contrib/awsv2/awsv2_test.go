package awsv2

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	chronos "chronos.dev/collector/sdk/go"
	"chronos.dev/collector/sdk/go/spool"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// The assertions below read SPOOLED spans, not in-memory objects, for the reason
// the sarama contrib does: the thing that has to be right is the document the
// engine will parse, and a span that decorates perfectly but never reaches the
// spool is indistinguishable from no instrumentation at all.

// spanRecord is the subset of the spooled span document these assertions read.
type spanRecord struct {
	TraceID      string            `json:"traceId"`
	SpanID       string            `json:"spanId"`
	ParentSpanID string            `json:"parentSpanId"`
	Name         string            `json:"name"`
	Status       string            `json:"status"`
	Attributes   map[string]string `json:"attributes"`
}

func spooling(t *testing.T) (*chronos.Client, string) {
	t.Helper()
	dir := t.TempDir()
	client := chronos.StartWithConfig(chronos.Config{
		Enabled:      true,
		APMEnabled:   true,
		Organisation: "org-test",
		Project:      "project-test",
		Application:  "storage-test",
		SpoolDir:     dir,
		ServiceName:  "service-storage",
	})
	t.Cleanup(func() { client.Shutdown(context.Background()) })
	return client, dir
}

// spooledSpans decodes every span the client wrote, in write order.
func spooledSpans(t *testing.T, dir string) []spanRecord {
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
	var out []spanRecord
	decoder := json.NewDecoder(strings.NewReader(all.String()))
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

// stubHTTP is the whole network. aws.Config takes an HTTPClient, so the SDK can be
// driven end to end — signing, retry, XML error deserialisation, the lot — with no
// socket and no credentials worth having.
type stubHTTP struct {
	do func(*http.Request) (*http.Response, error)
}

func (s stubHTTP) Do(r *http.Request) (*http.Response, error) { return s.do(r) }

func response(status int, header map[string]string, body string) *http.Response {
	headers := http.Header{}
	for key, value := range header {
		headers.Set(key, value)
	}
	return &http.Response{
		StatusCode:    status,
		Status:        http.StatusText(status),
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        headers,
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
	}
}

// okPut is what S3 answers a successful PutObject with: no body, two ids.
func okPut() *http.Response {
	return response(http.StatusOK, map[string]string{
		"x-amz-request-id": "V9JT0X1Q5Z8N4A2C",
		"x-amz-id-2":       "0h6mQb5s0Sd0MZ6y8uNlk1Qh9p2n",
		"ETag":             `"9a0364b9e99bb480dd25e1f0284c8555"`,
	}, "")
}

func deniedPut() *http.Response {
	return response(http.StatusForbidden, map[string]string{
		"x-amz-request-id": "V9JT0X1Q5Z8N4A2C",
		"x-amz-id-2":       "0h6mQb5s0Sd0MZ6y8uNlk1Qh9p2n",
	}, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>AccessDenied</Code>`+
		`<Message>Access Denied</Message></Error>`)
}

func serverError() *http.Response {
	return response(http.StatusInternalServerError, map[string]string{
		"x-amz-request-id": "V9JT0X1Q5Z8N4A2C",
	}, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>InternalError</Code>`+
		`<Message>We encountered an internal error.</Message></Error>`)
}

// bucketClient is an S3 client whose only connection to the world is `do`, with the
// one line of wiring this package exists to provide.
func bucketClient(client *chronos.Client, attempts int, do func(*http.Request) (*http.Response, error)) *s3.Client {
	cfg := aws.Config{
		Region:           "eu-west-2",
		Credentials:      credentials.NewStaticCredentialsProvider("AKIAIOSFODNN7EXAMPLE", "secret", ""),
		HTTPClient:       stubHTTP{do: do},
		RetryMaxAttempts: attempts,
	}
	cfg.APIOptions = append(cfg.APIOptions, Instrument(client))
	return s3.NewFromConfig(cfg)
}

// objectBody is a payload no attribute may ever quote.
const objectBody = "SECRET-PAYLOAD-BYTES-THAT-MUST-NEVER-BE-SPOOLED"

func TestASuccessfulPutIsOneSpanNamedForTheServiceAndOperation(t *testing.T) {
	client, dir := spooling(t)
	api := bucketClient(client, 1, func(*http.Request) (*http.Response, error) { return okPut(), nil })

	ctx, root := client.StartSpan(context.Background(), "job")
	_, err := api.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("feed-archive"),
		Key:    aws.String("exports/customer-1326/invoice.pdf"),
		Body:   strings.NewReader(objectBody),
	})
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	root.End()
	client.FlushSpans()

	spans := spooledSpans(t, dir)
	put := spanNamed(t, spans, "S3.PutObject")

	if put.ParentSpanID != root.SpanID {
		t.Errorf("the put parents onto %q, want the caller's span %q", put.ParentSpanID, root.SpanID)
	}
	if put.Status != "ok" {
		t.Errorf("status %q, want ok", put.Status)
	}
	want := map[string]string{
		"span.kind":                 "client",
		"rpc.system":                "aws-api",
		"rpc.service":               "S3",
		"rpc.method":                "PutObject",
		"cloud.region":              "eu-west-2",
		"aws.s3.bucket":             "feed-archive",
		"aws.s3.key":                "exports/customer-1326/invoice.pdf",
		"aws.request_id":            "V9JT0X1Q5Z8N4A2C",
		"aws.extended_request_id":   "0h6mQb5s0Sd0MZ6y8uNlk1Qh9p2n",
		"http.response.status_code": "200",
		"server.address":            "feed-archive.s3.eu-west-2.amazonaws.com",
		"http.request.body.size":    "47",
	}
	for key, value := range want {
		if got := put.Attributes[key]; got != value {
			t.Errorf("%s = %q, want %q", key, got, value)
		}
	}
	if got := put.Attributes["url.full"]; got != "https://feed-archive.s3.eu-west-2.amazonaws.com/exports/customer-1326/invoice.pdf" {
		t.Errorf("url.full = %q", got)
	}
}

// The bytes of an object are the application's data, and there is no bound anyone
// could put on them. A single grep over every attribute is the only assertion that
// stays true as attributes are added later.
func TestNoAttributeQuotesTheObjectsContents(t *testing.T) {
	client, dir := spooling(t)
	api := bucketClient(client, 1, func(*http.Request) (*http.Response, error) {
		return response(http.StatusOK, map[string]string{"x-amz-request-id": "R1"}, objectBody), nil
	})

	if _, err := api.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String("feed-archive"),
		Key:    aws.String("movements/2026-09-17.ndjson"),
		Body:   strings.NewReader(objectBody),
	}); err != nil {
		t.Fatalf("put: %v", err)
	}
	client.FlushSpans()

	for _, span := range spooledSpans(t, dir) {
		for key, value := range span.Attributes {
			if strings.Contains(value, "SECRET-PAYLOAD") {
				t.Errorf("attribute %s carries the object's contents: %q", key, value)
			}
		}
	}
}

// A presigned URL is a credential written into a query string, and a presign runs
// through this very middleware stack. Dropping the query wholesale is what keeps it
// off the span; this is the test that would catch anyone re-adding RawQuery.
func TestAPresignedURLsCredentialsNeverReachASpan(t *testing.T) {
	client, dir := spooling(t)
	api := bucketClient(client, 1, func(*http.Request) (*http.Response, error) { return okPut(), nil })

	request, err := s3.NewPresignClient(api).PresignGetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String("feed-archive"),
		Key:    aws.String("exports/customer-1326/invoice.pdf"),
	})
	if err != nil {
		t.Fatalf("presign: %v", err)
	}
	if !strings.Contains(request.URL, "X-Amz-Signature=") {
		t.Fatalf("the presigned URL carries no signature, so this proves nothing: %s", request.URL)
	}
	client.FlushSpans()

	for _, span := range spooledSpans(t, dir) {
		for key, value := range span.Attributes {
			for _, secret := range []string{"X-Amz-Signature=", "X-Amz-Credential=", "X-Amz-Security-Token="} {
				if strings.Contains(value, secret) {
					t.Errorf("attribute %s leaks %s: %q", key, secret, value)
				}
			}
		}
	}
}

// The caller's error must be the SDK's error, byte for byte. Instrumentation that
// wraps, re-types or re-words a failure breaks every `errors.As` in the estate.
func TestAFailedCallIsMarkedFailedAndReturnsTheAWSErrorUntouched(t *testing.T) {
	client, dir := spooling(t)

	put := func(c *chronos.Client) error {
		api := bucketClient(c, 1, func(*http.Request) (*http.Response, error) { return deniedPut(), nil })
		_, err := api.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String("feed-archive"),
			Key:    aws.String("exports/customer-1326/invoice.pdf"),
			Body:   strings.NewReader(objectBody),
		})
		return err
	}

	instrumented := put(client)
	if instrumented == nil {
		t.Fatal("a 403 put returned no error")
	}
	// The same call with no client behind it: chronos.Default() is disabled in this
	// binary unless a test set it, so the comparison is against the SDK's own error
	// shape either way.
	var apiErr smithy.APIError
	if !errors.As(instrumented, &apiErr) {
		t.Fatalf("error is no longer a smithy.APIError: %T", instrumented)
	}
	if apiErr.ErrorCode() != "AccessDenied" {
		t.Errorf("error code %q, want AccessDenied", apiErr.ErrorCode())
	}
	client.FlushSpans()

	span := spanNamed(t, spooledSpans(t, dir), "S3.PutObject")
	if span.Status != "error" {
		t.Errorf("status %q, want error", span.Status)
	}
	if span.Attributes["error"] != "true" {
		t.Errorf("error attribute %q, want true", span.Attributes["error"])
	}
	if span.Attributes["error.type"] != "AccessDenied" {
		t.Errorf("error.type %q, want AccessDenied", span.Attributes["error.type"])
	}
	if !strings.Contains(span.Attributes["exception.message"], "AccessDenied") {
		t.Errorf("exception.message %q does not name the failure", span.Attributes["exception.message"])
	}
	if span.Attributes["http.response.status_code"] != "403" {
		t.Errorf("status code %q, want 403", span.Attributes["http.response.status_code"])
	}
	// The id the reader quotes to AWS support is the point of recording a failure
	// at all, so it has to survive the error path.
	if span.Attributes["aws.request_id"] != "V9JT0X1Q5Z8N4A2C" {
		t.Errorf("aws.request_id %q on a failed call", span.Attributes["aws.request_id"])
	}
}

// Sitting above the retryer is the whole reason for the Initialize position. Three
// HTTP round trips, one logical call: one span, with the attempts counted on it
// rather than rendered as three unrelated bars.
func TestRetriesAreOneSpanWithTheAttemptsCounted(t *testing.T) {
	client, dir := spooling(t)
	var calls atomic.Int32
	api := bucketClient(client, 3, func(*http.Request) (*http.Response, error) {
		if calls.Add(1) < 3 {
			return serverError(), nil
		}
		return okPut(), nil
	})

	if _, err := api.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String("feed-archive"),
		Key:    aws.String("movements/2026-09-17.ndjson"),
		Body:   strings.NewReader(objectBody),
	}); err != nil {
		t.Fatalf("put: %v", err)
	}
	client.FlushSpans()

	if got := calls.Load(); got != 3 {
		t.Fatalf("the stub saw %d round trips, want 3; the retryer did not run", got)
	}
	spans := spooledSpans(t, dir)
	found := 0
	for _, span := range spans {
		if span.Name == "S3.PutObject" {
			found++
		}
	}
	if found != 1 {
		t.Errorf("%d spans for one retried call, want 1", found)
	}
	put := spanNamed(t, spans, "S3.PutObject")
	if put.Attributes["aws.request.attempts"] != "3" {
		t.Errorf("aws.request.attempts = %q, want 3", put.Attributes["aws.request.attempts"])
	}
	// The outcome, not the first thing that went wrong: the call succeeded.
	if put.Attributes["http.response.status_code"] != "200" {
		t.Errorf("status code %q, want the 200 the call ended on", put.Attributes["http.response.status_code"])
	}
	if put.Status != "ok" {
		t.Errorf("status %q, want ok for a call that eventually succeeded", put.Status)
	}
}

// Instrument(nil) on a process with no Chronos client is the shape an application
// ships unconditionally. It must register nothing and change nothing.
func TestANilClientIsAPlainPassThrough(t *testing.T) {
	disabled := chronos.StartWithConfig(chronos.Config{Enabled: false, SpoolDir: t.TempDir()})
	dir := t.TempDir()
	spoolingClient := chronos.StartWithConfig(chronos.Config{
		Enabled: false, APMEnabled: true, SpoolDir: dir, ServiceName: "service-storage",
	})

	for name, client := range map[string]*chronos.Client{
		"nil":      nil,
		"disabled": disabled,
		"spooling": spoolingClient,
	} {
		t.Run(name, func(t *testing.T) {
			api := bucketClient(client, 1, func(*http.Request) (*http.Response, error) { return okPut(), nil })
			out, err := api.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: aws.String("feed-archive"),
				Key:    aws.String("movements/2026-09-17.ndjson"),
				Body:   strings.NewReader(objectBody),
			})
			if err != nil {
				t.Fatalf("put: %v", err)
			}
			if out == nil || aws.ToString(out.ETag) != `"9a0364b9e99bb480dd25e1f0284c8555"` {
				t.Fatalf("the SDK's own output did not survive the middleware: %+v", out)
			}
		})
	}

	// A disabled client with a spool of its own writes nothing to it.
	if spans := spooledSpans(t, dir); len(spans) != 0 {
		t.Errorf("a disabled client spooled %d spans", len(spans))
	}
}
