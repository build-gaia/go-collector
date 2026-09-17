// Package awsv2 instruments aws-sdk-go-v2 clients with Chronos client spans.
//
// Its own module, not part of the core SDK, for the reason the sarama contrib is:
// the core SDK has four dependencies and a service that talks to no AWS API
// should not compile the AWS SDK to get HTTP tracing.
//
// # One line, then every AWS call in the process
//
// aws.Config carries `APIOptions`, a list of functions each of which is handed
// the smithy middleware stack of every operation the SDK runs. That is the only
// seam in Go that behaves like the PHP extension's auto-instrumentation: hook it
// once at config time and S3, SQS, DynamoDB and everything else are spanned
// without a single call site changing.
//
//	cfg.APIOptions = append(cfg.APIOptions, chronosaws.Instrument(nil))
//
// # Where in the stack, and why it matters
//
// The stack runs Initialize → Serialize → Build → Finalize → Deserialize, and the
// retryer lives in Finalize. So the position is not a detail, it decides what a
// span means:
//
//   - Initialize wraps the retry loop. One span per logical API call, whatever the
//     SDK had to do to complete it — which is the question a reader of a trace is
//     asking ("how long did this PutObject take") rather than "how many TCP
//     conversations did it take". Below Finalize the same call would render as
//     three sibling spans with no parent saying they were one attempt each.
//   - But Initialize cannot see an HTTP response: its output carries the
//     deserialised result, not the wire. Status, endpoint and the S3 extended
//     request id only exist at Deserialize.
//
// Hence two middlewares, not one. The span is owned at Initialize (added `After`,
// so the SDK's own RegisterServiceMetadata has already put the service id,
// operation name and region in the context by the time it runs), and a second at
// Deserialize decorates that same span with the wire facts. Deserialize runs once
// per attempt, so the last attempt's status and endpoint are what land — the
// outcome, which is what the span as a whole reports. The attempt count is kept
// separately as `aws.request.attempts` so a call that succeeded on the third try
// is still legible as one slow span rather than disappearing.
//
// # What is NOT captured
//
// Object CONTENTS, ever. The request and response bodies of an S3 call ARE the
// application's data, and unlike an HTTP API body there is no size anyone could
// sensibly bound — see the header of the core SDK's fs.go, which this follows.
// Sizes, buckets, keys, status and request ids answer "what did this cost, did it
// work, and what can I quote to AWS support"; the bytes answer a question
// telemetry should not be asking.
//
// Query strings are dropped from every address this package writes, rather than
// masked key by key. A presigned URL puts `X-Amz-Credential` and `X-Amz-Signature`
// in exactly that query, a presign goes through this same middleware stack, and
// masking per key would mean keeping a list of every credential-bearing parameter
// AWS ever adds. Dropping the whole query needs no such list. Free-text that could
// still quote one — an error message — goes through [redactText].
//
// # Fail-open, always
//
// Nothing here can change an API call's result or its error. A nil client, a
// disabled SDK, or a middleware the stack refuses to accept costs telemetry and
// nothing else.
package awsv2

import (
	"context"
	"errors"
	"net/url"
	"reflect"
	"regexp"
	"strconv"

	chronos "chronos.dev/collector/sdk/go"
	"chronos.dev/collector/sdk/go/redact"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// System is the `rpc.system` value every span here carries. OTel's RPC conventions
// name the AWS SDK as one protocol rather than one per service, so a reader can
// select all AWS work with a single predicate and then narrow by `rpc.service`.
const System = "aws-api"

const (
	// spanMiddlewareID and responseMiddlewareID are also the idempotence guard: a
	// stack refuses a second middleware with an ID it already holds, so applying
	// Instrument twice to one config produces one span, not two.
	spanMiddlewareID     = "ChronosSpan"
	responseMiddlewareID = "ChronosResponse"

	// AttrRPCService is the AWS service id — "S3", "SQS" — as the SDK itself names
	// it, not as the endpoint spells it.
	AttrRPCService = "rpc.service"
	// AttrRPCMethod is the API operation — "PutObject".
	AttrRPCMethod = "rpc.method"
	// AttrRequestID is the AWS request id, the one fact AWS support asks for first.
	AttrRequestID = "aws.request_id"
	// AttrExtendedRequestID is S3's `x-amz-id-2`, which support asks for second.
	AttrExtendedRequestID = "aws.extended_request_id"
	// AttrAttempts is how many times the SDK had to try. ABSENT rather than 1 when
	// the retryer reported nothing: "tried once" is a measurement, and an
	// unmeasured call must not be able to claim it.
	AttrAttempts = "aws.request.attempts"
	// AttrS3Bucket and AttrS3Key address the object. The key is a path, not a
	// payload, and is the difference between "S3 was slow" and "S3 was slow for
	// this one 4 GB export".
	AttrS3Bucket = "aws.s3.bucket"
	AttrS3Key    = "aws.s3.key"
)

// maxTextLength bounds a pathological key or error rather than an ordinary one. An
// S3 key may be 1024 bytes and an SDK error message quotes several of them.
const maxTextLength = 1024

// Instrument returns an aws.Config APIOption that spans every API call made
// through that config.
//
// Passing nil means the process default client, which is the ordinary case:
// chronos.Start() has already set it, and the wiring stays one line.
//
//	cfg.APIOptions = append(cfg.APIOptions, chronosaws.Instrument(nil))
//
// A nil or disabled client registers nothing at all — not a no-op middleware, no
// middleware — so an application that ships the call unconditionally pays no stack
// depth for telemetry it turned off.
func Instrument(c *chronos.Client) func(*middleware.Stack) error {
	target := c
	if target == nil {
		target = chronos.Default()
	}
	if target == nil || !target.Enabled() || !target.Config().APMEnabled {
		return func(*middleware.Stack) error { return nil }
	}
	return func(stack *middleware.Stack) error {
		if stack == nil {
			return nil
		}
		// Errors are swallowed on purpose. An APIOption that returns an error
		// FAILS THE API CALL before it is sent: a stack that will not take our
		// middleware must cost the caller its telemetry, never its write.
		_ = stack.Initialize.Add(spanMiddleware(target), middleware.After)
		_ = stack.Deserialize.Add(responseMiddleware(), middleware.Before)
		return nil
	}
}

// spanMiddleware owns the span: it opens before the retry loop and closes after it.
//
// Added with middleware.After so it is the last Initialize middleware to run.
// Order is the whole reason: the generated clients register RegisterServiceMetadata
// with middleware.Before, and APIOptions are applied after the operation has built
// its stack, so a Before of ours would jump the queue and read an empty service id
// and operation name out of a context nobody had populated yet.
func spanMiddleware(c *chronos.Client) middleware.InitializeMiddleware {
	return middleware.InitializeMiddlewareFunc(spanMiddlewareID, func(
		ctx context.Context,
		in middleware.InitializeInput,
		next middleware.InitializeHandler,
	) (middleware.InitializeOutput, middleware.Metadata, error) {
		service := awsmiddleware.GetServiceID(ctx)
		operation := awsmiddleware.GetOperationName(ctx)

		ctx, span := c.StartSpan(ctx, spanName(service, operation))
		span.SetAttribute("span.kind", "client")
		span.SetAttribute("rpc.system", System)
		if service != "" {
			span.SetAttribute(AttrRPCService, service)
		}
		if operation != "" {
			span.SetAttribute(AttrRPCMethod, operation)
		}
		// The region the call was routed to, not the one the process was
		// configured with: a cross-region client makes those two different, and
		// the routed one is what explains the latency.
		if region := awsmiddleware.GetRegion(ctx); region != "" {
			span.SetAttribute("cloud.region", region)
		}
		describeTarget(span, service, in.Parameters)

		out, metadata, err := next.HandleInitialize(ctx, in)

		if id, ok := awsmiddleware.GetRequestIDMetadata(metadata); ok && id != "" {
			span.SetAttribute(AttrRequestID, id)
		}
		if results, ok := retry.GetAttemptResults(metadata); ok && len(results.Results) > 0 {
			span.SetAttribute(AttrAttempts, strconv.Itoa(len(results.Results)))
		}
		if err != nil {
			// The AWS error code — AccessDenied, NoSuchBucket, SlowDown — under
			// OTel's `error.type` rather than a key of our own. It is the field a
			// reader groups failures by; the message below is what they read once
			// they have picked a group.
			var apiErr smithy.APIError
			if errors.As(err, &apiErr) && apiErr.ErrorCode() != "" {
				span.SetAttribute("error.type", apiErr.ErrorCode())
			}
			span.SetStatus("error")
			span.SetAttribute("error", "true")
			span.SetAttribute("exception.message", redactText(err.Error()))
		}
		span.End()
		return out, metadata, err
	})
}

// responseMiddleware decorates the span already in the context with the facts that
// only exist on the wire: the endpoint that answered, the status it answered with,
// and the ids AWS support will ask for.
//
// Added at Deserialize with middleware.Before so it wraps the send and every
// response deserializer. It runs once per ATTEMPT — the retryer is above it — and
// writes unconditionally, so the values left on the span are the last attempt's.
// That is deliberate: the span reports one logical call, and a call's status is the
// status it ended on, not the one it was retried past.
func responseMiddleware() middleware.DeserializeMiddleware {
	return middleware.DeserializeMiddlewareFunc(responseMiddlewareID, func(
		ctx context.Context,
		in middleware.DeserializeInput,
		next middleware.DeserializeHandler,
	) (middleware.DeserializeOutput, middleware.Metadata, error) {
		out, metadata, err := next.HandleDeserialize(ctx, in)

		// The innermost span on this context is ours: the only thing between the
		// Initialize middleware that opened it and here is the SDK's own stack,
		// which starts no Chronos spans. Nil when the SDK was disabled between
		// config time and the call.
		span := chronos.SpanFromContext(ctx)
		if span == nil {
			return out, metadata, err
		}

		if req, ok := in.Request.(*smithyhttp.Request); ok && req != nil && req.URL != nil {
			span.SetAttribute("server.address", req.URL.Host)
			span.SetAttribute("url.full", address(req.URL))
			// Content-Length, not the body. How many bytes an upload pushed is the
			// number that explains its duration; which bytes they were is not ours.
			if size := req.ContentLength; size > 0 {
				span.SetAttribute("http.request.body.size", strconv.FormatInt(size, 10))
			}
		}
		if res, ok := out.RawResponse.(*smithyhttp.Response); ok && res != nil && res.Response != nil {
			span.SetAttribute("http.response.status_code", strconv.Itoa(res.StatusCode))
			if extended := res.Header.Get("x-amz-id-2"); extended != "" {
				span.SetAttribute(AttrExtendedRequestID, redactText(extended))
			}
			if size := res.ContentLength; size > 0 {
				span.SetAttribute("http.response.body.size", strconv.FormatInt(size, 10))
			}
		}
		return out, metadata, err
	})
}

// describeTarget puts the object's address on an S3 span.
//
// # Why reflection, and why it is not expensive here
//
// The AWS SDK generates a distinct input struct per operation and exposes no
// accessor interface across them, so there is no way to ask "what bucket" without
// either reflecting or importing every service package this might ever see. The
// import is the worse trade: it would make a service that only speaks SQS compile
// the whole of S3 to get a span.
//
// So: reflection, but bounded to the case that pays for it. Gated on the service
// id being S3 — every other service skips on a string compare — and then exactly
// two FieldByName lookups on a struct the SDK already allocated. Nothing walks the
// input generically, and in particular nothing goes near Body.
func describeTarget(span *chronos.Span, service string, params any) {
	if service != "S3" || params == nil {
		return
	}
	value := reflect.ValueOf(params)
	for value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return
	}
	if bucket := stringField(value, "Bucket"); bucket != "" {
		span.SetAttribute(AttrS3Bucket, redactText(bucket))
	}
	// A key is an application-chosen path and can say something about the tenant
	// it belongs to — `exports/customer-1326/invoice.pdf` — which is exactly why
	// it is worth recording. It goes through the same masking as everything else
	// in case an application ever builds one out of a credential.
	if key := stringField(value, "Key"); key != "" {
		span.SetAttribute(AttrS3Key, redactText(key))
	}
}

// stringField reads one named string or *string field, or "" if the input does not
// have it — ListBuckets has no Bucket, and that is not an error.
func stringField(value reflect.Value, name string) string {
	field := value.FieldByName(name)
	if !field.IsValid() {
		return ""
	}
	if field.Kind() == reflect.Pointer {
		if field.IsNil() {
			return ""
		}
		field = field.Elem()
	}
	if field.Kind() != reflect.String {
		return ""
	}
	return field.String()
}

// spanName reads as OTel names an AWS call: `S3.PutObject`. Two facts, both already
// on the span as attributes, but a trace is read as a list of names first.
func spanName(service, operation string) string {
	switch {
	case service != "" && operation != "":
		return service + "." + operation
	case operation != "":
		return operation
	case service != "":
		return service
	default:
		return "AWS"
	}
}

// address is the endpoint WITHOUT its query string.
//
// The query is dropped rather than masked because a presigned request — built by
// the same stack this middleware is installed in — carries its credential and
// signature there, and there is no fixed list of parameter names that stays
// correct as AWS adds them. What remains, scheme, host and path, is the part a
// reader needs to tell two buckets or two endpoints apart.
func address(u *url.URL) string {
	if u == nil {
		return ""
	}
	return redactText(u.Scheme + "://" + u.Host + u.EscapedPath())
}

// sigV4Query matches a SigV4 credential carried as a query parameter, wherever it
// has been quoted into free text — an SDK error message naming the URL it failed
// on is the realistic case, since [address] already drops the query it can see.
var sigV4Query = regexp.MustCompile(`(?i)(X-Amz-(?:Signature|Credential|Security-Token)=)[^&\s"'\\]+`)

// redactText is the one gate every string this package writes passes through.
//
// Two maskings, both reusing the core SDK's redact package so a masked value looks
// the same here as it does on an HTTP span: [redact.Credentials] for a Chronos
// machine credential that has found its way into a key or a message, and the SigV4
// query parameters above for a presigned URL quoted in free text. Then a length
// bound, because an SDK error can quote several kilobytes of XML back at you.
func redactText(text string) string {
	masked := redact.Credentials(text)
	if sigV4Query.MatchString(masked) {
		masked = sigV4Query.ReplaceAllString(masked, "${1}"+redact.MaskValue(""))
	}
	if len(masked) > maxTextLength {
		return masked[:maxTextLength]
	}
	return masked
}
