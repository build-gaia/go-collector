package chronos

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"runtime/pprof"
	"strings"
	"time"

	"chronos.dev/collector/sdk/go/redact"
)

// Handler wraps next with a server span per request. Propagates W3C traceparent.
// When profiling is enabled, request goroutines inherit pprof labels (route,
// http.method) so process-wide CPU/goroutine profiles can be rolled up by route
// without starting a per-request CPU profile.
func (c *Client) Handler(next http.Handler) http.Handler {
	if c == nil || !c.cfg.Enabled || !c.cfg.APMEnabled {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		route := httpRoute(r)
		if shouldSkipHTTPPath(c.cfg.HTTPSkipPaths, route) {
			next.ServeHTTP(w, r)
			return
		}
		ctx := r.Context()
		name := r.Method + " " + route
		ctx, span := c.StartSpan(ctx, name)
		if tp := r.Header.Get("traceparent"); tp != "" {
			if traceID, parentID, ok := ParseTraceparent(tp); ok {
				span.TraceID = traceID
				span.ParentID = parentID
			}
		}
		span.HTTPMethod = r.Method
		span.HTTPRoute = route
		span.SetAttribute("http.method", r.Method)
		span.SetAttribute("url.path", r.URL.Path)
		if r.URL.RawQuery != "" {
			span.SetAttribute("url.query", r.URL.RawQuery)
		}
		scheme := requestScheme(r)
		span.SetAttribute("url.scheme", scheme)
		host := requestHost(r)
		span.SetAttribute("url.full", scheme+"://"+host+r.URL.RequestURI())

		if c.cfg.HTTPCapture {
			captureRequestExchange(span, r, c.cfg)
			if c.cfg.HTTPCaptureBodies && r.Body != nil {
				body, truncated := readBodyCap(r.Body, c.cfg.HTTPMaxBody)
				_ = r.Body.Close()
				r.Body = io.NopCloser(bytes.NewReader(body))
				span.SetAttribute("http.request.body", string(body))
				span.SetAttribute("http.request.body.size", attrInt(len(body)))
				if truncated {
					span.SetAttribute("http.request.body.truncated", "true")
				}
				if ct := r.Header.Get("Content-Type"); ct != "" {
					span.SetAttribute("http.request.body.content_type", ct)
				}
			}
		}

		rw := &responseRecorder{
			ResponseWriter: w,
			status:         200,
			captureBody:    c.cfg.HTTPCaptureBodies,
			maxBody:        c.cfg.HTTPMaxBody,
		}
		defer func() {
			span.HTTPStatus = uint32Status(rw.status)
			span.SetAttribute("http.status_code", attrInt(rw.status))
			span.SetStatus(statusFromCode(rw.status))
			if c.cfg.HTTPCapture {
				captureResponseExchange(span, rw.Header(), c.cfg)
				if c.cfg.HTTPCaptureBodies && len(rw.body) > 0 {
					span.SetAttribute("http.response.body", string(rw.body))
					span.SetAttribute("http.response.body.size", attrInt(len(rw.body)))
					if rw.bodyTruncated {
						span.SetAttribute("http.response.body.truncated", "true")
					}
					if ct := rw.Header().Get("Content-Type"); ct != "" {
						span.SetAttribute("http.response.body.content_type", ct)
					}
				}
			}
			c.recordHTTP(rw.status, time.Since(span.StartedAt))
			span.End()
		}()

		w.Header().Set("traceparent", span.Traceparent())
		serve := func(ctx context.Context) {
			next.ServeHTTP(rw, r.WithContext(ctx))
		}
		if c.cfg.ProfilerEnabled {
			// trace_id / span_id ride along so CPU samples taken during the request
			// carry a correlation back to this trace (see takeCorrelation).
			pprof.Do(ctx, pprof.Labels(
				"http.method", r.Method,
				"route", route,
				"trace_id", span.TraceID,
				"span_id", span.SpanID,
			), serve)
			return
		}
		serve(ctx)
	})
}

// httpRoute prefers the Go 1.22+ ServeMux pattern when set, else the URL path.
func httpRoute(r *http.Request) string {
	if r.Pattern != "" {
		return r.Pattern
	}
	if r.URL != nil && r.URL.Path != "" {
		return r.URL.Path
	}
	return "/"
}

func shouldSkipHTTPPath(skip []string, route string) bool {
	if len(skip) == 0 || route == "" {
		return false
	}
	// Patterns may be "GET /status" from ServeMux; compare on path only.
	path := route
	if i := strings.IndexByte(route, ' '); i >= 0 && i+1 < len(route) {
		path = route[i+1:]
	}
	for _, candidate := range skip {
		if candidate == "" {
			continue
		}
		if path == candidate || route == candidate {
			return true
		}
	}
	return false
}

type responseRecorder struct {
	http.ResponseWriter
	status        int
	body          []byte
	bodyTruncated bool
	wroteHeader   bool
	captureBody   bool
	maxBody       int
}

func (r *responseRecorder) WriteHeader(code int) {
	if r.wroteHeader {
		return
	}
	r.wroteHeader = true
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *responseRecorder) Write(b []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	if r.captureBody && r.maxBody > 0 {
		maxBody := bodyCapBytes(r.Header().Get("Content-Type"), r.maxBody)
		remain := maxBody - len(r.body)
		if remain > 0 {
			if len(b) > remain {
				r.body = append(r.body, b[:remain]...)
				r.bodyTruncated = true
			} else {
				r.body = append(r.body, b...)
			}
		} else {
			r.bodyTruncated = true
		}
	}
	return r.ResponseWriter.Write(b)
}

func (r *responseRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hj, ok := r.ResponseWriter.(http.Hijacker); ok {
		return hj.Hijack()
	}
	return nil, nil, http.ErrNotSupported
}

func (r *responseRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func requestScheme(r *http.Request) string {
	if proto := r.Header.Get("X-Forwarded-Proto"); proto != "" {
		return strings.TrimSpace(strings.Split(proto, ",")[0])
	}
	if r.TLS != nil {
		return "https"
	}
	return "http"
}

func requestHost(r *http.Request) string {
	if host := r.Header.Get("X-Forwarded-Host"); host != "" {
		return strings.TrimSpace(strings.Split(host, ",")[0])
	}
	return r.Host
}

func captureRequestExchange(span *Span, r *http.Request, cfg Config) {
	headers := headerMap(r.Header)
	if cfg.HTTPRedact {
		headers = redact.Map(headers, cfg.RedactPatterns)
	}
	setJSONObjectAttr(span, "http.request.headers", headers)

	cookies := cookieMap(r)
	if cfg.HTTPRedact {
		cookies = redact.Map(cookies, cfg.RedactPatterns)
	}
	if len(cookies) > 0 {
		setJSONObjectAttr(span, "http.request.cookies", cookies)
	}

	query := queryMap(r)
	if len(query) > 0 {
		setJSONObjectAttr(span, "http.request.query", query)
	}
}

func captureResponseExchange(span *Span, h http.Header, cfg Config) {
	headers := headerMap(h)
	if cfg.HTTPRedact {
		headers = redact.Map(headers, cfg.RedactPatterns)
	}
	setJSONObjectAttr(span, "http.response.headers", headers)
}

func headerMap(h http.Header) map[string]string {
	out := make(map[string]string, len(h))
	for k, vals := range h {
		if len(vals) == 0 {
			continue
		}
		out[k] = vals[0]
	}
	return out
}

func cookieMap(r *http.Request) map[string]string {
	cookies := r.Cookies()
	if len(cookies) == 0 {
		return nil
	}
	out := make(map[string]string, len(cookies))
	for _, c := range cookies {
		if c == nil || c.Name == "" {
			continue
		}
		out[c.Name] = c.Value
	}
	return out
}

func queryMap(r *http.Request) map[string]string {
	if r.URL == nil {
		return nil
	}
	values := r.URL.Query()
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]string, len(values))
	for k, vals := range values {
		if len(vals) == 0 {
			continue
		}
		out[k] = vals[0]
	}
	return out
}

func setJSONObjectAttr(span *Span, key string, values map[string]string) {
	if span == nil || len(values) == 0 {
		return
	}
	raw, err := json.Marshal(values)
	if err != nil {
		return
	}
	span.SetAttribute(key, string(raw))
}

func readBodyCap(r io.Reader, max int) ([]byte, bool) {
	if max <= 0 {
		max = 65536
	}
	limited := io.LimitReader(r, int64(max)+1)
	body, err := io.ReadAll(limited)
	if err != nil {
		return nil, false
	}
	if len(body) > max {
		return body[:max], true
	}
	return body, false
}

// htmlMaxBodyBytes is the hard ceiling for text/html response bodies. Error pages
// and rendered views routinely exceed the default 64 KiB cap; 100 MiB keeps a full
// page readable without letting a runaway view pin the process.
const htmlMaxBodyBytes = 100 * 1024 * 1024

func bodyCapBytes(contentType string, defaultMax int) int {
	media, _, _ := strings.Cut(contentType, ";")
	if strings.EqualFold(strings.TrimSpace(media), "text/html") {
		if defaultMax > htmlMaxBodyBytes {
			return defaultMax
		}
		return htmlMaxBodyBytes
	}
	if defaultMax <= 0 {
		return 65536
	}
	return defaultMax
}

// Transport wraps an http.RoundTripper so outbound calls become client spans, carry
// W3C trace context to the next service, and land in the IO profile alongside SQL.
//
// Unlike the SQL driver — which chronos.Open installs, so it needs no call-site change —
// an HTTP client has no registry to hook, so this is the one seam a caller must opt into:
//
//	client := &http.Client{Transport: chronos.Default().Transport(nil)}
func (c *Client) Transport(next http.RoundTripper) http.RoundTripper {
	if next == nil {
		next = http.DefaultTransport
	}
	if c == nil || !c.cfg.Enabled {
		return next
	}
	return &chronosTransport{next: next, client: c}
}

type chronosTransport struct {
	next   http.RoundTripper
	client *Client
}

func (t *chronosTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	start := time.Now()

	var span *Span
	if t.client.cfg.APMEnabled && SpanFromContext(ctx) != nil {
		_, span = t.client.StartSpan(ctx, "HTTP "+req.Method)
		span.SetAttribute("span.kind", "client")
		span.SetAttribute("http.request.method", req.Method)
		span.SetAttribute("url.full", req.URL.Redacted())
		span.SetAttribute("server.address", req.URL.Host)
		span.HTTPMethod = req.Method
		span.HTTPRoute = req.URL.Path
		if tp := span.Traceparent(); tp != "" {
			// Clone before mutating: the caller owns the request it handed us.
			req = req.Clone(ctx)
			req.Header.Set("traceparent", tp)
		}
	}

	res, err := t.next.RoundTrip(req)

	// skip: runtime.Callers, recordIOWait, RoundTrip → the caller's own code.
	t.client.recordIOWait(ctx, "HTTP "+req.Method+" "+req.URL.Host, "http", time.Since(start), 4)

	if span != nil {
		if err != nil {
			span.SetStatus("error")
			span.SetAttribute("error", "true")
			span.SetAttribute("exception.message", err.Error())
		} else if res != nil {
			span.HTTPStatus = uint32Status(res.StatusCode)
			span.SetAttribute("http.response.status_code", attrInt(res.StatusCode))
			span.SetStatus(statusFromCode(res.StatusCode))
		}
		span.End()
	}
	return res, err
}
