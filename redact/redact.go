// Package redact masks sensitive keys before spool write.
package redact

import (
	"regexp"
	"strings"
)

const maskPrefix = "*********"

// MaskValue keeps the last four characters for values of length >= 10.
func MaskValue(v string) string {
	if len(v) < 10 {
		return maskPrefix
	}
	return maskPrefix + v[len(v)-4:]
}

// IsSensitive reports whether key matches any pattern (case-insensitive substring).
func IsSensitive(key string, patterns []string) bool {
	lower := strings.ToLower(key)
	for _, p := range patterns {
		if p != "" && strings.Contains(lower, strings.ToLower(p)) {
			return true
		}
	}
	return false
}

// Map masks sensitive keys in a flat string map.
func Map(in map[string]string, patterns []string) map[string]string {
	if len(in) == 0 {
		return in
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		if IsSensitive(k, patterns) {
			out[k] = MaskValue(v)
		} else {
			out[k] = v
		}
	}
	return out
}

// credentialShape matches an opaque Chronos machine credential — `token_<id>.<secret>`,
// both halves unpadded base64url, as minted by the Control API's `newAccessToken`.
//
// The ID half is captured so it can be KEPT: it is stored in plaintext in
// `identity_tokens.id`, it is what a lookup indexes on, and it is not usable
// without the secret. Keeping it means a leaked-key incident can be traced to the
// key that leaked, which is the whole reason to log the exchange at all; masking
// the pair wholesale would leave a reader knowing only that some credential
// appeared.
var credentialShape = regexp.MustCompile(`(token_[A-Za-z0-9_-]{6,})\.[A-Za-z0-9_-]{16,}`)

// Credentials masks the secret half of every Chronos machine credential in text.
//
// # Why this exists separately from [Map]
//
// Every other redaction here matches a KEY name: a header called `Authorization`,
// a form field called `password`. A captured BODY has no key to match — it arrives
// as one string under `http.response.body`, and until this function existed
// nothing masked it at all. So the one place a freshly minted ingest key is
// visible in plaintext (the mint endpoint's response) was also the one place
// key-name redaction could not reach.
//
// Matched on the credential's SHAPE rather than on the field it sits in, because
// the field name is not ours to rely on: the same value travels as `secret` in one
// payload, in a `curl` example in another, and in a copy-paste inside a log line
// in a third.
func Credentials(text string) string {
	// The overwhelming majority of bodies contain no credential at all, and a
	// substring test is an order of magnitude cheaper than the regexp.
	if !strings.Contains(text, "token_") {
		return text
	}
	return credentialShape.ReplaceAllString(text, "$1."+maskPrefix)
}
