// Package redact masks sensitive keys before spool write.
package redact

import (
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
