package chronos

import "testing"

func TestBodyCapBytes(t *testing.T) {
	t.Parallel()

	if got := bodyCapBytes("text/html; charset=utf-8", 65536); got != htmlMaxBodyBytes {
		t.Fatalf("html response cap: got %d want %d", got, htmlMaxBodyBytes)
	}
	if got := bodyCapBytes("application/json", 65536); got != 65536 {
		t.Fatalf("json response cap: got %d want 65536", got)
	}
	if got := bodyCapBytes("TEXT/HTML", 65536); got != htmlMaxBodyBytes {
		t.Fatalf("case-insensitive html: got %d want %d", got, htmlMaxBodyBytes)
	}
	if got := bodyCapBytes("text/html", 200*1024*1024); got != 200*1024*1024 {
		t.Fatalf("configured default above html ceiling should win: got %d", got)
	}
}
