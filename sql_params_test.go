package chronos

import (
	"database/sql/driver"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestBoundedParametersJSONRendersDriverValues(t *testing.T) {
	stamp := time.Date(2026, 8, 28, 9, 30, 0, 0, time.UTC)
	params := valueParams([]driver.Value{
		int64(42), "acme-ltd", 12.5, true, nil, []byte("blob"), stamp,
	})

	got := boundedParametersJSON(params)

	want := `["42","acme-ltd","12.5","true","","blob","2026-08-28T09:30:00Z"]`
	if got != want {
		t.Fatalf("rendered parameters = %s, want %s", got, want)
	}
}

// An empty list yields no attribute at all, so the desktop can tell "this
// collector does not capture parameters" from "this statement bound none".
func TestBoundedParametersJSONOmitsEmptyList(t *testing.T) {
	if got := boundedParametersJSON(valueParams(nil)); got != "" {
		t.Fatalf("empty parameter list rendered %q, want the empty string", got)
	}
}

func TestBoundedParametersJSONCapsCountAndLength(t *testing.T) {
	values := make([]driver.Value, 100)
	for i := range values {
		values[i] = strings.Repeat("x", 500)
	}

	var decoded []string
	if err := json.Unmarshal([]byte(boundedParametersJSON(valueParams(values))), &decoded); err != nil {
		t.Fatalf("capped parameters are not valid JSON: %v", err)
	}
	if len(decoded) != maxParameters {
		t.Fatalf("kept %d parameters, want the %d cap", len(decoded), maxParameters)
	}
	for i, value := range decoded {
		if len(value) != maxParameterLength {
			t.Fatalf("parameter %d is %d bytes, want the %d cap", i, len(value), maxParameterLength)
		}
	}
}

// Capping is by byte and can split a rune. encoding/json must still produce
// parseable output, because malformed JSON costs the whole list rather than one
// value's tail.
func TestBoundedParametersJSONSurvivesSplitRune(t *testing.T) {
	// 21 three-byte runes is 63 bytes; the 22nd straddles the 64-byte cap.
	value := strings.Repeat("☂", 22)

	var decoded []string
	if err := json.Unmarshal([]byte(boundedParametersJSON(valueParams([]driver.Value{value}))), &decoded); err != nil {
		t.Fatalf("split-rune parameter is not valid JSON: %v", err)
	}
	if len(decoded) != 1 {
		t.Fatalf("decoded %d parameters, want 1", len(decoded))
	}
}

// Named parameters are read positionally: the desktop binds the array by
// ordinal when it rebuilds the statement for EXPLAIN.
func TestBoundedParametersJSONReadsNamedValues(t *testing.T) {
	params := namedParams([]driver.NamedValue{
		{Ordinal: 1, Name: "client", Value: int64(7)},
		{Ordinal: 2, Name: "state", Value: "active"},
	})

	if got, want := boundedParametersJSON(params), `["7","active"]`; got != want {
		t.Fatalf("rendered named parameters = %s, want %s", got, want)
	}
}
