package chronos

import (
	"database/sql/driver"
	"encoding/json"
	"strconv"
	"time"
)

// The bound values of a statement are what turn a fingerprint back into a query
// an operator can EXPLAIN, so they ride the same two attributes the PHP
// collector writes — `db.parameters` (a JSON array of stringified values, in
// ordinal order) and `db.parameters.count` (how many were actually bound, before
// any capping). The desktop reads exactly that pair; see parseParameters in
// components/trace/spanModel.ts and the engine's per-fingerprint parameter
// sampler behind /api/v1/db/queries/{fingerprint}/parameters.
//
// The caps mirror Span::MAX_PARAMETERS / MAX_PARAMETER_LENGTH on the PHP side:
// at most 32 values, each capped to 64 bytes. A statement bound with a 5 MB blob
// or a 10,000-row IN list must not turn one span into an unbounded payload, and
// a value longer than 64 bytes has already told the operator what shape it is.
const (
	maxParameters      = 32
	maxParameterLength = 64
)

// paramList is one statement's bound parameters in either of the two shapes
// database/sql hands a driver: the modern []driver.NamedValue and the legacy
// []driver.Value that the pre-context Stmt.Exec / Stmt.Query paths still carry.
// Holding both without converting keeps the untraced path allocation-free —
// most statements run with no ambient span and never read these at all.
type paramList struct {
	named  []driver.NamedValue
	values []driver.Value
}

func namedParams(args []driver.NamedValue) paramList { return paramList{named: args} }
func valueParams(args []driver.Value) paramList      { return paramList{values: args} }

func (p paramList) len() int {
	if p.named != nil {
		return len(p.named)
	}
	return len(p.values)
}

func (p paramList) at(i int) driver.Value {
	if p.named != nil {
		return p.named[i].Value
	}
	return p.values[i]
}

// boundedParametersJSON renders the first maxParameters values as a JSON array
// of strings. It returns "" for an empty list so the caller can omit the
// attribute entirely: a missing attribute reads as "this collector does not
// capture parameters", an empty array as "this statement bound none".
//
// Capping is by byte, which can split a multi-byte rune. That is safe here
// because encoding/json replaces invalid UTF-8 with U+FFFD, so the result is
// always parseable JSON — a truncated value is a display concern, malformed
// JSON would cost the whole list.
func boundedParametersJSON(params paramList) string {
	count := params.len()
	if count == 0 {
		return ""
	}
	if count > maxParameters {
		count = maxParameters
	}
	capped := make([]string, count)
	for i := 0; i < count; i++ {
		capped[i] = capParameter(stringifyParameter(params.at(i)))
	}
	encoded, err := json.Marshal(capped)
	if err != nil {
		return ""
	}
	return string(encoded)
}

func capParameter(value string) string {
	if len(value) > maxParameterLength {
		return value[:maxParameterLength]
	}
	return value
}

// stringifyParameter renders a driver.Value the way the PHP collector renders a
// binding, so the same query fingerprinted from two runtimes shows its values
// the same way. driver.Value is a closed set after the driver's conversion, but
// a NamedValueChecker may pass through its own types, hence the JSON fallback.
//
// A nil binding renders as "" rather than "null", matching PHP's stringify().
func stringifyParameter(value driver.Value) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case []byte:
		return string(v)
	case bool:
		return strconv.FormatBool(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64)
	case time.Time:
		return v.Format(time.RFC3339Nano)
	default:
		encoded, err := json.Marshal(v)
		if err != nil {
			return ""
		}
		return string(encoded)
	}
}
