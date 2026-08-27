package chronos

import (
	"net/url"
	"path"
	"strings"
)

// connTarget is the connection identity behind a statement span: which server,
// which schema, as whom. The engine's data-source detection sweep reads exactly
// these attributes off recent spans to mint a source it can later run EXPLAIN
// against, so a statement span without them is invisible to plan capture — which
// is why the PHP collector has always tagged them from the framework's own
// connection config (see Laravel RichTelemetryHooks::addConnectionMetadata and
// Symfony1 DoctrineSpanListener).
//
// Go has no framework to ask, but it has something PHP does not: the DSN passed
// to chronos.Open. Every field here comes from that string. The password is
// parsed only to be discarded — it is never held on the struct, so it cannot
// reach a span.
type connTarget struct {
	host   string
	port   string
	name   string
	user   string
	driver string
}

func (t connTarget) empty() bool {
	return t.host == "" && t.port == "" && t.name == "" && t.user == ""
}

// parseDSN reads connection identity out of a driver DSN. An unrecognised or
// malformed DSN yields a zero target rather than a guess: a wrong host mints a
// data source the operator then has to disown, which is worse than no host.
func parseDSN(system, driverName, dsn string) connTarget {
	target := parseDSNTarget(system, dsn)
	target.driver = strings.TrimSpace(driverName)
	return target
}

func parseDSNTarget(system, dsn string) connTarget {
	dsn = strings.TrimSpace(dsn)
	if dsn == "" {
		return connTarget{}
	}
	switch system {
	case "mysql":
		return parseMySQLDSN(dsn)
	case "postgresql":
		if isURLDSN(dsn) {
			return parseURLDSN(dsn, "5432", "")
		}
		return parseKeywordDSN(dsn, "5432")
	case "mssql":
		if isURLDSN(dsn) {
			return parseURLDSN(dsn, "1433", "database")
		}
		return parseSemicolonDSN(dsn, "1433")
	case "sqlite":
		return connTarget{name: sqliteName(dsn)}
	default:
		if isURLDSN(dsn) {
			return parseURLDSN(dsn, "", "")
		}
		return connTarget{}
	}
}

func isURLDSN(dsn string) bool {
	i := strings.Index(dsn, "://")
	return i > 0 && !strings.ContainsAny(dsn[:i], " ;=")
}

// parseURLDSN handles the `scheme://user:pass@host:port/dbname?params` family
// (lib/pq, pgx, sqlserver). databaseParam names the query parameter carrying the
// schema when it is not in the path, as it is for sqlserver.
func parseURLDSN(dsn, defaultPort, databaseParam string) connTarget {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return connTarget{}
	}
	target := connTarget{host: parsed.Hostname(), port: parsed.Port()}
	if target.port == "" && target.host != "" {
		target.port = defaultPort
	}
	if parsed.User != nil {
		target.user = parsed.User.Username()
	}
	target.name = strings.TrimPrefix(parsed.Path, "/")
	if databaseParam != "" {
		if value := parsed.Query().Get(databaseParam); value != "" {
			target.name = value
		}
	}
	return target
}

// parseKeywordDSN handles libpq's space-separated `host=... dbname=...` form.
// Single-quoted values are supported because libpq allows them for values
// containing spaces.
func parseKeywordDSN(dsn, defaultPort string) connTarget {
	target := connTarget{}
	for key, value := range keywordPairs(dsn, ' ') {
		switch key {
		case "host", "hostaddr":
			if target.host == "" {
				target.host = value
			}
		case "port":
			target.port = value
		case "user":
			target.user = value
		case "dbname", "database":
			target.name = value
		}
	}
	if target.port == "" && target.host != "" {
		target.port = defaultPort
	}
	return target
}

// parseSemicolonDSN handles the ADO form the sqlserver driver also accepts:
// `server=host\instance;user id=sa;database=master`.
func parseSemicolonDSN(dsn, defaultPort string) connTarget {
	target := connTarget{}
	for key, value := range keywordPairs(dsn, ';') {
		switch key {
		case "server", "address", "data source", "host":
			host, instancePort := splitServerInstance(value)
			target.host = host
			if instancePort != "" {
				target.port = instancePort
			}
		case "port":
			target.port = value
		case "user id", "uid", "user":
			target.user = value
		case "database", "initial catalog":
			target.name = value
		}
	}
	if target.port == "" && target.host != "" {
		target.port = defaultPort
	}
	return target
}

// splitServerInstance splits `host,1434` (the ADO port spelling) off a server
// value. A `host\instance` name keeps the instance: the instance is part of the
// server's identity and the port is resolved by the browser service.
func splitServerInstance(value string) (host, port string) {
	if i := strings.LastIndex(value, ","); i >= 0 {
		return strings.TrimSpace(value[:i]), strings.TrimSpace(value[i+1:])
	}
	return strings.TrimSpace(value), ""
}

// keywordPairs splits a `key=value` list on sep, lowercasing keys and stripping
// the quotes libpq and ADO both allow around values.
func keywordPairs(dsn string, sep rune) map[string]string {
	pairs := map[string]string{}
	for _, field := range splitUnquoted(dsn, sep) {
		i := strings.IndexByte(field, '=')
		if i <= 0 {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(field[:i]))
		value := strings.TrimSpace(field[i+1:])
		value = strings.Trim(value, "'\"")
		if key == "" || value == "" {
			continue
		}
		if _, seen := pairs[key]; !seen {
			pairs[key] = value
		}
	}
	return pairs
}

func splitUnquoted(s string, sep rune) []string {
	fields := []string{}
	quote := rune(0)
	current := strings.Builder{}
	for _, r := range s {
		switch {
		case quote != 0:
			if r == quote {
				quote = 0
			}
			current.WriteRune(r)
		case r == '\'' || r == '"':
			quote = r
			current.WriteRune(r)
		case r == sep:
			fields = append(fields, current.String())
			current.Reset()
		default:
			current.WriteRune(r)
		}
	}
	fields = append(fields, current.String())
	return fields
}

// parseMySQLDSN handles go-sql-driver's
// `user:pass@protocol(address)/dbname?params`. The split is on the LAST slash,
// as the driver's own ParseDSN does, so a password or socket path containing a
// slash does not fool it.
func parseMySQLDSN(dsn string) connTarget {
	slash := strings.LastIndex(dsn, "/")
	if slash < 0 {
		return connTarget{}
	}
	before, after := dsn[:slash], dsn[slash+1:]

	target := connTarget{name: after}
	if i := strings.IndexByte(target.name, '?'); i >= 0 {
		target.name = target.name[:i]
	}

	if at := strings.LastIndex(before, "@"); at >= 0 {
		credentials := before[:at]
		before = before[at+1:]
		if colon := strings.IndexByte(credentials, ':'); colon >= 0 {
			target.user = credentials[:colon]
		} else {
			target.user = credentials
		}
	}

	protocol, address := before, ""
	if open := strings.IndexByte(before, '('); open >= 0 && strings.HasSuffix(before, ")") {
		protocol, address = before[:open], before[open+1:len(before)-1]
	}
	switch strings.ToLower(protocol) {
	case "unix":
		// A socket path is the server's identity here; there is no port.
		target.host = address
	default: // tcp, and the empty protocol the driver defaults to tcp
		if address == "" {
			address = "127.0.0.1:3306"
		}
		target.host, target.port = splitHostPort(address)
		if target.port == "" {
			target.port = "3306"
		}
	}
	return target
}

// splitHostPort splits `host:port`, tolerating a bracketed IPv6 literal and a
// bare host with no port. net.SplitHostPort is not used because it errors on the
// portless form, which is the common one.
func splitHostPort(address string) (host, port string) {
	if strings.HasPrefix(address, "[") {
		if end := strings.Index(address, "]"); end >= 0 {
			host = address[1 : end+1-1]
			rest := address[end+1:]
			return host, strings.TrimPrefix(rest, ":")
		}
	}
	if i := strings.LastIndex(address, ":"); i >= 0 && !strings.Contains(address[i+1:], ":") {
		return address[:i], address[i+1:]
	}
	return address, ""
}

// sqliteName is the database file, basename only. The full path is a local
// filesystem detail and the file name is the identity a datastore node reads by.
func sqliteName(dsn string) string {
	trimmed := strings.TrimPrefix(dsn, "file:")
	if i := strings.IndexByte(trimmed, '?'); i >= 0 {
		trimmed = trimmed[:i]
	}
	if trimmed == "" || trimmed == ":memory:" {
		return trimmed
	}
	return path.Base(trimmed)
}
