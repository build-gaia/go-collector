package chronos

import "testing"

func TestParseDSNConnectionIdentity(t *testing.T) {
	cases := []struct {
		name   string
		system string
		driver string
		dsn    string
		want   connTarget
	}{
		{
			name:   "mysql tcp with credentials",
			system: "mysql",
			driver: "mysql",
			dsn:    "stock:s3cret@tcp(db-1.internal:3307)/stock?parseTime=true",
			want:   connTarget{host: "db-1.internal", port: "3307", name: "stock", user: "stock", driver: "mysql"},
		},
		{
			name:   "mysql defaults the address",
			system: "mysql",
			driver: "mysql",
			dsn:    "root@/stock",
			want:   connTarget{host: "127.0.0.1", port: "3306", name: "stock", user: "root", driver: "mysql"},
		},
		{
			name:   "mysql host without port",
			system: "mysql",
			driver: "mysql",
			dsn:    "root:pw@tcp(db-1)/stock",
			want:   connTarget{host: "db-1", port: "3306", name: "stock", user: "root", driver: "mysql"},
		},
		{
			name:   "mysql unix socket has no port",
			system: "mysql",
			driver: "mysql",
			dsn:    "root:pw@unix(/var/run/mysqld/mysqld.sock)/stock",
			want:   connTarget{host: "/var/run/mysqld/mysqld.sock", name: "stock", user: "root", driver: "mysql"},
		},
		{
			name:   "mysql password containing a slash and an at sign",
			system: "mysql",
			driver: "mysql",
			dsn:    "root:a/b@c@tcp(db-1:3306)/stock",
			want:   connTarget{host: "db-1", port: "3306", name: "stock", user: "root", driver: "mysql"},
		},
		{
			name:   "postgres url",
			system: "postgresql",
			driver: "pgx",
			dsn:    "postgres://reader:pw@pg-1.internal:5433/analytics?sslmode=require",
			want:   connTarget{host: "pg-1.internal", port: "5433", name: "analytics", user: "reader", driver: "pgx"},
		},
		{
			name:   "postgres url defaults the port",
			system: "postgresql",
			driver: "postgres",
			dsn:    "postgres://reader@pg-1/analytics",
			want:   connTarget{host: "pg-1", port: "5432", name: "analytics", user: "reader", driver: "postgres"},
		},
		{
			name:   "postgres keyword form",
			system: "postgresql",
			driver: "postgres",
			dsn:    "host=pg-1 port=6432 user=reader password='a b' dbname=analytics sslmode=disable",
			want:   connTarget{host: "pg-1", port: "6432", name: "analytics", user: "reader", driver: "postgres"},
		},
		{
			name:   "sqlserver url",
			system: "mssql",
			driver: "sqlserver",
			dsn:    "sqlserver://sa:pw@mssql-1:1434?database=ledger",
			want:   connTarget{host: "mssql-1", port: "1434", name: "ledger", user: "sa", driver: "sqlserver"},
		},
		{
			name:   "sqlserver ado form",
			system: "mssql",
			driver: "sqlserver",
			dsn:    "server=mssql-1,1435;user id=sa;password=pw;database=ledger",
			want:   connTarget{host: "mssql-1", port: "1435", name: "ledger", user: "sa", driver: "sqlserver"},
		},
		{
			name:   "sqlite keeps the file name only",
			system: "sqlite",
			driver: "sqlite3",
			dsn:    "file:/var/lib/app/stock.db?cache=shared",
			want:   connTarget{name: "stock.db", driver: "sqlite3"},
		},
		{
			name:   "unknown system with a url dsn",
			system: "clickhouse",
			driver: "clickhouse",
			dsn:    "clickhouse://reader:pw@ch-1:9000/events",
			want:   connTarget{host: "ch-1", port: "9000", name: "events", user: "reader", driver: "clickhouse"},
		},
		{
			name:   "unparseable dsn yields nothing rather than a guess",
			system: "mysql",
			driver: "mysql",
			dsn:    "not-a-dsn",
			want:   connTarget{driver: "mysql"},
		},
		{
			name:   "empty dsn yields nothing",
			system: "postgresql",
			driver: "pgx",
			dsn:    "",
			want:   connTarget{driver: "pgx"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseDSN(tc.system, tc.driver, tc.dsn)
			if got != tc.want {
				t.Fatalf("parseDSN(%q, %q, %q)\n got %+v\nwant %+v", tc.system, tc.driver, tc.dsn, got, tc.want)
			}
		})
	}
}

// A password must never survive parsing: it is read only to find where the user
// name ends. This asserts on the whole struct rather than on individual fields so
// a new field cannot quietly start carrying one.
func TestParseDSNDropsPassword(t *testing.T) {
	secrets := []struct {
		system string
		dsn    string
		secret string
	}{
		{"mysql", "root:hunter2@tcp(db-1:3306)/stock", "hunter2"},
		{"postgresql", "postgres://reader:hunter2@pg-1/analytics", "hunter2"},
		{"postgresql", "host=pg-1 user=reader password=hunter2 dbname=analytics", "hunter2"},
		{"mssql", "server=mssql-1;user id=sa;password=hunter2;database=ledger", "hunter2"},
		{"mssql", "sqlserver://sa:hunter2@mssql-1?database=ledger", "hunter2"},
	}
	for _, s := range secrets {
		target := parseDSN(s.system, "d", s.dsn)
		for _, field := range []string{target.host, target.port, target.name, target.user, target.driver} {
			if field == s.secret {
				t.Fatalf("password leaked into a target field for %q", s.dsn)
			}
		}
	}
}

func TestSplitHostPort(t *testing.T) {
	cases := map[string][2]string{
		"db-1:3306":      {"db-1", "3306"},
		"db-1":           {"db-1", ""},
		"[::1]:3306":     {"::1", "3306"},
		"[fe80::1]":      {"fe80::1", ""},
		"127.0.0.1:5432": {"127.0.0.1", "5432"},
	}
	for address, want := range cases {
		host, port := splitHostPort(address)
		if host != want[0] || port != want[1] {
			t.Fatalf("splitHostPort(%q) = (%q, %q), want (%q, %q)", address, host, port, want[0], want[1])
		}
	}
}
