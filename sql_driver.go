package chronos

import (
	"context"
	"database/sql/driver"
	"errors"
	"sync"
)

type sqlDriver struct {
	parent driver.Driver
	client *Client
	name   string
	system string

	targetsMu sync.Mutex
	targets   map[string]connTarget
}

func (d *sqlDriver) Open(name string) (driver.Conn, error) {
	conn, err := d.parent.Open(name)
	if err != nil {
		return nil, err
	}
	return wrapSQLConn(conn, d, d.target(name)), nil
}

func (d *sqlDriver) OpenConnector(name string) (driver.Connector, error) {
	if dc, ok := d.parent.(driver.DriverContext); ok {
		c, err := dc.OpenConnector(name)
		if err != nil {
			return nil, err
		}
		return &sqlConnector{parent: c, drv: d, target: d.target(name)}, nil
	}
	return &dsnConnector{dsn: name, drv: d}, nil
}

type sqlConnector struct {
	parent driver.Connector
	drv    *sqlDriver
	target connTarget
}

func (c *sqlConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.parent.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return wrapSQLConn(conn, c.drv, c.target), nil
}

func (c *sqlConnector) Driver() driver.Driver { return c.drv }

type dsnConnector struct {
	dsn string
	drv *sqlDriver
}

func (c *dsnConnector) Connect(_ context.Context) (driver.Conn, error) {
	return c.drv.Open(c.dsn)
}

func (c *dsnConnector) Driver() driver.Driver { return c.drv }

type sqlConn struct {
	parent driver.Conn
	drv    *sqlDriver
	target connTarget
}

func wrapSQLConn(parent driver.Conn, drv *sqlDriver, target connTarget) driver.Conn {
	return &sqlConn{parent: parent, drv: drv, target: target}
}

func (c *sqlConn) Prepare(query string) (driver.Stmt, error) {
	stmt, err := c.parent.Prepare(query)
	if err != nil {
		return nil, err
	}
	return wrapSQLStmt(stmt, query, c.drv, c.target), nil
}

func (c *sqlConn) Close() error { return c.parent.Close() }

func (c *sqlConn) Begin() (driver.Tx, error) {
	tx, err := c.parent.Begin()
	if err != nil {
		return nil, err
	}
	return &sqlTx{parent: tx}, nil
}

func (c *sqlConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if b, ok := c.parent.(driver.ConnBeginTx); ok {
		tx, err := b.BeginTx(ctx, opts)
		if err != nil {
			return nil, err
		}
		return &sqlTx{parent: tx}, nil
	}
	return c.Begin()
}

func (c *sqlConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if p, ok := c.parent.(driver.ConnPrepareContext); ok {
		stmt, err := p.PrepareContext(ctx, query)
		if err != nil {
			return nil, err
		}
		return wrapSQLStmt(stmt, query, c.drv, c.target), nil
	}
	return c.Prepare(query)
}

func (c *sqlConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if e, ok := c.parent.(driver.ExecerContext); ok {
		_, op := c.drv.startSQLSpan(ctx, query, c.target, namedParams(args))
		res, err := e.ExecContext(ctx, query, args)
		if errors.Is(err, driver.ErrSkip) {
			abandonSpan(op)
		} else {
			finishSQLSpan(op, err)
			return res, err
		}
	}
	if e, ok := c.parent.(driver.Execer); ok {
		vals, err := namedToValues(args)
		if err != nil {
			return nil, err
		}
		_, op := c.drv.startSQLSpan(ctx, query, c.target, namedParams(args))
		res, err := e.Exec(query, vals)
		if errors.Is(err, driver.ErrSkip) {
			abandonSpan(op)
		} else {
			finishSQLSpan(op, err)
			return res, err
		}
	}
	return nil, driver.ErrSkip
}

func (c *sqlConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if q, ok := c.parent.(driver.QueryerContext); ok {
		_, op := c.drv.startSQLSpan(ctx, query, c.target, namedParams(args))
		rows, err := q.QueryContext(ctx, query, args)
		if errors.Is(err, driver.ErrSkip) {
			abandonSpan(op)
		} else {
			finishSQLSpan(op, err)
			return rows, err
		}
	}
	if q, ok := c.parent.(driver.Queryer); ok {
		vals, err := namedToValues(args)
		if err != nil {
			return nil, err
		}
		_, op := c.drv.startSQLSpan(ctx, query, c.target, namedParams(args))
		rows, err := q.Query(query, vals)
		if errors.Is(err, driver.ErrSkip) {
			abandonSpan(op)
		} else {
			finishSQLSpan(op, err)
			return rows, err
		}
	}
	return nil, driver.ErrSkip
}

func (c *sqlConn) Ping(ctx context.Context) error {
	if p, ok := c.parent.(driver.Pinger); ok {
		return p.Ping(ctx)
	}
	return nil
}

func (c *sqlConn) ResetSession(ctx context.Context) error {
	if r, ok := c.parent.(driver.SessionResetter); ok {
		return r.ResetSession(ctx)
	}
	return nil
}

func (c *sqlConn) IsValid() bool {
	if v, ok := c.parent.(driver.Validator); ok {
		return v.IsValid()
	}
	return true
}

func (c *sqlConn) CheckNamedValue(nv *driver.NamedValue) error {
	if checker, ok := c.parent.(driver.NamedValueChecker); ok {
		return checker.CheckNamedValue(nv)
	}
	return driver.ErrSkip
}

type sqlStmt struct {
	parent driver.Stmt
	query  string
	drv    *sqlDriver
	target connTarget
}

func wrapSQLStmt(parent driver.Stmt, query string, drv *sqlDriver, target connTarget) driver.Stmt {
	return &sqlStmt{parent: parent, query: query, drv: drv, target: target}
}

func (s *sqlStmt) Close() error  { return s.parent.Close() }
func (s *sqlStmt) NumInput() int { return s.parent.NumInput() }

func (s *sqlStmt) Exec(args []driver.Value) (driver.Result, error) {
	_, op := s.drv.startSQLSpan(context.Background(), s.query, s.target, valueParams(args))
	res, err := s.parent.Exec(args)
	finishSQLSpan(op, err)
	return res, err
}

func (s *sqlStmt) Query(args []driver.Value) (driver.Rows, error) {
	_, op := s.drv.startSQLSpan(context.Background(), s.query, s.target, valueParams(args))
	rows, err := s.parent.Query(args)
	finishSQLSpan(op, err)
	return rows, err
}

func (s *sqlStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	_, op := s.drv.startSQLSpan(ctx, s.query, s.target, namedParams(args))
	var (
		res driver.Result
		err error
	)
	defer func() { finishSQLSpan(op, err) }()

	if e, ok := s.parent.(driver.StmtExecContext); ok {
		res, err = e.ExecContext(ctx, args)
		return res, err
	}
	vals, convErr := namedToValues(args)
	if convErr != nil {
		err = convErr
		return nil, err
	}
	res, err = s.parent.Exec(vals)
	return res, err
}

func (s *sqlStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	_, op := s.drv.startSQLSpan(ctx, s.query, s.target, namedParams(args))
	var (
		rows driver.Rows
		err  error
	)
	defer func() { finishSQLSpan(op, err) }()

	if q, ok := s.parent.(driver.StmtQueryContext); ok {
		rows, err = q.QueryContext(ctx, args)
		return rows, err
	}
	vals, convErr := namedToValues(args)
	if convErr != nil {
		err = convErr
		return nil, err
	}
	rows, err = s.parent.Query(vals)
	return rows, err
}

type sqlTx struct {
	parent driver.Tx
}

func (t *sqlTx) Commit() error   { return t.parent.Commit() }
func (t *sqlTx) Rollback() error { return t.parent.Rollback() }

func namedToValues(args []driver.NamedValue) ([]driver.Value, error) {
	vals := make([]driver.Value, len(args))
	for i, a := range args {
		if len(a.Name) > 0 {
			return nil, errors.New("chronos: named parameters require a driver that implements NamedValueChecker / ExecerContext")
		}
		vals[i] = a.Value
	}
	return vals, nil
}
