package migrate

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type VersionTableManager struct {
	TableName string
}

func (m *VersionTableManager) GetCurrentVersion(ctx context.Context, conn *pgx.Conn) (int, error) {
	exists, err := m.CheckExists(ctx, conn)
	if err != nil {
		return 0, err
	}
	if !exists {
		return -1, nil
	}

	var v int
	var finished_at *time.Time
	var down_started_at *time.Time

	sql := fmt.Sprintf(
		`select version, finished_at, down_started_at from %s
order by version desc
limit 1`,
		m.TableName,
	)
	if err := conn.QueryRow(ctx, sql).Scan(&v, &finished_at, &down_started_at); err != nil {
		return v, err
	}

	if finished_at == nil || finished_at.IsZero() {
		return v, fmt.Errorf("migration %d up was never finished", v)
	}
	if down_started_at != nil && !down_started_at.IsZero() {
		return v, fmt.Errorf("migration %d down was never finished", v)
	}

	return v, nil
}

func (m *VersionTableManager) CheckExists(ctx context.Context, conn *pgx.Conn) (ok bool, err error) {
	var count int
	if i := strings.IndexByte(m.TableName, '.'); i == -1 {
		err = conn.QueryRow(ctx, "select count(*) from pg_catalog.pg_class where relname=$1 and relkind='r' and pg_table_is_visible(oid)", m.TableName).Scan(&count)
	} else {
		schema, table := m.TableName[:i], m.TableName[i+1:]
		err = conn.QueryRow(ctx, "select count(*) from pg_catalog.pg_tables where schemaname=$1 and tablename=$2", schema, table).Scan(&count)
	}
	return count > 0, err
}

func (m *VersionTableManager) Migration() Migration {
	var upSql strings.Builder
	if i := strings.IndexByte(m.TableName, '.'); i > 0 {
		schema := m.TableName[:i]
		fmt.Fprintf(&upSql, `create schema if not exists %s; `, schema)
	}
	fmt.Fprintf(
		&upSql,
		`create table %s (
	version int4 not null primary key check (version >= 0),
	started_at timestamptz not null default now(),
	finished_at timestamptz,
	down_started_at timestamptz
);`,
		m.TableName,
	)

	return Migration{
		Meta: Meta{
			Version: 0,
			Name:    "version_table",
		},
		Up: Step{
			Name: "up",
			SQL:  upSql.String(),
		},
		Down: Step{
			Name: "down",
			SQL:  fmt.Sprintf(`drop table %s;`, m.TableName),
		},
	}
}

func (m *VersionTableManager) InsertVersionUp(ctx context.Context, conn *pgx.Conn, version int) error {
	sql := fmt.Sprintf(`insert into %s (version, started_at, finished_at) values ($1, now(), clock_timestamp());`, m.TableName)
	_, err := conn.Exec(ctx, sql, version)
	return err
}

func (m *VersionTableManager) StartVersionUp(ctx context.Context, conn *pgx.Conn, version int) error {
	sql := fmt.Sprintf(`insert into %s (version, started_at) values ($1, clock_timestamp());`, m.TableName)
	_, err := conn.Exec(ctx, sql, version)
	return err
}

func (m *VersionTableManager) FinishVersionUp(ctx context.Context, conn *pgx.Conn, version int) error {
	sql := fmt.Sprintf(`update %s set finished_at = clock_timestamp() where version = $1`, m.TableName)
	_, err := conn.Exec(ctx, sql, version)
	return err
}

func (m *VersionTableManager) StartVersionDown(ctx context.Context, conn *pgx.Conn, version int) error {
	sql := fmt.Sprintf(`update %s set down_started_at = clock_timestamp() where version = $1`, m.TableName)
	_, err := conn.Exec(ctx, sql, version)
	return err
}

func (m *VersionTableManager) FinishVersionDown(ctx context.Context, conn *pgx.Conn, version int) error {
	sql := fmt.Sprintf(`delete from %s where version = $1`, m.TableName)
	_, err := conn.Exec(ctx, sql, version)
	return err
}
