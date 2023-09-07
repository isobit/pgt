package migrate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/Masterminds/sprig/v3"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/isobit/pgt/migrate/internal/sqlsplit"
	"github.com/isobit/pgt/util"
)

var (
	migrationPattern     = regexp.MustCompile(`\A(\d+)_.+\.sql\z`)
	disableTxPattern     = regexp.MustCompile(`(?m)^--pgt:disable-txn$`)
	createAboveDropBelow = "--pgt:down"
)

var ErrNoFwMigration = errors.New("no sql in forward migration step")

type BadVersionError string

func (e BadVersionError) Error() string {
	return string(e)
}

type IrreversibleMigrationError struct {
	m *Migration
}

func (e IrreversibleMigrationError) Error() string {
	return fmt.Sprintf("irreversible migration: %d - %s", e.m.Sequence, e.m.Name)
}

type NoMigrationsFoundError struct{}

func (e NoMigrationsFoundError) Error() string {
	return "migrations not found"
}

type MigrationPgError struct {
	MigrationName string
	Sql           string
	*pgconn.PgError
}

func (e MigrationPgError) Error() string {
	if e.MigrationName == "" {
		return e.PgError.Error()
	}
	if ex, err := ExtractErrorLine(e.Sql, int(e.Position)); err == nil {
		return fmt.Sprintf("%s:%d:%d %s", e.MigrationName, ex.LineNum, ex.ColumnNum, e.PgError.Error())
	}
	return fmt.Sprintf("%s: %s", e.MigrationName, e.PgError.Error())
}

func (e MigrationPgError) Unwrap() error {
	return e.PgError
}

type Migration struct {
	Sequence int32
	Name     string
	UpSQL    string
	DownSQL  string
}

type MigratorOptions struct {
	// DisableTx causes the Migrator not to run migrations in a transaction.
	DisableTx bool
}

type Migrator struct {
	conn            *pgx.Conn
	versionTable    string
	options         *MigratorOptions
	Migrations      []*Migration
	BeforeMigration func(int32, string, string, string) error // OnStart is called when a migration is run with the sequence, name, direction, and SQL
	Data            map[string]interface{}                    // Data available to use in migrations
}

// NewMigrator initializes a new Migrator. It is highly recommended that versionTable be schema qualified.
func NewMigrator(ctx context.Context, conn *pgx.Conn, versionTable string) (m *Migrator, err error) {
	return NewMigratorEx(ctx, conn, versionTable, &MigratorOptions{})
}

// NewMigratorEx initializes a new Migrator. It is highly recommended that versionTable be schema qualified.
func NewMigratorEx(ctx context.Context, conn *pgx.Conn, versionTable string, opts *MigratorOptions) (m *Migrator, err error) {
	m = &Migrator{conn: conn, versionTable: versionTable, options: opts}
	m.Migrations = []*Migration{
		m.migrationZero(),
	}
	m.Data = map[string]interface{}{}
	return
}

// FindMigrations finds all migration files in fsys.
func FindMigrations(fsys fs.FS) ([]string, error) {
	fileInfos, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, err
	}

	paths := make([]string, 0, len(fileInfos))

	for _, fi := range fileInfos {
		if fi.IsDir() {
			continue
		}

		matches := migrationPattern.FindStringSubmatch(fi.Name())
		if len(matches) != 2 {
			continue
		}

		n, err := strconv.ParseInt(matches[1], 10, 32)
		if err != nil {
			// The regexp already validated that the prefix is all digits so this *should* never fail
			return nil, err
		}

		if n-1 < int64(len(paths)) && paths[n-1] != "" {
			return nil, fmt.Errorf("duplicate migration %d", n)
		}

		// Set at specific index, so that paths are properly sorted
		paths = setAt(paths, fi.Name(), n-1)
	}

	for i, path := range paths {
		if path == "" {
			return nil, fmt.Errorf("missing migration %d", i+1)
		}
	}

	return paths, nil
}

func (m *Migrator) LoadMigrations(fsys fs.FS) error {
	mainTmpl := template.New("main").Funcs(sprig.TxtFuncMap()).Funcs(
		template.FuncMap{
			"install_snapshot": func(name string) (string, error) {
				codePackageFSys, err := fs.Sub(fsys, filepath.Join("snapshots", name))
				if err != nil {
					return "", err
				}
				codePackage, err := LoadCodePackage(codePackageFSys)
				if err != nil {
					return "", err
				}

				return codePackage.Eval(m.Data)
			},
		},
	)

	sharedPaths, err := fs.Glob(fsys, filepath.Join("*", "*.sql"))
	if err != nil {
		return err
	}

	for _, p := range sharedPaths {
		body, err := fs.ReadFile(fsys, p)
		if err != nil {
			return err
		}

		_, err = mainTmpl.New(p).Parse(string(body))
		if err != nil {
			return err
		}
	}

	paths, err := FindMigrations(fsys)
	if err != nil {
		return err
	}

	if len(paths) == 0 {
		return NoMigrationsFoundError{}
	}

	for _, p := range paths {
		body, err := fs.ReadFile(fsys, p)
		if err != nil {
			return err
		}

		pieces := strings.SplitN(string(body), createAboveDropBelow, 2)
		var upSQL, downSQL string
		upSQL = strings.TrimSpace(pieces[0])
		upSQL, err = m.evalMigration(mainTmpl.New(filepath.Base(p)+" up"), upSQL)
		if err != nil {
			return err
		}
		// Make sure there is SQL in the forward migration step.
		containsSQL := false
		for _, v := range strings.Split(upSQL, "\n") {
			// Only account for regular single line comment, empty line and space/comment combination
			cleanString := strings.TrimSpace(v)
			if len(cleanString) != 0 &&
				!strings.HasPrefix(cleanString, "--") {
				containsSQL = true
				break
			}
		}
		if !containsSQL {
			return ErrNoFwMigration
		}

		if len(pieces) == 2 {
			downSQL = strings.TrimSpace(pieces[1])
			downSQL, err = m.evalMigration(mainTmpl.New(filepath.Base(p)+" down"), downSQL)
			if err != nil {
				return err
			}
		}

		m.AppendMigration(filepath.Base(p), upSQL, downSQL)
	}

	return nil
}

func (m *Migrator) evalMigration(tmpl *template.Template, sql string) (string, error) {
	tmpl, err := tmpl.Parse(sql)
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer
	err = tmpl.Execute(&buf, m.Data)
	if err != nil {
		return "", err
	}

	return buf.String(), nil
}

func (m *Migrator) AppendMigration(name, upSQL, downSQL string) {
	sequence := int32(len(m.Migrations))
	m.Migrations = append(
		m.Migrations,
		&Migration{
			Sequence: sequence,
			Name:     name,
			UpSQL:    upSQL,
			DownSQL:  downSQL,
		})
	return
}

// Migrate runs pending migrations
// It calls m.OnStart when it begins a migration
func (m *Migrator) Migrate(ctx context.Context) error {
	return m.MigrateTo(ctx, int32(len(m.Migrations)-1))
}

// Lock to ensure multiple migrations cannot occur simultaneously
const lockNum = uint64(0x49D7B5E9559EB483)

// Constant above is arbitrary random number; can be regenerated with
// echo "obase=16;ibase=16;$(openssl rand -hex 8 | tr '[:lower:]' '[:upper:]') - 7FFFFFFFFFFFFFFF" | bc

func acquireAdvisoryLock(ctx context.Context, conn *pgx.Conn) error {
	util.Logf(3, "acquiring advisory lock")
	var acquired bool
	if err := conn.QueryRow(ctx, "select pg_try_advisory_lock($1);", lockNum).Scan(&acquired); err != nil {
		return err
	}
	if acquired {
		return nil
	}
	util.Logf(-1, "run this to unlock if last migration crashed: select pg_advisory_unlock(%d)", lockNum)
	return fmt.Errorf("failed to acquire advisory lock; is another migration being run?")
}

func releaseAdvisoryLock(ctx context.Context, conn *pgx.Conn) error {
	util.Logf(3, "releasing advisory lock")
	_, err := conn.Exec(ctx, "select pg_advisory_unlock($1)", lockNum)
	return err
}

// MigrateTo migrates to targetVersion
func (m *Migrator) MigrateTo(ctx context.Context, targetVersion int32) (err error) {
	err = acquireAdvisoryLock(ctx, m.conn)
	if err != nil {
		return err
	}
	defer func() {
		unlockErr := releaseAdvisoryLock(ctx, m.conn)
		if err == nil && unlockErr != nil {
			err = unlockErr
		}
	}()

	currentVersion, err := m.GetCurrentVersion(ctx)
	if err != nil {
		return err
	}

	if targetVersion < -1 || targetVersion >= int32(len(m.Migrations)) {
		errMsg := fmt.Sprintf("destination version %d is outside the valid versions of 0 to %d", targetVersion, len(m.Migrations))
		return BadVersionError(errMsg)
	}

	if currentVersion < -1 || currentVersion >= int32(len(m.Migrations)) {
		errMsg := fmt.Sprintf("current version %d is outside the valid versions of -1 to %d", currentVersion, len(m.Migrations))
		return BadVersionError(errMsg)
	}

	var direction int32
	if currentVersion < targetVersion {
		direction = 1
	} else {
		direction = -1
	}

	for currentVersion != targetVersion {
		var current *Migration
		var sql, directionName string
		var sequence int32
		if direction == 1 {
			current = m.Migrations[currentVersion+1]
			sequence = current.Sequence
			sql = current.UpSQL
			directionName = "up"
		} else {
			current = m.Migrations[currentVersion]
			sequence = current.Sequence - 1
			sql = current.DownSQL
			directionName = "down"
			if current.DownSQL == "" {
				return IrreversibleMigrationError{m: current}
			}
		}

		useTx := currentVersion == -1 || !m.options.DisableTx
		var sqlStatements []string
		if disableTxPattern.MatchString(sql) {
			useTx = false
			sql = disableTxPattern.ReplaceAllLiteralString(sql, "")
		}

		if m.BeforeMigration != nil {
			if err := m.BeforeMigration(current.Sequence, current.Name, directionName, sql); err != nil {
				return err
			}
		}

		if useTx {
			sqlStatements = []string{sql}
		} else {
			sqlStatements = sqlsplit.Split(sql)
		}

		var tx pgx.Tx
		if useTx {
			tx, err = m.conn.Begin(ctx)
			if err != nil {
				return err
			}
			defer tx.Rollback(ctx)
		} else {
			if direction == 1 {
				_, err := m.conn.Exec(ctx, fmt.Sprintf(`
				insert into %s(version, started_at)
				values ($1, now());
				`, m.versionTable), sequence)
				if err != nil {
					return err
				}
			} else {
				_, err = m.conn.Exec(ctx, fmt.Sprintf(`
				update %s set down_started_at = now()
				where version = $1
				`, m.versionTable), currentVersion)
				if err != nil {
					return err
				}
			}
		}

		// Execute the migration
		for _, statement := range sqlStatements {
			util.Logf(3, "exec: %s", statement)
			_, err = m.conn.Exec(ctx, statement)
			if err != nil {
				if err, ok := err.(*pgconn.PgError); ok {
					return MigrationPgError{MigrationName: current.Name, Sql: statement, PgError: err}
				}
				return err
			}
		}

		// Reset all database connection settings. Important to do before
		// updating version as search_path may have been changed.
		util.Logf(3, "exec: reset all")
		if _, err := m.conn.Exec(ctx, "reset all"); err != nil {
			return err
		}

		if useTx {
			// WIP lock reporting
			if false {
				var pid int
				if err := m.conn.QueryRow(ctx, `select pg_backend_pid()`).Scan(&pid); err != nil {
					return err
				}

				rows, err := m.conn.Query(ctx, `
				select coalesce(mode, ''), coalesce(nspname || '.' || relname, '')
				from pg_catalog.pg_locks
				join pg_catalog.pg_class on relation = pg_class.oid
				join pg_catalog.pg_namespace on relnamespace = pg_namespace.oid
				where pid = $1
					and locktype = 'relation'
					and pg_table_is_visible(pg_class.oid)
					and nspname != 'pg_catalog';
				`, pid)
				if err != nil {
					return err
				}

				type lock struct {
					Mode     string
					Relation string
				}
				locks, err := pgx.CollectRows(rows, pgx.RowToStructByPos[lock])
				if err != nil {
					return err
				}
				util.Logf(3, "%s locks: %v", current.Name, locks)
				for _, lock := range locks {
					switch lock.Mode {
					case "AccessExclusiveLock":
					case "ExclusiveLock":
					case "ShareRowExclusiveLock":
					case "ShareLock":
						util.Logf(-1, "%s takes aggressive lock: %s on %s", current.Name, lock.Mode, lock.Relation)
					}
				}
			}

			if direction == 1 {
				util.Logf(3, "exec: insert version %d", sequence)
				_, err = m.conn.Exec(ctx, fmt.Sprintf(`
				insert into %s(version, started_at, finished_at)
				values ($1, now(), clock_timestamp());
				`, m.versionTable), sequence)
				if err != nil {
					return err
				}
			} else if currentVersion > 0 {
				util.Logf(3, "exec: delete version %d", sequence)
				_, err = m.conn.Exec(ctx, fmt.Sprintf(`
				delete from %s where version = $1
				`, m.versionTable), currentVersion)
				if err != nil {
					return err
				}
			}

			err = tx.Commit(ctx)
			if err != nil {
				return err
			}
		} else {
			if direction == 1 {
				util.Logf(3, "exec: update version %d set finished", sequence)
				_, err = m.conn.Exec(ctx, fmt.Sprintf(`
				update %s set finished_at = now()
				where version = $1
				`, m.versionTable), sequence)
				if err != nil {
					return err
				}
			} else if currentVersion > 0 {
				util.Logf(3, "exec: delete version %d", sequence)
				_, err = m.conn.Exec(ctx, fmt.Sprintf(`
				delete from %s where version = $1
				`, m.versionTable), currentVersion)
				if err != nil {
					return err
				}
			}
		}

		currentVersion = currentVersion + direction
	}

	return nil
}

func (m *Migrator) GetCurrentVersion(ctx context.Context) (int32, error) {
	versionTableExists, err := m.versionTableExists(ctx)
	if err != nil {
		return 0, err
	}
	if !versionTableExists {
		return -1, nil
	}

	var v int32
	var finished_at *time.Time
	var down_started_at *time.Time
	if err := m.conn.QueryRow(ctx, fmt.Sprintf(`
	select version, finished_at, down_started_at from %s
	order by version desc
	limit 1
	`, m.versionTable)).Scan(&v, &finished_at, &down_started_at); err != nil {
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

func (m *Migrator) ForceSetCurrentVersion(ctx context.Context, v int32) (err error) {
	err = acquireAdvisoryLock(ctx, m.conn)
	if err != nil {
		return
	}
	defer func() {
		unlockErr := releaseAdvisoryLock(ctx, m.conn)
		if err == nil && unlockErr != nil {
			err = unlockErr
		}
	}()

	txn, err := m.conn.Begin(ctx)
	if err != nil {
		return
	}
	defer txn.Rollback(ctx)

	if _, err = txn.Exec(ctx, fmt.Sprintf("lock %s;", m.versionTable)); err != nil {
		return
	}

	if _, err = txn.Exec(ctx, fmt.Sprintf(`
	delete from %s
	where version >= $1;
	`, m.versionTable), v); err != nil {
		return
	}

	if _, err = txn.Exec(ctx, fmt.Sprintf(`
	insert into %s (version, started_at, finished_at)
	values ($1, now(), now())
	`, m.versionTable), v); err != nil {
		return
	}

	err = txn.Commit(ctx)

	return
}

func (m *Migrator) migrationZero() *Migration {
	var upSql strings.Builder
	if i := strings.IndexByte(m.versionTable, '.'); i > 0 {
		schema := m.versionTable[:i]
		fmt.Fprintf(&upSql, `create schema if not exists %s;`, schema)
	}
	fmt.Fprintf(
		&upSql,
		`
create table %s (
	version int4 not null primary key check (version >= 0),
	started_at timestamptz not null default now(),
	finished_at timestamptz,
	down_started_at timestamptz
);`,
		m.versionTable,
	)

	return &Migration{
		Sequence: 0,
		Name:     "version_table",
		UpSQL:    upSql.String(),
		DownSQL:  fmt.Sprintf(`drop table %s;`, m.versionTable),
	}
}

func (m *Migrator) versionTableExists(ctx context.Context) (ok bool, err error) {
	var count int
	if i := strings.IndexByte(m.versionTable, '.'); i == -1 {
		err = m.conn.QueryRow(ctx, "select count(*) from pg_catalog.pg_class where relname=$1 and relkind='r' and pg_table_is_visible(oid)", m.versionTable).Scan(&count)
	} else {
		schema, table := m.versionTable[:i], m.versionTable[i+1:]
		err = m.conn.QueryRow(ctx, "select count(*) from pg_catalog.pg_tables where schemaname=$1 and tablename=$2", schema, table).Scan(&count)
	}
	return count > 0, err
}

func setAt(strs []string, value string, pos int64) []string {
	// If pos > length - 1, append empty strings to make it the right size
	if pos > int64(len(strs))-1 {
		strs = append(strs, make([]string, 1+pos-int64(len(strs)))...)
	}
	strs[pos] = value
	return strs
}
