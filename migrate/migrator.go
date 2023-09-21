package migrate

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/isobit/pgt/migrate/sqlsplit"
	"github.com/isobit/pgt/util"
)

type Migrator struct {
	conn *pgx.Conn
	vtm  *VersionTableManager

	migrations []Migration

	BeforeExec func(meta Meta, step Step) error
}

func NewMigrator(conn *pgx.Conn, versionTableName string, migrations []Migration) *Migrator {
	vtm := &VersionTableManager{TableName: versionTableName}
	return &Migrator{
		conn: conn,
		vtm:  vtm,
		migrations: append(
			[]Migration{vtm.Migration()},
			migrations...,
		),
	}
}

func (m *Migrator) VersionRange() (int, int) {
	return -1, len(m.migrations) - 1
}

func (m *Migrator) CurrentVersion(ctx context.Context) (int, error) {
	return m.vtm.GetCurrentVersion(ctx, m.conn)
}

func (m *Migrator) Migration(version int) Migration {
	return m.migrations[version]
}

func (m *Migrator) MigrateTo(ctx context.Context, targetVersion int) (err error) {
	release, err := m.acquireLock(ctx)
	if err != nil {
		return err
	}
	defer release()

	minVersion, maxVersion := m.VersionRange()

	if targetVersion < minVersion || targetVersion > maxVersion {
		return fmt.Errorf("target version %d is outside the valid version range of %d to %d", targetVersion, minVersion, maxVersion)
	}

	currentVersion, err := m.CurrentVersion(ctx)
	if err != nil {
		return err
	}

	if currentVersion < minVersion || currentVersion > maxVersion {
		return fmt.Errorf("current version %d is outside the valid version range of %d to %d", currentVersion, minVersion, maxVersion)
	}

	down := targetVersion < currentVersion
	for currentVersion != targetVersion {
		if !down {
			currentVersion += 1
		}

		migration := m.migrations[currentVersion]

		if migration.Version != currentVersion {
			return fmt.Errorf("unexpected migration version %d at index %d", migration.Version, currentVersion)
		}

		if err := m.apply(ctx, migration, down); err != nil {
			return err
		}

		if down {
			currentVersion -= 1
		}
	}

	return nil
}

func (m *Migrator) ForceSkipTo(ctx context.Context, targetVersion int) (err error) {
	release, err := m.acquireLock(ctx)
	if err != nil {
		return err
	}
	defer release()

	minVersion, maxVersion := m.VersionRange()

	if targetVersion < minVersion || targetVersion > maxVersion {
		return fmt.Errorf("target version %d is outside the valid version range of %d to %d", targetVersion, minVersion, maxVersion)
	}

	currentVersion, err := m.CurrentVersion(ctx)
	if err != nil {
		return err
	}

	if currentVersion < minVersion || currentVersion > maxVersion {
		return fmt.Errorf("current version %d is outside the valid version range of %d to %d", currentVersion, minVersion, maxVersion)
	}

	down := targetVersion < currentVersion
	for currentVersion != targetVersion {
		if down {
			m.vtm.FinishVersionDown(ctx, m.conn, currentVersion)
			currentVersion -= 1
		} else {
			currentVersion += 1
			m.vtm.InsertVersionUp(ctx, m.conn, currentVersion)
		}
	}

	return nil
}

func (m *Migrator) apply(ctx context.Context, migration Migration, down bool) error {
	var step Step
	if down {
		step = migration.Down
		if step.SQL == "" {
			return fmt.Errorf("cannot apply down step for irreversible migration: %s", migration.Filename)
		}
	} else {
		step = migration.Up
	}

	if err := m.BeforeExec(migration.Meta, step); err != nil {
		return err
	}

	var tx pgx.Tx
	var sqlStatements []string
	if step.Config.DisableTransaction {
		sqlStatements = sqlsplit.Split(step.SQL)
	} else {
		sqlStatements = []string{step.SQL}

		var err error
		tx, err = m.conn.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
	}

	if tx == nil {
		if down {
			if err := m.vtm.StartVersionDown(ctx, m.conn, migration.Version); err != nil {
				return err
			}
		} else {
			if err := m.vtm.StartVersionUp(ctx, m.conn, migration.Version); err != nil {
				return err
			}
		}

	}

	statementPosOffset := 0
	for _, statement := range sqlStatements {
		util.Logf(3, "exec: %s", statement)
		if _, err := m.conn.Exec(ctx, statement); err != nil {
			if err, ok := err.(*pgconn.PgError); ok && err.Position > 0 {
				line, col := step.PositionToLineCol(int(err.Position) + statementPosOffset)
				return MigrationError{
					Err:       err,
					Meta:      migration.Meta,
					LineNum:   line,
					ColumnNum: col,
				}
			}
			return MigrationError{
				Err:  err,
				Meta: migration.Meta,
			}
		}
		statementPosOffset += len(statement) + 1
	}

	// Reset all database connection settings. Important to do before
	// updating version as search_path may have been changed.
	util.Logf(3, "exec: reset all")
	if _, err := m.conn.Exec(ctx, "reset all"); err != nil {
		return err
	}

	if down {
		if migration.Version > 0 {
			// FinishVersionDown works for both the tx and no-tx case.
			if err := m.vtm.FinishVersionDown(ctx, m.conn, migration.Version); err != nil {
				return err
			}
		}
	} else {
		if tx != nil {
			if err := m.vtm.InsertVersionUp(ctx, m.conn, migration.Version); err != nil {
				return err
			}
		} else {
			if err := m.vtm.FinishVersionUp(ctx, m.conn, migration.Version); err != nil {
				return err
			}
		}
	}

	if tx != nil {
		if err := tx.Commit(ctx); err != nil {
			return MigrationError{
				Err:  fmt.Errorf("commit failed: %w", err),
				Meta: migration.Meta,
			}
		}
	}

	return nil
}

// Lock to ensure multiple migrations cannot occur simultaneously.
// Constant is arbitrary random number; can be regenerated with
// echo "obase=16;ibase=16;$(openssl rand -hex 8 | tr '[:lower:]' '[:upper:]') - 7FFFFFFFFFFFFFFF" | bc
const lockNum = uint64(0x49D7B5E9559EB483)

func (m *Migrator) acquireLock(ctx context.Context) (func(), error) {
	util.Logf(3, "acquiring advisory lock")
	var acquired bool
	if err := m.conn.QueryRow(ctx, "select pg_try_advisory_lock($1);", lockNum).Scan(&acquired); err != nil {
		return nil, err
	}
	if !acquired {
		util.Logf(-1, "run this to unlock if last migration crashed: select pg_advisory_unlock(%d)", lockNum)
		return nil, fmt.Errorf("failed to acquire advisory lock; is another migration being run?")
	}
	return func() {
		util.Logf(3, "releasing advisory lock")
		if _, err := m.conn.Exec(ctx, "select pg_advisory_unlock($1)", lockNum); err != nil {
			util.Logf(-1, "error releasing advisory lock: %s", err)
		}
	}, nil
}

type MigrationError struct {
	Err error
	Meta
	LineNum   int
	ColumnNum int
}

func (e MigrationError) Error() string {
	if e.LineNum == 0 {
		return fmt.Sprintf("%s: %s", e.Filename, e.Err)
	}
	if e.ColumnNum == 0 {
		return fmt.Sprintf("%s:%d: %s", e.Filename, e.LineNum, e.Err)
	}
	return fmt.Sprintf("%s:%d:%d: %s", e.Filename, e.LineNum, e.ColumnNum, e.Err)
}
