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

func (m *Migrator) MaxVersion() int {
	return len(m.migrations) - 1
}

func (m *Migrator) Migration(version int) Migration {
	return m.migrations[version]
}

func (m *Migrator) MigrateTo(ctx context.Context, targetVersion int) (err error) {
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

	minVersion := -1
	maxVersion := len(m.migrations) - 1

	if targetVersion < minVersion || targetVersion > maxVersion {
		return fmt.Errorf("target version %d is outside the valid version range of %d to %d", targetVersion, minVersion, maxVersion)
	}

	currentVersion, err := m.vtm.GetCurrentVersion(ctx, m.conn)
	if err != nil {
		return err
	}

	if currentVersion < minVersion || targetVersion > maxVersion {
		return fmt.Errorf("current version %d is outside the valid version range of %d to %d", currentVersion, minVersion, maxVersion)
	}

	down := targetVersion < currentVersion

	// util.Logf(0, "migrating from version %d to version %d", currentVersion, targetVersion)
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

	for _, statement := range sqlStatements {
		util.Logf(3, "exec: %s", statement)
		if _, err := m.conn.Exec(ctx, statement); err != nil {
			if err, ok := err.(*pgconn.PgError); ok {
				fmt.Println(err.Position)
				line, col := step.PositionToLineCol(int(err.Position))
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

// Lock to ensure multiple migrations cannot occur simultaneously.
// Constant is arbitrary random number; can be regenerated with
// echo "obase=16;ibase=16;$(openssl rand -hex 8 | tr '[:lower:]' '[:upper:]') - 7FFFFFFFFFFFFFFF" | bc
const lockNum = uint64(0x49D7B5E9559EB483)

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
