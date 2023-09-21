package pgt

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/isobit/pgt/migrate"
	"github.com/isobit/pgt/util"
)

type MigrateCommand struct {
	Database string `cli:"required,short=d,env=PGT_DATABASE,help=database connection string"`
	Source   string `cli:"required,short=s,env=PGT_MIGRATION_SOURCE,help=path to migrations directory"`
	Target   *int   `cli:"short=t,env=PGT_MIGRATION_TARGET,help=version to target,default=latest"`

	VersionTable string `cli:"env=PGT_VERSION_TABLE"`

	Query bool `cli:"short=q,help=print the current version instead of migrating"`

	Yes       bool `cli:"short=y,help=bypass all confirmation prompts"`
	ForceSkip bool `cli:"help=forcibly set the schema version to the target skipping any migrations"`

	Test               bool `cli:"env=PGT_TEST,help=enable test mode"`
	TestDatabaseName   string
	RetainTestDatabase bool

	Dump        string `cli:"env=PGT_DUMP,help=file path to dump schema to"`
	DumpCommand string `cli:"env=PGT_DUMP_COMMAND,help=command used to dump schema"`

	MaxBlockDuration  time.Duration `cli:"env=PGT_MAX_BLOCK_DURATION"`
	MaxBlockProcesses int           `cli:"env=PGT_MAX_BLOCK_PROCESSES"`

	interactive bool
}

func NewMigrateCommand(interactive bool) *MigrateCommand {
	return &MigrateCommand{
		VersionTable:      "pgt.schema_version",
		TestDatabaseName:  fmt.Sprintf("pgt_migrate_test_%d", time.Now().UTC().Unix()),
		DumpCommand:       "pg_dump --schema-only '{{.Url}}'",
		MaxBlockDuration:  10 * time.Second,
		MaxBlockProcesses: 0,

		interactive: interactive,
	}
}

func (cmd *MigrateCommand) Before() error {
	if cmd.ForceSkip {
		if cmd.Test {
			return fmt.Errorf("cannot use --force-skip and --test together")
		}
		if cmd.Dump != "" {
			return fmt.Errorf("cannot use --force-skip and --dump together")
		}
	}
	return nil
}

func (cmd *MigrateCommand) Run(ctx context.Context) error {
	migrations, err := cmd.loadMigrations()
	if err != nil {
		return err
	}

	poolCfg, err := pgxpool.ParseConfig(cmd.Database)
	if err != nil {
		return err
	}

	retainTestDatabase := cmd.RetainTestDatabase
	if cmd.Test {
		origPool, err := pgxpool.NewWithConfig(ctx, poolCfg.Copy())
		if err != nil {
			return err
		}

		testDatabase := cmd.TestDatabaseName
		util.Logf(0, "creating test database: %s", testDatabase)
		if _, err := origPool.Exec(ctx, fmt.Sprintf(`create database "%s";`, testDatabase)); err != nil {
			return err
		}
		defer func() {
			if retainTestDatabase {
				util.Logf(-1, "retaining test database: %s", testDatabase)
				return
			}
			util.Logf(0, "dropping test database: %s", testDatabase)
			if _, err := origPool.Exec(ctx, fmt.Sprintf(`drop database "%s";`, testDatabase)); err != nil {
				util.Logf(-2, "error dropping test database: %s", err)
			}
		}()
		poolCfg.ConnConfig.Database = testDatabase
	}

	var dumper *util.Dumper
	if cmd.Dump != "" {
		d, err := util.NewDumper(poolCfg.ConnConfig, cmd.DumpCommand)
		if err != nil {
			return err
		}
		dumper = d
	}

	mctx, cancel := context.WithCancel(ctx)
	defer cancel()

	pool, err := pgxpool.NewWithConfig(mctx, poolCfg)
	if err != nil {
		return err
	}
	defer pool.Close()

	conn, err := util.AcquireWithBlockWatcher(mctx, pool, cmd.MaxBlockDuration, cmd.MaxBlockProcesses)
	if err != nil {
		return err
	}
	defer conn.Release()

	m := migrate.NewMigrator(conn.Conn.Conn(), cmd.VersionTable, migrations)
	m.BeforeExec = func(meta migrate.Meta, step migrate.Step) error {
		desc := fmt.Sprintf("%d (%s) %s", meta.Version, meta.Name, step.Name)
		if !(cmd.Yes || cmd.Test) {
			if cmd.interactive {
				reprompt := true
				for reprompt {
					ans, err := util.Prompt(ctx, fmt.Sprintf("execute %s [y/n/?]?", desc))
					if err != nil {
						return err
					}
					switch ans {
					case "n", "N":
						return fmt.Errorf("migrate aborted")
					case "y", "Y":
						reprompt = false
					case "?":
						r := regexp.MustCompile(`(?m)^`)
						fmt.Fprintln(os.Stderr)
						fmt.Fprintln(os.Stderr, r.ReplaceAllLiteralString(step.SQL, "\t"))
						fmt.Fprintln(os.Stderr)
					}
				}
			} else {
				return fmt.Errorf("refusing to execute %s in non-interactive mode without -y", desc)
			}
		}
		util.Logf(0, "executing %s", desc)
		return nil
	}

	if cmd.Query {
		version, err := m.CurrentVersion(ctx)
		if err != nil {
			return err
		}
		fmt.Println(version)
		return nil
	}

	// Default target version to the max version if none is specified.
	_, maxVersion := m.VersionRange()
	targetVersion := maxVersion
	if cmd.Target != nil {
		targetVersion = *cmd.Target
	}

	if cmd.ForceSkip {
		return m.ForceSkipTo(ctx, targetVersion)
	}

	var merr error
	if cmd.Test {
		for v := 0; v <= maxVersion; v += 1 {
			util.Logf(0, "testing migration for version %d", v)
			merr = m.MigrateTo(mctx, v)
			if merr != nil {
				break
			}

			if m.Migration(v).Down.SQL == "" {
				util.Logf(0, "skipping down test for irreversible migration version %d", v)
				continue
			}

			// Test down and then up again.
			merr = m.MigrateTo(mctx, v-1)
			if merr != nil {
				break
			}
			merr = m.MigrateTo(mctx, v)
			if merr != nil {
				break
			}
		}
	} else {
		util.Logf(0, "migrating to version %d", targetVersion)
		merr = m.MigrateTo(mctx, targetVersion)
		if merr == nil {
			util.Logf(0, "migration to %d complete", targetVersion)
		}
	}
	if merr != nil {
		if merr, ok := merr.(migrate.MigrationError); ok {
			if pgErr, ok := merr.Err.(*pgconn.PgError); ok {
				if pgErr.Code == "25001" {
					util.Logf(-1, "%s: hint: add the following to disable migration transaction wrapping: --pgt:no_transaction", merr.Filename)
				}
			}
		}
		return merr
	}

	if dumper != nil && cmd.Dump != "" {
		util.Logf(0, "dumping to %s", cmd.Dump)
		if err := dumper.Dump(ctx, cmd.Dump); err != nil {
			return err
		}
	}

	return nil
}

func (cmd *MigrateCommand) loadMigrations() ([]migrate.Migration, error) {
	if _, err := os.Stat(cmd.Source); err != nil {
		return nil, err
	}
	loader := migrate.NewLoader(os.DirFS(cmd.Source))
	return loader.Load()
}
