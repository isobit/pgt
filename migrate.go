package pgt

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"time"

	// "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/isobit/pgt/migrate"
	"github.com/isobit/pgt/util"
)

type MigrateCommand struct {
	Database string `cli:"required,short=d,env=PGT_DATABASE,help=database connection string"`
	Source   string `cli:"required,short=s,env=PGT_MIGRATION_SOURCE,help=path to migrations directory"`
	Target   *int   `cli:"short=t,env=PGT_MIGRATION_TARGET,help=version to target,default=latest"`
	// Data map[string]string

	VersionTable string `cli:"env=PGT_VERSION_TABLE"`

	interactive bool
	Yes         bool `cli:"short=y"`

	Test               bool `cli:"env=PGT_TEST,help=enable test mode"`
	TestDatabaseName   string
	RetainTestDatabase bool

	Dump        string `cli:"env=PGT_DUMP,help=file path to dump schema to"`
	DumpCommand string `cli:"env=PGT_DUMP_COMMAND,help=command used to dump schema"`

	MaxBlockDuration  time.Duration `cli:"env=PGT_MAX_BLOCK_DURATION"`
	MaxBlockProcesses int           `cli:"env=PGT_MAX_BLOCK_PROCESSES"`
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

	var merr error
	if cmd.Test {
		for v := 0; v <= m.MaxVersion(); v += 1 {
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
	} else if cmd.Target != nil {
		version := *cmd.Target
		util.Logf(0, "migrating to version %d", version)
		merr = m.MigrateTo(mctx, version)
	} else {
		util.Logf(0, "migrating to latest version")
		merr = m.MigrateTo(mctx, m.MaxVersion())
	}
	if merr != nil {
		if merr, ok := merr.(*migrate.MigrationError); ok {
			if pgErr, ok := merr.Err.(*pgconn.PgError); ok {
				if pgErr.Code == "25001" {
					util.Logf(-1, "%s: hint: add the following to disable migration transaction wrapping: --pgt:disable-txn", merr.Filename)
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

// type MigrateVersionCommand struct {
// 	Database        string `cli:"required,short=d,env=PGT_DATABASE,help=database connection string"`
// 	ForceSetVersion *int32 `cli:"help=override current version in version table"`
// 	VersionTable    string `cli:"env=PGT_VERSION_TABLE"`
// }

// func NewMigrateVersionCommand() *MigrateVersionCommand {
// 	return &MigrateVersionCommand{
// 		VersionTable: "pgt.schema_version",
// 	}
// }

// func (cmd *MigrateVersionCommand) Run(ctx context.Context) error {
// 	conn, err := pgx.Connect(ctx, cmd.Database)
// 	if err != nil {
// 		return err
// 	}

// 	m, err := migrate.NewMigrator(ctx, conn, cmd.VersionTable)
// 	if err != nil {
// 		return err
// 	}

// 	if cmd.ForceSetVersion != nil {
// 		if err := m.ForceSetCurrentVersion(ctx, *cmd.ForceSetVersion); err != nil {
// 			return err
// 		}
// 	}

// 	version, err := m.GetCurrentVersion(ctx)
// 	if err != nil {
// 		return err
// 	}
// 	fmt.Println(version)

// 	return nil
// }
