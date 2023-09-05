package pgt

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/isobit/pgt/migrate"
	"github.com/isobit/pgt/util"
)

type MigrateCommand struct {
	Database string `cli:"required,short=d,env=PGT_DATABASE,help=database connection string"`
	Source   string `cli:"required,short=s,env=PGT_MIGRATION_SOURCE,help=path to migrations directory"`
	Target   *int32 `cli:"short=t,env=PGT_MIGRATION_TARGET,help=version to target,default=latest"`
	// Data map[string]string

	Test bool `cli:"env=PGT_TEST,help=enable test mode"`

	Dump        string `cli:"env=PGT_DUMP,help=file path to dump schema to"`
	DumpCommand string `cli:"env=PGT_DUMP_COMMAND,help=command used to dump schema"`

	VersionTable     string        `cli:"env=PGT_VERSION_TABLE"`
	MaxBlockDuration time.Duration `cli:"env=PGT_MAX_BLOCK_DURATION"`
}

func NewMigrateCommand() *MigrateCommand {
	return &MigrateCommand{
		DumpCommand: "pg_dump --schema-only '{{.Url}}'",

		VersionTable:     "pgt.schema_version",
		MaxBlockDuration: 10 * time.Second,
	}
}

func (cmd *MigrateCommand) Run(ctx context.Context) error {
	poolCfg, err := pgxpool.ParseConfig(cmd.Database)
	if err != nil {
		return err
	}

	if _, err := os.Stat(cmd.Source); err != nil {
		return err
	}

	retainTestDatabase := false
	testDatabase := ""
	if cmd.Test {
		origPool, err := pgxpool.NewWithConfig(ctx, poolCfg.Copy())
		if err != nil {
			return err
		}

		testDatabase = fmt.Sprintf("pgt_migrate_test_%d", time.Now().UTC().Unix())
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

	conn, err := util.AcquireWithBlockWatcher(mctx, pool, cmd.MaxBlockDuration)
	if err != nil {
		return err
	}
	defer conn.Release()

	m, err := migrate.NewMigrator(mctx, conn.Conn.Conn(), cmd.VersionTable)
	if err != nil {
		return err
	}
	if err := m.LoadMigrations(os.DirFS(cmd.Source)); err != nil {
		return err
	}
	m.OnStart = func(sequence int32, name string, direction string, sql string) {
		util.Logf(0, "starting %s migration for version %d", direction, sequence)
	}

	var merr error
	if cmd.Target != nil {
		version := *cmd.Target
		util.Logf(0, "migrating to version %d", version)
		merr = m.MigrateTo(mctx, version)
	} else {
		util.Logf(0, "migrating to latest version")
		merr = m.Migrate(mctx)
	}
	if merr != nil {
		if merr, ok := merr.(migrate.MigrationPgError); ok {
			if merr.Code == "25001" {
				util.Logf(-1, "%s: hint: add the following to disable migration transaction wrapping: --pgt:disable-txn", merr.MigrationName)
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

	if cmd.Test {
		util.Logf(0, "migrating down to 0")
		if err := m.MigrateTo(mctx, 0); err != nil {
			return err
		}
	}

	return nil
}

type MigrateVersionCommand struct {
	Database        string `cli:"required,short=d,env=PGT_DATABASE,help=database connection string"`
	ForceSetVersion *int32 `cli:"help=override current version in version table"`
	VersionTable    string `cli:"env=PGT_VERSION_TABLE"`
}

func NewMigrateVersionCommand() *MigrateVersionCommand {
	return &MigrateVersionCommand{
		VersionTable: "pgt.schema_version",
	}
}

func (cmd *MigrateVersionCommand) Run(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, cmd.Database)
	if err != nil {
		return err
	}

	m, err := migrate.NewMigrator(ctx, conn, cmd.VersionTable)
	if err != nil {
		return err
	}

	if cmd.ForceSetVersion != nil {
		if err := m.ForceSetCurrentVersion(ctx, *cmd.ForceSetVersion); err != nil {
			return err
		}
	}

	version, err := m.GetCurrentVersion(ctx)
	if err != nil {
		return err
	}
	fmt.Println(version)

	return nil
}
