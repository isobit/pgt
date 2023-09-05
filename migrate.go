package pgt

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/isobit/pgt/migrate"
	"github.com/isobit/pgt/util"
)

type MigrateCommand struct {
	Database string `cli:"required,short=d,env=PGT_DATABASE_URL"`
	Source   string `cli:"required,short=s,env=PGT_MIGRATION_SOURCE"`
	Version  int32

	Test bool `cli:"env=PGT_TEST"`

	Dump        string `cli:"env=PGT_DUMP"`
	DumpDown    string `cli:"env=PGT_DUMP_DOWN"`
	DumpCommand string `cli:"env=PGT_DUMP_COMMAND"`

	VersionTable     string `cli:"env=PGT_VERSION_TABLE"`
	MaxBlockDuration time.Duration
}

func NewMigrateCommand() *MigrateCommand {
	return &MigrateCommand{
		Version: -1,

		DumpCommand: "pg_dump --schema-only '{{.Url}}'",

		VersionTable:     "schema_version",
		MaxBlockDuration: 10 * time.Second,
	}
}

func (cmd *MigrateCommand) Run(ctx context.Context) error {
	poolCfg, err := pgxpool.ParseConfig(cmd.Database)
	if err != nil {
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
		if _, err := origPool.Exec(ctx, fmt.Sprintf("create database \"%s\";", testDatabase)); err != nil {
			return err
		}
		defer func() {
			if retainTestDatabase {
				util.Logf(-1, "retaining test database: %s", testDatabase)
				return
			}
			util.Logf(0, "dropping test database: %s", testDatabase)
			if _, err := origPool.Exec(ctx, fmt.Sprintf("drop database \"%s\";", testDatabase)); err != nil {
				util.Logf(-2, "error dropping test database: %s", err)
			}
		}()
		poolCfg.ConnConfig.Database = testDatabase
	}

	var dumper *util.Dumper
	if cmd.Dump != "" || cmd.DumpDown != "" {
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

	if cmd.Version < 0 {
		util.Logf(0, "migrating to latest version")
		if err := m.Migrate(mctx); err != nil {
			return err
		}
	} else {
		util.Logf(0, "migrating to version %d", cmd.Version)
		if err := m.MigrateTo(mctx, cmd.Version); err != nil {
			return err
		}
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

		if dumper != nil && cmd.DumpDown != "" {
			util.Logf(0, "dumping post-down to %s", cmd.DumpDown)
			if err := dumper.Dump(ctx, cmd.DumpDown); err != nil {
				return err
			}
		}
	}

	return nil
}
