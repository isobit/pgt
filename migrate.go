package pgt

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/golang-migrate/migrate/v4"
	migrate_pgx "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5/pgxpool"
	pgx_stdlib "github.com/jackc/pgx/v5/stdlib"

	"github.com/isobit/pgt/util"
)

type MigrateCommand struct {
	Database    string `cli:"required,short=d,env=PGT_DATABASE_URL"`
	Source      string `cli:"required,short=s,env=PGT_MIGRATION_SOURCE"`
	Version     *uint
	Test        bool   `cli:"env=PGT_TEST"`
	Dump        string `cli:"env=PGT_DUMP"`
	DumpDown    string `cli:"env=PGT_DUMP_DOWN"`
	DumpCommand string `cli:"env=PGT_DUMP_COMMAND"`
}

func NewMigrateCommand() *MigrateCommand {
	return &MigrateCommand{
		DumpCommand: "pg_dump --schema-only '{{.Url}}'",
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

	db, err := sql.Open("pgx", pgx_stdlib.RegisterConnConfig(poolCfg.ConnConfig))
	if err != nil {
		return fmt.Errorf("error opening postgres: %w", err)
	}
	defer db.Close()

	mDriver, err := migrate_pgx.WithInstance(db, &migrate_pgx.Config{})
	if err != nil {
		return fmt.Errorf("error creating migration driver: %w", err)
	}

	m, err := migrate.NewWithDatabaseInstance(
		cmd.Source,
		poolCfg.ConnConfig.Database,
		mDriver,
	)
	if err != nil {
		return fmt.Errorf("error instantiating migration: %w", err)
	}
	defer m.Close()

	if cmd.Version != nil {
		version := *cmd.Version
		util.Logf(0, "migrating up to version %d", version)
		if err := m.Migrate(version); err != nil {
			retainTestDatabase = true
			return fmt.Errorf("error applying migration: %w", err)
		}
	} else {
		util.Logf(0, "migrating up")
		if err := m.Up(); err != nil {
			retainTestDatabase = true
			return fmt.Errorf("error migrating up: %w", err)
		}
	}

	if dumper != nil && cmd.Dump != "" {
		util.Logf(0, "dumping to %s", cmd.Dump)
		if err := dumper.Dump(ctx, cmd.Dump); err != nil {
			return err
		}
	}

	if cmd.Test {
		util.Logf(0, "migrating down")
		if err := m.Down(); err != nil {
			retainTestDatabase = true
			return fmt.Errorf("error migrating down: %w", err)
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
