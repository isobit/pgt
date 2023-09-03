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
	if cmd.Dump != "" {
		d, err := util.NewDumper(poolCfg.ConnConfig, cmd.DumpCommand, cmd.Dump)
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

	util.Logf(0, "running migrations")
	if cmd.Version != nil {
		if err := m.Migrate(*cmd.Version); err != nil {
			retainTestDatabase = true
			return fmt.Errorf("error applying migration: %w", err)
		}
	} else {
		if err := m.Up(); err != nil {
			retainTestDatabase = true
			return fmt.Errorf("error applying migration: %w", err)
		}
	}

	if dumper != nil {
		if err := dumper.Dump(ctx); err != nil {
			return err
		}
	}

	return nil
}
