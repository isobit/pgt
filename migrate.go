package pgt

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"text/template"
	"time"

	"github.com/golang-migrate/migrate/v4"
	migrate_pgx "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/google/shlex"
	"github.com/jackc/pgx/v5"
	pgx_stdlib "github.com/jackc/pgx/v5/stdlib"
)

type MigrateCommand struct {
	Database    string `cli:"required,short=d,env=PGT_DATABASE_URL"`
	Source      string `cli:"required,short=s,env=PGT_MIGRATION_SOURCE"`
	Test        bool   `cli:"short=t,env=PGT_TEST"`
	DumpOutput  string `cli:"short=o,env=PGT_DUMP_OUTPUT"`
	DumpCommand string `cli:"env=PGT_DUMP_COMMAND"`
}

func NewMigrateCommand() *MigrateCommand {
	return &MigrateCommand{
		DumpCommand: "pg_dump --schema-only '{{.Url}}'",
	}
}

func (cmd *MigrateCommand) Run(ctx context.Context) error {
	pgCfg, err := pgx.ParseConfig(cmd.Database)
	if err != nil {
		return err
	}

	retainTestDatabase := false
	testDatabase := ""
	if cmd.Test {
		origCfg := pgCfg.Copy()
		conn, err := pgx.ConnectConfig(ctx, origCfg)
		if err != nil {
			return err
		}

		testDatabase = fmt.Sprintf("pgt_migrate_test_%d", time.Now().UTC().Unix())
		logf("creating test database: %s", testDatabase)
		if _, err := conn.Exec(ctx, fmt.Sprintf("create database \"%s\";", testDatabase)); err != nil {
			return err
		}
		defer func() {
			if retainTestDatabase {
				logf("retaining test database: %s", testDatabase)
				return
			}
			logf("dropping test database: %s", testDatabase)
			if _, err := conn.Exec(ctx, fmt.Sprintf("drop database \"%s\";", testDatabase)); err != nil {
				logf("error dropping test database: %s", err)
			}
		}()
		pgCfg.Database = testDatabase
	}

	var dumper *Dumper
	if cmd.DumpOutput != "" {
		d, err := NewDumper(pgCfg, cmd.DumpCommand, cmd.DumpOutput)
		if err != nil {
			return err
		}
		dumper = d
	}

	db, err := sql.Open("pgx", pgx_stdlib.RegisterConnConfig(pgCfg))
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
		pgCfg.Database,
		mDriver,
	)
	if err != nil {
		return fmt.Errorf("error instantiating migration: %w", err)
	}
	defer m.Close()

	logf("running migrations")
	if err := m.Up(); err != nil {
		retainTestDatabase = true
		return fmt.Errorf("error applying migration: %w", err)
	}

	if dumper != nil {
		if err := dumper.Dump(ctx); err != nil {
			return err
		}
	}

	return nil
}

type Dumper struct {
	CommandPath string
	CommandArgs []string
	CommandEnv  []string
	OutputPath  string
}

func NewDumper(pgCfg *pgx.ConnConfig, command string, outputPath string) (*Dumper, error) {
	cmdTemplate, err := template.New("").Parse(command)
	if err != nil {
		return nil, err
	}

	cmdData := struct {
		*pgx.ConnConfig
		Url url.URL
	}{
		ConnConfig: pgCfg,
		Url: url.URL{
			Scheme: "postgres",
			User:   url.User(pgCfg.User),
			Host:   fmt.Sprintf("%s:%d", pgCfg.Host, pgCfg.Port),
			Path:   pgCfg.Database,
		},
	}

	cmdStringBuilder := strings.Builder{}
	if err := cmdTemplate.Execute(&cmdStringBuilder, &cmdData); err != nil {
		return nil, err
	}

	cmdArgs, err := shlex.Split(cmdStringBuilder.String())
	if err != nil {
		return nil, err
	}

	cmdPath, err := exec.LookPath(cmdArgs[0])
	if err != nil {
		return nil, fmt.Errorf("dump command not found: %w", err)
	}

	cmdEnv := os.Environ()
	if pgCfg.Password != "" {
		cmdEnv = append(cmdEnv, fmt.Sprintf("PGPASSWORD=%s", pgCfg.Password))
	}

	dumper := &Dumper{
		CommandPath: cmdPath,
		CommandArgs: cmdArgs[1:],
		CommandEnv:  cmdEnv,
		OutputPath:  outputPath,
	}
	return dumper, nil
}

func (d *Dumper) Dump(ctx context.Context) error {
	logf("dumping to %s using %s %s", d.OutputPath, d.CommandPath, strings.Join(d.CommandArgs, " "))
	cmd := exec.CommandContext(ctx, d.CommandPath, d.CommandArgs...)
	cmd.Env = d.CommandEnv

	outfile, err := os.Create(d.OutputPath)
	if err != nil {
		return fmt.Errorf("error creating dump output %s: %w", d.OutputPath, err)
	}
	defer outfile.Close()
	cmd.Stdout = outfile

	stderr := strings.Builder{}
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		logf("dump stderr: %s", stderr.String())
		return fmt.Errorf("dump failed: %w", err)
	}
	if err := cmd.Wait(); err != nil {
		logf("dump stderr: %s", stderr.String())
		return fmt.Errorf("dump failed: %w", err)
	}
	return nil
}