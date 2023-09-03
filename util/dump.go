package util

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"text/template"

	"github.com/google/shlex"
	"github.com/jackc/pgx/v5"
)

type Dumper struct {
	commandPath string
	commandArgs []string
	commandEnv  []string
	outputPath  string
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
		commandPath: cmdPath,
		commandArgs: cmdArgs[1:],
		commandEnv:  cmdEnv,
		outputPath:  outputPath,
	}
	return dumper, nil
}

func (d *Dumper) Dump(ctx context.Context) error {
	Logf(1, "dumping to %s using %s %s", d.outputPath, d.commandPath, strings.Join(d.commandArgs, " "))
	cmd := exec.CommandContext(ctx, d.commandPath, d.commandArgs...)
	cmd.Env = d.commandEnv

	outfile, err := os.Create(d.outputPath)
	if err != nil {
		return fmt.Errorf("error creating dump output %s: %w", d.outputPath, err)
	}
	defer outfile.Close()
	cmd.Stdout = outfile

	stderr := strings.Builder{}
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		Logf(2, "dump stderr: %s", stderr.String())
		return fmt.Errorf("dump failed: %w", err)
	}
	if err := cmd.Wait(); err != nil {
		Logf(2, "dump stderr: %s", stderr.String())
		return fmt.Errorf("dump failed: %w", err)
	}
	return nil
}
