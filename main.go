package main

import (
	"fmt"
	"os"

	"github.com/isobit/cli"

	"github.com/isobit/pgt/internal/bench"
	"github.com/isobit/pgt/internal/exec"
	"github.com/isobit/pgt/internal/inspect"
	"github.com/isobit/pgt/internal/migrate"
	"github.com/isobit/pgt/internal/mux"
	"github.com/isobit/pgt/internal/util"
)

var Version string = "unknown"

func main() {
	interactive := false
	if stderrStat, err := os.Stderr.Stat(); err == nil && stderrStat.Mode()&os.ModeCharDevice != 0 {
		interactive = true
		util.LogColor = true
	}

	cmd := cli.New(
		"pgt", &CLI{},
		cli.WithDescription("Pretty Good Tools for PostgreSQL"),
		cli.New("migrate", migrate.NewMigrateCommand(interactive)),
		cli.New("mux", mux.NewMuxCommand()),
		cli.New("inspect", inspect.NewInspectCommand()),
		cli.New("bench", bench.NewBenchCommand()),
		cli.New("exec", exec.NewExecCommand()),
	)
	if err := cmd.Parse().RunWithSigCancel(); err != nil {
		util.Logf(-2, "%s", err)
		os.Exit(1)
	}
}

type CLI struct {
	Verbosity int  `cli:"short=v,help=set verbosity level"`
	Version   bool `cli:"short=V,help=show version"`
}

func (cmd CLI) Before() error {
	util.LogLevel = cmd.Verbosity
	return nil
}

func (cmd CLI) Run() error {
	if cmd.Version {
		fmt.Println(Version)
		return nil
	}
	return cli.UsageErrorf("no command specified")
}
