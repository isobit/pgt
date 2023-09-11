package main

import (
	"fmt"
	"os"

	"github.com/isobit/cli"
	"github.com/isobit/pgt"
	pgt_util "github.com/isobit/pgt/util"
)

func main() {
	interactive := false
	if stderrStat, err := os.Stderr.Stat(); err == nil && stderrStat.Mode()&os.ModeCharDevice != 0 {
		interactive = true
		pgt_util.LogColor = true
	}

	cmd := cli.New(
		"pgt", &CLI{},
		cli.New("migrate", pgt.NewMigrateCommand(interactive)),
		// cli.New("version", pgt.NewMigrateVersionCommand()),
		cli.New("exec", pgt.NewExecCommand()),
		cli.New("bench", pgt.NewBenchCommand()),
	)
	if err := cmd.Parse().RunWithSigCancel(); err != nil {
		pgt_util.Logf(-2, "%s", err)
		os.Exit(1)
	}
}

type CLI struct {
	Verbosity int  `cli:"short=v,help=set verbosity level"`
	Version   bool `cli:"short=V,help=show version"`
}

func (cmd CLI) Before() error {
	pgt_util.LogLevel = cmd.Verbosity
	return nil
}

func (cmd CLI) Run() error {
	if cmd.Version {
		fmt.Fprintln(os.Stderr, pgt.Version)
		return nil
	}
	return cli.UsageErrorf("no command specified")
}
