package main

import (
	"os"

	"github.com/isobit/cli"
	"github.com/isobit/pgt"
	pgt_util "github.com/isobit/pgt/util"
)

func main() {
	if stderrStat, err := os.Stderr.Stat(); err == nil {
		if stderrStat.Mode()&os.ModeCharDevice != 0 {
			pgt_util.LogColor = true
		}
	}

	cmd := cli.New(
		"pgt", &CLI{},
		cli.New("migrate", pgt.NewMigrateCommand()),
		cli.New("version", pgt.NewMigrateVersionCommand()),
		cli.New("exec", pgt.NewExecCommand()),
		cli.New("bench", pgt.NewBenchCommand()),
	)
	if err := cmd.Parse().RunWithSigCancel(); err != nil {
		pgt_util.Logf(-2, "%s", err)
		os.Exit(1)
	}
}

type CLI struct {
	Verbosity int `cli:"short=v"`
}

func (cmd CLI) Before() error {
	pgt_util.LogLevel = cmd.Verbosity
	return nil
}
