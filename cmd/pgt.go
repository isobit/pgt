package main

import (
	"github.com/isobit/cli"
	"github.com/isobit/pgt"
)

func main() {
	cmd := cli.New(
		"pgt", nil,
		cli.New("migrate", pgt.NewMigrateCommand()),
		cli.New("bench", pgt.NewBenchCommand()),
		cli.New("blockwatcher", &pgt.BlockWatcherCommand{}),
	)
	cmd.Parse().RunFatalWithSigCancel()
}
