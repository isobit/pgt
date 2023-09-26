package fmt

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/mjibson/sqlfmt"
)

type FmtCommand struct {
	LineWidth int `cli:"short=w"`
	TabWidth  int `cli:"short=t"`
	Case      string
}

func NewFmtCommand() *FmtCommand {
	return &FmtCommand{
		LineWidth: 80,
		TabWidth:  0,
		Case:      "lower",
	}
}

func (cmd *FmtCommand) Run(ctx context.Context) error {
	data, err := io.ReadAll(os.Stdin)
	if err != nil {
		return err
	}

	stmts := []string{string(data)}

	cfg := tree.PrettyCfg{
		LineWidth: cmd.LineWidth,
		Simplify:  true,
		TabWidth:  4,
		UseTabs:   true,
		Align:     tree.PrettyNoAlign,
	}
	if cmd.TabWidth != 0 {
		cfg.TabWidth = cmd.TabWidth
		cfg.UseTabs = false
	}
	switch cmd.Case {
	case "lower":
		cfg.Case = strings.ToLower
	case "upper":
		cfg.Case = strings.ToUpper
	default:
		return fmt.Errorf("unknown case, expected \"lower\" or \"upper\": %s", cmd.Case)
	}
	fmt.Fprintln(os.Stderr, cfg)

	fmtStmts, err := sqlfmt.FmtSQL(cfg, stmts)
	if err != nil {
		return err
	}

	fmt.Println(fmtStmts)

	return nil
}
