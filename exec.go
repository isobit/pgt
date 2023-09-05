package pgt

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	util "github.com/isobit/pgt/util"
)

type ExecCommand struct {
	Database string `cli:"short=d"`
	Query    string `cli:"short=q"`

	MaxBlockDuration time.Duration
}

func NewExecCommand() *ExecCommand {
	return &ExecCommand{
		MaxBlockDuration: 10 * time.Second,
	}
}

func (cmd *ExecCommand) Run(ctx context.Context) error {
	pool, err := pgxpool.New(ctx, cmd.Database)
	if err != nil {
		return err
	}
	defer pool.Close()

	conn, err := util.AcquireWithBlockWatcher(ctx, pool, cmd.MaxBlockDuration)
	if err != nil {
		return err
	}
	defer conn.Release()

	tag, err := conn.Exec(ctx, cmd.Query)
	if err != nil {
		return err
	}

	util.Logf(0, "%+v", tag)

	return nil
}
