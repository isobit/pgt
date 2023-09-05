package pgt

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	util "github.com/isobit/pgt/util"
)

type ExecCommand struct {
	Database string `cli:"required,short=d,env=PGT_DATABASE,help=database connection string"`
	Query    string `cli:"required,short=q"`

	MaxBlockDuration  time.Duration `cli:"env=PGT_MAX_BLOCK_DURATION"`
	MaxBlockProcesses int           `cli:"env=PGT_MAX_BLOCK_PROCESSES"`
}

func NewExecCommand() *ExecCommand {
	return &ExecCommand{
		MaxBlockDuration:  10 * time.Second,
		MaxBlockProcesses: 0,
	}
}

func (cmd *ExecCommand) Run(ctx context.Context) error {
	pool, err := pgxpool.New(ctx, cmd.Database)
	if err != nil {
		return err
	}
	defer pool.Close()

	conn, err := util.AcquireWithBlockWatcher(ctx, pool, cmd.MaxBlockDuration, cmd.MaxBlockProcesses)
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
