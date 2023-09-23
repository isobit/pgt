package pgt

import (
	"context"

	"github.com/isobit/pgt/mux"
)

type MuxCommand struct {
	Database   string `cli:"required,short=d,env=PGT_DATABASE,help=database connection string"`
	ListenAddr string `cli:"required,short=l"`
}

func NewMuxCommand() *MuxCommand {
	return &MuxCommand{}
}

func (cmd *MuxCommand) Run(ctx context.Context) error {
	return mux.Listen(ctx, cmd.Database, cmd.ListenAddr)
}
