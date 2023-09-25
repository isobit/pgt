package pgt

import (
	"context"

	"github.com/isobit/cli"

	"github.com/isobit/pgt/mux"
)

type MuxCommand struct {
	Database   string `cli:"required,short=d,env=PGT_DATABASE,help=database connection string"`
	ListenAddr string `cli:"required,short=l"`
}

func (*MuxCommand) SetupCommand(cmd *cli.Command) {
	cmd.SetHelp("simple and production-safe plain SQL migrations")
	cmd.SetDescription(`
		Proxies PostgreSQL protocol TCP connections to the specified listen
		address to the specified upstream database.
		
		Connections can be multiplexed into the same upstream
		connection/session by specifying the same session ID. Database names
		passed by clients will be ignored (they will always proxy to the same
		specified upstream database), unless they contain "@", in which case
		the substring following "@" will be used as the session ID. If no
		session ID is specified, it defaults to the client address, e.g.
		"127.0.0.1:12345".

		This is particularly useful for debugging applications mid-transaction;
		to inspect the state of the database as seen by an application
		transaction, simply connect to the same session using psql.

		Example using an explicit session key:

			$ pgt mux -d postgres://localhost:5432/postgres -l localhost:5433
			# These will both connect to the same session:
			$ psql postgres://localhost:5433/@foo
			$ psql postgres://localhost:5433/@foo
	`)
}

func NewMuxCommand() *MuxCommand {
	return &MuxCommand{}
}

func (cmd *MuxCommand) Run(ctx context.Context) error {
	return mux.Listen(ctx, cmd.Database, cmd.ListenAddr)
}
