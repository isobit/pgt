package explain

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	// "github.com/isobit/pgt/internal/util"
)

type Command struct {
	Database string `cli:"required,short=d,env=PGT_DATABASE,help=database connection string"`
	Query    string `cli:"required,short=q"`
}

func NewCommand() *Command {
	return &Command{}
}

func (cmd *Command) Run(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, cmd.Database)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	if _, err := conn.Exec(ctx, `set plan_cache_mode = force_generic_plan`); err != nil {
		return err
	}

	if _, err := conn.Exec(ctx, fmt.Sprintf(`prepare q as %s`, cmd.Query)); err != nil {
		return err
	}

	var cardinality int
	if err := conn.QueryRow(ctx, `select cardinality(parameter_types) from pg_prepared_statements where name = 'q'`).Scan(&cardinality); err != nil {
		return err
	}

	var params string
	{
		var b strings.Builder
		for i := 0; i < cardinality; i++ {
			if i > 0 {
				b.WriteString(",")
			}
			b.WriteString("null")
		}
		params = b.String()
	}

	rows, err := conn.Query(ctx, fmt.Sprintf(`explain execute q(%s)`, params))
	if err != nil {
		return err
	}
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return err
		}
		fmt.Printf("%s\n", line)
	}

	return nil
}
