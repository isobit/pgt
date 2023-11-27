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
	JSON     bool
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

	format := "text"
	if cmd.JSON {
		format = "json"
	}

	plan, err := genericExplain(ctx, conn, cmd.Query, format)
	if err != nil {
		return err
	}

	for _, line := range plan {
		fmt.Println(line)
	}

	return nil
}

func genericExplain(ctx context.Context, conn *pgx.Conn, query string, format string) ([]string, error) {
	if _, err := conn.Exec(ctx, `set plan_cache_mode = force_generic_plan`); err != nil {
		return nil, err
	}

	if _, err := conn.Exec(ctx, `prepare q as ` + query); err != nil {
		return nil, err
	}

	var cardinality int
	if err := conn.
		QueryRow(ctx, `select cardinality(parameter_types) from pg_prepared_statements where name = 'q'`).
		Scan(&cardinality); err != nil {
		return nil, err
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

	rows, err := conn.Query(ctx, fmt.Sprintf(`explain (format %s) execute q(%s)`, format, params))
	if err != nil {
		return nil, err
	}
	plan := []string{}
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return nil, err
		}
		plan = append(plan, line)
	}

	return plan, nil
}
