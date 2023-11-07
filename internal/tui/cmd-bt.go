package tui

import (
	"context"
	"fmt"

	// "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/charmbracelet/bubbles/table"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type Command struct {
	Database string `cli:"required,short=d,env=PGT_DATABASE,help=database connection string"`
}

func New() *Command {
	return &Command{}
}

func (cmd *Command) Run(ctx context.Context) error {
	columns := []table.Column{
		{Title: "queryid", Width: 10},
		{Title: "query", Width: 25},
		{Title: "calls", Width: 5},
		{Title: "rows", Width: 5},
		{Title: "total_exec_time", Width: 7},
		{Title: "mean_exec_time", Width: 7},
	}
	rows := []table.Row{}

	t := table.New(
		table.WithColumns(columns),
		table.WithRows(rows),
		table.WithFocused(true),
		table.WithHeight(7),
	)
	t.SetStyles(tableStyles)

	pool, err := pgxpool.New(ctx, cmd.Database)
	if err != nil {
		return err
	}
	defer pool.Close()

	m, err := newAppModel(ctx, pool)
	if err != nil {
		return err
	}
	if _, err := tea.NewProgram(m).Run(); err != nil {
		return err
	}
	return nil
}


var baseStyle = lipgloss.NewStyle().
	BorderStyle(lipgloss.NormalBorder()).
	BorderForeground(lipgloss.Color("240"))

var (
	tableStyles table.Styles
)

func init() {
	tableStyles = table.DefaultStyles()
	tableStyles.Header = tableStyles.Header.
		BorderStyle(lipgloss.NormalBorder()).
		BorderForeground(lipgloss.Color("240")).
		BorderBottom(true).
		Bold(false)
	tableStyles.Selected = tableStyles.Selected.
		Foreground(lipgloss.Color("15")).
		Background(lipgloss.Color("6")).
		Bold(false)
}

type appModel struct {
	ctx context.Context
	pool *pgxpool.Pool
	queryTable *queryTableModel
	showExplain bool
	explain string
}

func newAppModel(ctx context.Context, pool *pgxpool.Pool) (*appModel, error) {
	query := `
		select queryid, query, calls, rows, total_exec_time, mean_exec_time
		from pg_stat_statements
		order by total_exec_time desc
		limit 50
	`
	t, err := newQueryTableModel(ctx, pool, query)
	if err != nil {
		return nil, err
	}
	return &appModel{
		ctx: ctx,
		pool: pool,
		queryTable: t,
	}, nil
}

func (m *appModel) Init() tea.Cmd {
	return nil
}

func (m *appModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "esc":
			if m.showExplain {
				m.showExplain = false
			}
		case "enter":
			explainQuery := fmt.Sprintf("explain (generic_plan) %s", m.queryTable.selectedRow()[1])
			cmd = tea.Batch(cmd, tea.Printf(explainQuery))
			conn, err := m.pool.Acquire(m.ctx)
			if err != nil {
				return m, tea.Batch(cmd, tea.Printf("error: %s", err))
			}
			defer conn.Release()
			pgConn := conn.Conn().PgConn()
			mrr := pgConn.Exec(m.ctx, explainQuery)
			r, err := mrr.ReadAll()
			if err != nil {
				return m, tea.Batch(cmd, tea.Printf("error: %s", err))
			}
			cmd = tea.Batch(cmd, tea.Printf("%+v", r[0]))
			m.explain = ""
			for _, row := range r[0].Rows {
				m.explain = m.explain + "\n" + string(row[0])
			}
			m.showExplain = true
			// if err := r.Scan(&m.explain); err != nil {
			// 	return m, tea.Batch(cmd, tea.Printf("error: %s", err))
			// }
			// m.showExplain = true
		case "q", "ctrl+c":
			return m, tea.Quit

		}
	}
	m.queryTable.Update(msg)
	return m, cmd
}

func (m *appModel) View() string {
	var view string
	if m.showExplain {
		view = m.explain
	} else {
		view = m.queryTable.View()
	}
	return baseStyle.Render(view) + "\n"
}

type queryTableModel struct {
	ctx context.Context
	pool *pgxpool.Pool
	table table.Model
	query string
}

func newQueryTableModel(ctx context.Context, pool *pgxpool.Pool, query string) (*queryTableModel, error) {
	t := table.New(
		table.WithFocused(true),
		table.WithHeight(7),
	)
	t.SetStyles(tableStyles)

	m := &queryTableModel{
		ctx: ctx,
		pool: pool,
		table: t,
		query: query,
	}
	if err := m.fetch(); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *queryTableModel) Init() tea.Cmd {
	return nil
}

func (m *queryTableModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		// case "enter":
		// 	return m, tea.Batch(
		// 		tea.Printf("Let's go to %s!", m.table.SelectedRow()[1]),
		// 	)
		case "r":
			if err := m.fetch(); err != nil {
				tea.Println(err)
			}
		}
	}
	m.table, _ = m.table.Update(msg)
	return m, cmd
}

func (m *queryTableModel) View() string {
	return m.table.View()
}

func (m *queryTableModel) selectedRow() table.Row {
	return m.table.SelectedRow()
}

func (m *queryTableModel) fetch() error {
	r, err := m.pool.Query(m.ctx, m.query)
	if err != nil {
		return err
	}

	cols := []table.Column{}
	for _, fd := range r.FieldDescriptions() {
		width := 10
		if fd.Name == "query" {
			width = 25
		}
		cols = append(cols, table.Column{
			Title: fd.Name,
			Width: width,
		})
	}
	m.table.SetColumns(cols)

	rows := []table.Row{}
	for r.Next() {
		vals, err := r.Values()
		if err != nil {
			return err
		}
		row := table.Row{}
		for _, val := range vals {
			row = append(row, fmt.Sprintf("%v", val))
		}
		rows = append(rows, row)
	}
	m.table.SetRows(rows)
	return nil
}
