package tui

//import (
//	"context"
//	"errors"
//	"fmt"

//	// "time"

//	"github.com/jackc/pgx/v5/pgxpool"

//	// "github.com/gdamore/tcell/v2"
//	// "github.com/rivo/tview"

//	"github.com/awesome-gocui/gocui"
//	// "github.com/isobit/pgt/internal/util"
//)

//type Command struct {
//	Database string `cli:"required,short=d,env=PGT_DATABASE,help=database connection string"`
//}

//func New() *Command {
//	return &Command{}
//}

//// func NewExecCommand() *ExecCommand {
//// 	return &ExecCommand{
//// 		MaxBlockDuration:  10 * time.Second,
//// 		MaxBlockProcesses: 0,
//// 	}
//// }

////func (cmd *Command) Run(ctx context.Context) error {
////	pool, err := pgxpool.New(ctx, cmd.Database)
////	if err != nil {
////		return err
////	}
////	defer pool.Close()

////	// box := tview.NewBox().SetBorder(true).SetTitle("Hello, world!")
////	// if err := tview.NewApplication().SetRoot(box, true).Run(); err != nil {
////	// 	panic(err)
////	// }

////	table := tview.NewTable()
////	// table.SetBorders(true)
////	// table.SetBordersColor(tcell.ColorBlue)
////	//
////	table.SetSelectable(true, true)
////	table.SetSelectedFunc(func(row int, column int) {
////		table.GetCell(row, 0).SetTextColor(tcell.ColorRed)
////		// table.SetSelectable(false, false)
////	})
////	table.SetDoneFunc(func(key tcell.Key) {
////		if key == tcell.KeyEscape {
////			row, _ := table.GetSelection()
////			table.GetCell(row, 0).SetTextColor(tcell.ColorDefault)
////		}
////		if key == tcell.KeyEnter {
////			// table.SetSelectable(true, true)
////		}
////	})

////	table.SetFixed(1, 0)
////	table.SetCell(0, 0, tview.NewTableCell("queryid"))
////	table.SetCell(0, 1, tview.NewTableCell("query"))
////	table.SetCell(0, 2, tview.NewTableCell("calls"))
////	table.SetCell(0, 3, tview.NewTableCell("rows"))
////	table.SetCell(0, 4, tview.NewTableCell("total_exec_time"))
////	table.SetCell(0, 5, tview.NewTableCell("mean_exec_time"))

////	r, err := pool.Query(ctx, `
////		select queryid, query, calls, rows, total_exec_time, mean_exec_time
////		from pg_stat_statements
////		order by total_exec_time desc
////		limit 50
////	`)
////	if err != nil {
////		return err
////	}
////	for i := 1; r.Next(); i++ {
////		var queryid int
////		var query string
////		var calls int
////		var rows int
////		var totalExecTime float64
////		var meanExecTime float64
////		if err := r.Scan(&queryid, &query, &calls, &rows, &totalExecTime, &meanExecTime); err != nil {
////			return err
////		}
////		// util.Logf(1, "queryid=%d", queryid)
////		table.SetCell(i, 0, tview.NewTableCell(fmt.Sprintf("%d", queryid)))
////		table.SetCell(i, 1, tview.NewTableCell(fmt.Sprintf("%s", query)))
////		table.SetCell(i, 2, tview.NewTableCell(fmt.Sprintf("%d", calls)))
////		table.SetCell(i, 3, tview.NewTableCell(fmt.Sprintf("%d", rows)))
////		table.SetCell(i, 4, tview.NewTableCell(fmt.Sprintf("%f", totalExecTime)))
////		table.SetCell(i, 5, tview.NewTableCell(fmt.Sprintf("%f", meanExecTime)))
////	}
////	// rows.Close()

////	app := tview.NewApplication()
////	app.SetRoot(table, true)
////	app.SetFocus(table)

////	app.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
////		// k := event.Key()
////		r := event.Rune()
////		switch {
////		// case k == tcell.KeyTab:
////		// 	switch app.GetFocus() {
////		// 	case logView:
////		// 		app.SetFocus(tree)
////		// 	case tree:
////		// 		app.SetFocus(logView)
////		// 	}
////		case r == 'q':
////			app.Stop()
////		default:
////			return event
////		}
////		return nil
////	})

////	return app.Run()
////}

//func (cmd *Command) Run(ctx context.Context) error {
//	g, err := gocui.NewGui(gocui.OutputNormal, true)
//	if err != nil {
//		return err
//	}
//	defer g.Close()

//	g.SetManagerFunc(layout)
//	g.SetKeybinding("", 'q', gocui.ModNone, func(g *gocui.Gui, v *gocui.View) error {
//		return gocui.ErrQuit
//	})

//	// if err := g.SetKeybinding("", gocui.KeyCtrlC, gocui.ModNone, quit); err != nil {
//	// 	log.Panicln(err)
//	// }

//	pool, err := pgxpool.New(ctx, cmd.Database)
//	if err != nil {
//		return err
//	}
//	defer pool.Close()

//	r, err := pool.Query(ctx, `
//		select queryid, query, calls, rows, total_exec_time, mean_exec_time
//		from pg_stat_statements
//		order by total_exec_time desc
//		limit 50
//	`)
//	if err != nil {
//		return err
//	}
//	for i := 1; r.Next(); i++ {
//		var queryid int
//		var query string
//		var calls int
//		var rows int
//		var totalExecTime float64
//		var meanExecTime float64
//		if err := r.Scan(&queryid, &query, &calls, &rows, &totalExecTime, &meanExecTime); err != nil {
//			return err
//		}
//	}

//	if err := g.MainLoop(); err != nil && !errors.Is(err, gocui.ErrQuit) {
//		return err
//	}
//	return nil
//}

//func layout(g *gocui.Gui) error {
//	maxX, maxY := g.Size()
//	if v, err := g.SetView("hello", 0, 0, maxX/3, maxY-1, 0); err != nil {
//		if !errors.Is(err, gocui.ErrUnknownView) {
//			return err
//		}

//		if _, err := g.SetCurrentView("hello"); err != nil {
//			return err
//		}

//		v.Title = "Test"
//		fmt.Fprintln(v, "Hello world!")
//	}

//	return nil
//}
