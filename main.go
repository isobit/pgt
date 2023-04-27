package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/Masterminds/sprig/v3"
	"github.com/isobit/cli"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	cmd := cli.New("dbench", &Cmd{
		J: 1,
		N: 1,
		R: 1,
	})
	cmd.Parse().RunFatal()
}

type Cmd struct {
	Database         string `cli:"required,short=d,env=DATABASE_URL"`
	TemplateFilename string `cli:"short=t"`
	J                int    `cli:"help=number of concurrent connections/goroutines"`
	N                int    `cli:"help=number of transactions per connection"`
	R                int    `cli:"help=number of rows per transaction"`
	// Data map[string]string
}

func warmPool(ctx context.Context, pool *pgxpool.Pool) error {
	maxConns := pool.Config().MaxConns
	conns := make([]*pgxpool.Conn, maxConns)
	wg := sync.WaitGroup{}
	for i := int32(0); i < maxConns; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := pool.Acquire(ctx)
			if err != nil {
				panic(err)
			}
			conns[i] = conn
			if err := conn.Ping(ctx); err != nil {
				panic(err)
			}
		}()
	}
	wg.Wait()
	for _, conn := range conns {
		conn.Release()
	}
	return nil
}

func (cmd *Cmd) Run() error {
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	tmpl, err := readTemplate(cmd.TemplateFilename)
	if err != nil {
		return err
	}
	initTmpl := tmpl.Lookup("init")
	infoTmpl := tmpl.Lookup("info")

	poolCfg, err := pgxpool.ParseConfig(cmd.Database)
	if err != nil {
		return err
	}
	poolCfg.MaxConns = int32(cmd.J)
	logf("initializing connection pool")
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := warmPool(ctx, pool); err != nil {
		return err
	}

	r := runner{
		name:     fmt.Sprintf("bench_%s", time.Now().Format("2006_01_02T15_04_05")),
		r:        cmd.R,
		txnTmpl:  tmpl,
		initTmpl: initTmpl,
		pool:     pool,
		stats:    &statTracker{},
	}

	if infoTmpl != nil {
		r.infoTmpl = infoTmpl

		infoConn, err := pgx.Connect(ctx, cmd.Database)
		if err != nil {
			return err
		}
		r.infoConn = infoConn
	}

	if err := r.executeInit(ctx); err != nil {
		return err
	}

	go func() {
		infoTicker := time.NewTicker(1 * time.Second)
		for {
			select {
			case <-ctx.Done():
				return
			case <-infoTicker.C:
				r.logInfo(context.TODO())
			}
		}
	}()

	logf("starting benchmark")
	wg := sync.WaitGroup{}
	for j := 0; j < cmd.J; j++ {
		wg.Add(1)
		j := j
		go func() {
			defer wg.Done()
			for i := 0; i < cmd.N; i++ {
				if err := r.executeTxn(ctx, j, i); err != nil {
					panic(err)
				}
			}
		}()
	}
	wg.Wait()

	r.logInfo(ctx)

	r.stats.Lock()
	tps := float64(r.stats.cumulativeCount) / r.stats.cumulativeTotal.Seconds()
	avg := time.Duration(float64(r.stats.cumulativeTotal) / float64(r.stats.cumulativeCount))
	fmt.Printf("summary tps %.2f avg %s\n", tps, avg)

	return nil
}

type initTmplData struct {
	Name string
}

type txnTmplData struct {
	Name string
	I    int
	J    int
	R    int
}

type runner struct {
	name string
	r    int

	initTmpl *template.Template
	txnTmpl  *template.Template
	infoTmpl *template.Template

	pool     *pgxpool.Pool
	infoConn *pgx.Conn

	stats *statTracker
}

func (r *runner) executeInit(ctx context.Context) error {
	if r.initTmpl == nil {
		return nil
	}

	logf("running init")
	initSql, err := execTemplate(r.initTmpl, initTmplData{
		Name: r.name,
	})
	if err != nil {
		return err
	}
	if _, err := r.pool.Exec(ctx, initSql); err != nil {
		logf(initSql)
		return err
	}
	return nil
}

func (r *runner) executeTxn(ctx context.Context, j int, i int) error {
	txnSql, err := execTemplate(r.txnTmpl, txnTmplData{
		Name: r.name,
		I:    i,
		J:    j,
		R:    r.r,
	})
	if err != nil {
		return err
	}

	conn, err := r.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	start := time.Now()
	if _, err := conn.Exec(ctx, txnSql); err != nil {
		logf(txnSql)
		return err
	}
	elapsed := time.Since(start)
	conn.Release()

	// logf("elapsed: %s", elapsed)
	r.stats.record(elapsed)

	return nil
}

func (r *runner) logInfo(ctx context.Context) {
	statInfo := r.stats.flush()
	if statInfo.count == 0 {
		return
	}

	defer fmt.Printf("\n")
	// fmt.Printf("t=%d n=%d c=%d tps=%0.2f avg=%s", time.Now().Unix(), statInfo.cumulativeCount, statInfo.count, statInfo.tps, statInfo.avg)
	fmt.Printf("t %d n %d c %d tps %0.2f avg %s", time.Now().Unix(), statInfo.cumulativeCount, statInfo.count, statInfo.tps, statInfo.avg)

	if r.infoTmpl == nil || r.infoConn == nil {
		return
	}

	infoSql, err := execTemplate(r.infoTmpl, initTmplData{
		Name: r.name,
	})
	if err != nil {
		panic(err)
	}
	rows, err := r.infoConn.Query(ctx, infoSql)
	if err != nil {
		logf(infoSql)
		panic(err)
	}
	defer rows.Close()
	for rows.Next() {
		if err := rows.Err(); err != nil {
			panic(err)
		}
		fields := []string{}
		for _, fd := range rows.FieldDescriptions() {
			fields = append(fields, fd.Name)
		}
		values, err := rows.Values()
		if err != nil {
			panic(err)
		}
		for i, v := range values {
			// fmt.Printf(" %s=%v", fields[i], v)
			fmt.Printf(" %s %v", fields[i], v)
		}
	}
}

type statTracker struct {
	sync.Mutex
	cumulativeCount int
	cumulativeTotal time.Duration
	count           int
	total           time.Duration
}

func (s *statTracker) record(t time.Duration) {
	s.Lock()
	defer s.Unlock()
	s.cumulativeCount += 1
	s.cumulativeTotal += t
	s.count += 1
	s.total += t
}

type statInfo struct {
	cumulativeCount int
	count           int
	tps             float64
	avg             time.Duration
}

func (s *statTracker) flush() statInfo {
	s.Lock()
	defer s.Unlock()

	count := s.count
	tps := float64(count) / s.total.Seconds()
	avg := time.Duration(float64(s.total) / float64(count))

	s.count = 0
	s.total = 0

	return statInfo{
		cumulativeCount: s.cumulativeCount,
		count:           count,
		tps:             tps,
		avg:             avg,
	}
}

func readTemplate(filename string) (*template.Template, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	t := template.New("")
	t.Funcs(sprig.TxtFuncMap())
	return t.Parse(string(data))
}

func execTemplate(tmpl *template.Template, data any) (string, error) {
	b := strings.Builder{}
	if err := tmpl.Execute(&b, data); err != nil {
		return "", err
	}
	return b.String(), nil
}

// func execTemplate(tmpl *template.Template, name string, data any) (string, error) {
// 	b := strings.Builder{}
// 	if err := tmpl.ExecuteTemplate(&b, name, data); err != nil {
// 		return "", err
// 	}
// 	return b.String(), nil
// }

func logf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
}
