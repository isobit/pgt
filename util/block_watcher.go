package util

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type BlockWatcherCommand struct {
	Database               string        `cli:"short=d"`
	MaxBlockedWaitDuration time.Duration `cli:"short=m"`

	Query string
}

func (cmd *BlockWatcherCommand) Run(ctx context.Context) error {
	pool, err := pgxpool.New(ctx, cmd.Database)
	if err != nil {
		return err
	}

	conn, err := AcquireWithBlockWatcher(ctx, pool, cmd.MaxBlockedWaitDuration)
	if err != nil {
		return err
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, cmd.Query); err != nil {
		return err
	}

	return nil
}

type BlockWatchedPoolConn struct {
	*pgxpool.Conn
	watcherCancel context.CancelFunc
	watcherDone   chan bool
}

func (c *BlockWatchedPoolConn) Release() {
	c.watcherCancel()
	<-c.watcherDone
	c.Conn.Release()
}

func AcquireWithBlockWatcher(ctx context.Context, pool *pgxpool.Pool, maxBlockDuration time.Duration) (*BlockWatchedPoolConn, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}

	var pid int
	row := conn.QueryRow(ctx, `select pg_backend_pid()`)
	if err := row.Scan(&pid); err != nil {
		conn.Release()
		return nil, err
	}

	watcher := BlockWatcher{
		pool:             pool,
		pid:              pid,
		maxBlockDuration: maxBlockDuration,
	}

	watcherDone := make(chan bool)
	watcherCtx, watcherCancel := context.WithCancel(ctx)
	go func() {
		for retry := 0; retry < 3; retry++ {
			if err := watcher.Watch(watcherCtx); err != nil {
				Logf(-1, "error in block watcher: %s", err)
				continue
			}
			watcherDone <- true
			return
		}
		panic(fmt.Sprintf("block watcher errored too many times, aborting: %s", err))
	}()

	return &BlockWatchedPoolConn{
		Conn:          conn,
		watcherCancel: watcherCancel,
		watcherDone:   watcherDone,
	}, nil
}

type BlockWatcher struct {
	pool             *pgxpool.Pool
	pid              int
	maxBlockDuration time.Duration
}

func (w *BlockWatcher) Watch(ctx context.Context) error {
	Logf(2, "block watcher started for %d", w.pid)
	defer Logf(2, "block watcher cancelled for %d", w.pid)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			checkCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
			if err := w.check(checkCtx); err != nil {
				cancel()
				return err
			}
			cancel()
		}
	}
}

func (w *BlockWatcher) check(ctx context.Context) error {
	Logf(3, "checking if %d is blocking processes", w.pid)
	rows, err := w.pool.Query(
		ctx,
		`
		select pid
		from pg_locks
		where
			not granted
			and $1 = any(pg_blocking_pids(pid))
			and waitstart < now() - $2::interval
		group by (pid)
		`,
		w.pid,
		w.maxBlockDuration,
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	blockedPids, err := pgx.CollectRows(rows, pgx.RowTo[int])
	if err != nil {
		return err
	}
	if blockedPids == nil || len(blockedPids) == 0 {
		return nil
	}

	pidStrs := make([]string, len(blockedPids))
	for i, pid := range blockedPids {
		pidStrs[i] = fmt.Sprintf("%d", pid)
	}

	Logf(
		-1, "cancelling backend %d; other processes were blocked waiting on it for more than %s: %s",
		w.pid, w.maxBlockDuration, strings.Join(pidStrs, ", "),
	)
	if err := w.cancelBackend(ctx); err != nil {
		return err
	}

	return nil
}

func (w *BlockWatcher) cancelBackend(ctx context.Context) error {
	if _, err := w.pool.Exec(ctx, `select pg_cancel_backend($1)`, w.pid); err != nil {
		return err
	}
	return nil
}
