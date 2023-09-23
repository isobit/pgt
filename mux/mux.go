package mux

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/isobit/pgt/util"
)

func Listen(ctx context.Context, database string, listenAddr string) error {
	pool, err := pgxpool.New(ctx, database)
	if err != nil {
		return err
	}

	cm := &Mux{
		pool:  pool,
		conns: map[string]*MuxConn{},
	}

	addr, err := net.ResolveTCPAddr("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("invalid address: %w", err)
	}

	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return err
	}
	defer listener.Close()
	util.Logf(0, "listening: %s", listener.Addr())

	go func() {
		<-ctx.Done()
		listener.Close()
	}()

	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			if conn != nil {
				util.Logf(-1, "accept error: %s: %s", conn.RemoteAddr(), err)
			} else {
				util.Logf(-1, "accept error: %s", err)
			}
			continue
		}
		go func() {
			defer conn.Close()

			remoteAddr := conn.RemoteAddr()
			util.Logf(1, "accepted: %s", remoteAddr)
			defer util.Logf(1, "closed: %s", remoteAddr)

			if err := cm.HandleConn(ctx, conn); err != nil {
				util.Logf(-1, "%s", err)
			}
		}()
	}
}

type MuxConn struct {
	sync.Mutex
	*pgxpool.Conn
	recvCh   chan pgproto3.BackendMessage
	refcount int
}

func NewMuxConn(conn *pgxpool.Conn) *MuxConn {
	frontend := conn.Conn().PgConn().Frontend()
	recvCh := make(chan pgproto3.BackendMessage)
	go func() {
		defer close(recvCh)
		for {
			msg, err := frontend.Receive()
			if err != nil {
				util.Logf(-1, "receive error: %s", err)
				return
			}
			recvCh <- msg
		}
	}()

	return &MuxConn{
		Conn:     conn,
		recvCh:   recvCh,
		refcount: 1,
	}
}

type Mux struct {
	sync.Mutex
	pool  *pgxpool.Pool
	conns map[string]*MuxConn
}

func (cm *Mux) acquire(ctx context.Context, key string) (*MuxConn, error) {
	cm.Lock()
	defer cm.Unlock()

	if conn, ok := cm.conns[key]; ok {
		conn.Lock()
		defer conn.Unlock()
		conn.refcount++
		return conn, nil
	}

	pgConn, err := cm.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	conn := NewMuxConn(pgConn)
	cm.conns[key] = conn
	return conn, nil
}

func (cm *Mux) release(ctx context.Context, key string) {
	cm.Lock()
	defer cm.Unlock()

	conn, ok := cm.conns[key]
	if !ok {
		return
	}

	conn.Lock()
	defer conn.Unlock()
	conn.refcount--
	if conn.refcount <= 0 {
		conn.Release()
		delete(cm.conns, key)
	}
}

func (cm *Mux) HandleConn(ctx context.Context, conn net.Conn) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	remoteAddr := conn.RemoteAddr()

	backend := pgproto3.NewBackend(conn, conn)

	util.Logf(3, "receiving startup message")
	msg, err := cm.handleStartup(conn, backend)
	if err != nil {
		return fmt.Errorf("error in startup: %w", err)
	}

	key, ok := msg.Parameters["database"]
	if !ok {
		key = remoteAddr.String()
	}
	util.Logf(3, "conn key: %s", key)
	pgConn, err := cm.acquire(ctx, key)
	if err != nil {
		return err
	}
	defer cm.release(ctx, key)

	backend.Send(&pgproto3.AuthenticationOk{})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if err := backend.Flush(); err != nil {
		return fmt.Errorf("error sending response to startup message: %w", err)
	}

	clientCh := make(chan pgproto3.FrontendMessage)
	go func() {
		defer close(clientCh)
		for {
			msg, err := backend.Receive()
			if err != nil {
				// return fmt.Errorf("backend receive error: %w", err)
				return
			}
			clientCh <- msg
		}
	}()

	frontend := pgConn.Conn.Conn().PgConn().Frontend()
	for {
		msg := <-clientCh
		if msg, ok := msg.(*pgproto3.Terminate); ok {
			util.Logf(3, "got terminate message: %+v", msg)
			return nil
		}
		util.Logf(3, "client -> server: %+v", msg)
		frontend.Send(msg)
		frontend.Flush()

		pgConn.Lock()

	L:
		for {
			select {
			case clientMsg := <-clientCh:
				util.Logf(3, "client -> server: %+v", clientMsg)
				frontend.Send(clientMsg)
				frontend.Flush()
			case serverMsg := <-pgConn.recvCh:
				util.Logf(3, "server -> client: %+v", serverMsg)
				backend.Send(serverMsg)
				backend.Flush()
				if _, ok := serverMsg.(*pgproto3.ReadyForQuery); ok {
					util.Logf(3, "got ReadyForQuery, breaking to yield")
					break L
				}
			}
		}

		pgConn.Unlock()
	}
}

func (cm *Mux) handleStartup(conn net.Conn, backend *pgproto3.Backend) (*pgproto3.StartupMessage, error) {
	msg, err := backend.ReceiveStartupMessage()
	if err != nil {
		return nil, fmt.Errorf("error receiving startup message: %w", err)
	}
	switch msg := msg.(type) {
	case *pgproto3.StartupMessage:
		util.Logf(3, "got StartupMessage: %+v", msg)
		return msg, nil
	case *pgproto3.SSLRequest:
		util.Logf(3, "got SSLRequest: %+v", msg)
		if _, err := conn.Write([]byte{'N'}); err != nil {
			return nil, fmt.Errorf("error sending deny SSL request: %w", err)
		}
		return cm.handleStartup(conn, backend)
	default:
		return nil, fmt.Errorf("invalid startup msg")
	}
}
