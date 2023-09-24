package mux

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"sync"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/isobit/pgt/util"
)

func Listen(ctx context.Context, database string, listenAddr string) error {
	pgCfg, err := pgx.ParseConfig(database)
	if err != nil {
		return err
	}

	cm := &Mux{
		connConfig: pgCfg,
		conns:      map[string]*MuxServerConn{},
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
				util.Logf(-2, "accept error: %s: %s", conn.RemoteAddr(), err)
			} else {
				util.Logf(-2, "accept error: %s", err)
			}
			continue
		}
		go func() {
			defer conn.Close()

			remoteAddr := conn.RemoteAddr()
			util.Logf(2, "accepted: %s", remoteAddr)
			defer util.Logf(1, "closed: %s", remoteAddr)

			if err := cm.HandleConn(ctx, conn); err != nil {
				util.Logf(-2, "error handling %s: %s", remoteAddr, err)
			}
		}()
	}
}

type MuxServerConn struct {
	sync.Mutex
	*pgconn.HijackedConn
	*pgproto3.Frontend
	refcount      int
	recvCh        <-chan pgproto3.BackendMessage
	recvBroadcast *Broadcast[pgproto3.BackendMessage]
}

func NewMuxServerConn(conn *pgconn.HijackedConn) *MuxServerConn {
	frontend := conn.Frontend
	recvCh := make(chan pgproto3.BackendMessage)
	recvBroadcast := NewBroadcast[pgproto3.BackendMessage]()

	go func() {
		defer close(recvCh)
		for {
			msg, err := frontend.Receive()
			if err != nil {
				if !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.ErrUnexpectedEOF) {
					util.Logf(-1, "server receive error: %s", err)
				}
				return
			}
			switch msg.(type) {
			case
				*pgproto3.NotificationResponse,
				*pgproto3.ParameterStatus,
				*pgproto3.NoticeResponse:

				recvBroadcast.Send(msg)
			default:
				recvCh <- msg
			}
		}
	}()

	return &MuxServerConn{
		HijackedConn:  conn,
		Frontend:      frontend,
		refcount:      1,
		recvCh:        recvCh,
		recvBroadcast: recvBroadcast,
	}
}

type MuxClientConn struct {
	net.Conn
	*pgproto3.Backend
	recvCh chan pgproto3.FrontendMessage
}

func NewMuxClientConn(conn net.Conn) *MuxClientConn {
	backend := pgproto3.NewBackend(conn, conn)
	if util.LogLevel >= 4 {
		backend.Trace(util.Log, pgproto3.TracerOptions{})
	}
	recvCh := make(chan pgproto3.FrontendMessage)

	return &MuxClientConn{
		Conn:    conn,
		Backend: backend,
		recvCh:  recvCh,
	}
}

func (c *MuxClientConn) startReceiver() {
	go func() {
		defer close(c.recvCh)
		for {
			msg, err := c.Receive()
			if err != nil {
				if !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.ErrUnexpectedEOF) {
					util.Logf(-1, "client receive error: %s", err)
				}
				return
			}
			c.recvCh <- msg
		}
	}()
}

type Mux struct {
	sync.Mutex
	connConfig *pgx.ConnConfig
	conns      map[string]*MuxServerConn
}

func (m *Mux) acquire(ctx context.Context, key string) (*MuxServerConn, error) {
	m.Lock()
	defer m.Unlock()

	if conn, ok := m.conns[key]; ok {
		conn.Lock()
		defer conn.Unlock()
		conn.refcount++
		return conn, nil
	}

	pgxConn, err := pgx.ConnectConfig(ctx, m.connConfig)
	if err != nil {
		return nil, err
	}
	pgConn := pgxConn.PgConn()
	if err := pgConn.SyncConn(ctx); err != nil {
		return nil, err
	}
	hijackedPgConn, err := pgConn.Hijack()
	if err != nil {
		return nil, err
	}

	conn := NewMuxServerConn(hijackedPgConn)
	m.conns[key] = conn
	return conn, nil
}

func (m *Mux) release(ctx context.Context, key string) {
	m.Lock()
	defer m.Unlock()

	conn, ok := m.conns[key]
	if !ok {
		return
	}

	conn.Lock()
	defer conn.Unlock()
	conn.refcount--
	if conn.refcount <= 0 {
		delete(m.conns, key)
	}
}

func (m *Mux) HandleConn(ctx context.Context, clientNetConn net.Conn) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	clientConn := NewMuxClientConn(clientNetConn)

	util.Logf(3, "receiving startup message")
	msg, err := handleStartup(clientConn)
	if err != nil {
		return fmt.Errorf("error in startup: %w", err)
	}

	var key string
	if dbname, ok := msg.Parameters["database"]; ok {
		if _, keypart, ok := strings.Cut(dbname, "@"); ok {
			key = keypart
		}
	}
	if key == "" {
		key = clientConn.RemoteAddr().String()
	}
	util.Logf(1, "accepted %s with key %s", clientConn.RemoteAddr(), key)

	serverConn, err := m.acquire(ctx, key)
	if err != nil {
		clientConn.Send(&pgproto3.ErrorResponse{Message: err.Error()})
		clientConn.Flush()
		return err
	}
	defer m.release(ctx, key)

	util.Logf(3, "sending AuthenticationOk")
	clientConn.Send(&pgproto3.AuthenticationOk{})

	util.Logf(3, "sending ParameterStatus")
	serverConn.Lock()
	for name, val := range serverConn.ParameterStatuses {
		util.Logf(3, "sending ParameterStatus{Name: %s, Value: %s}", name, val)
		clientConn.Send(&pgproto3.ParameterStatus{Name: name, Value: val})
	}
	serverConn.Unlock()

	util.Logf(3, "sending ReadyForQuery")
	clientConn.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})

	if err := clientConn.Flush(); err != nil {
		return fmt.Errorf("error sending response to startup message: %w", err)
	}

	go func() {
		broadcastCh := serverConn.recvBroadcast.Subscribe()
		defer serverConn.recvBroadcast.Unsubscribe(broadcastCh)
		for {
			msg, ok := <-broadcastCh
			if !ok {
				return
			}
			if util.LogLevel >= 3 {
				util.Logf(3, "server -> client: broadcast: %s %+v", reflect.TypeOf(msg), msg)
			}
			clientConn.Send(msg)
			if err := clientConn.Flush(); err != nil {
				return
			}
		}
	}()
	clientConn.startReceiver()

	for {
		msg, ok := <-clientConn.recvCh
		if !ok {
			util.Logf(3, "client closed")
			return nil
		}
		if msg, ok := msg.(*pgproto3.Terminate); ok {
			util.Logf(3, "got terminate message: %+v", msg)
			return nil
		}
		handleCommandCycle(clientConn, serverConn, msg)
	}
}

func handleCommandCycle(clientConn *MuxClientConn, serverConn *MuxServerConn, initialMsg pgproto3.FrontendMessage) error {
	serverConn.Lock()
	defer serverConn.Unlock()

	util.Logf(3, "begin command cycle for client %s", clientConn.RemoteAddr())
	defer util.Logf(3, "end command cycle for client %s", clientConn.RemoteAddr())

	if util.LogLevel >= 3 {
		util.Logf(3, "client -> server: %s %+v", reflect.TypeOf(initialMsg), initialMsg)
	}
	serverConn.Send(initialMsg)
	if err := serverConn.Flush(); err != nil {
		return err
	}

	for {
		select {
		case clientMsg, ok := <-clientConn.recvCh:
			if !ok {
				util.Logf(3, "client closed")
				return nil
			}
			if util.LogLevel >= 3 {
				util.Logf(3, "client -> server: %s %+v", reflect.TypeOf(clientMsg), clientMsg)
			}
			serverConn.Send(clientMsg)
			serverConn.Flush()
		case serverMsg, ok := <-serverConn.recvCh:
			if !ok {
				util.Logf(3, "server closed")
				return nil
			}
			if util.LogLevel >= 3 {
				util.Logf(3, "server -> client: %s %+v", reflect.TypeOf(serverMsg), serverMsg)
			}
			clientConn.Send(serverMsg)
			clientConn.Flush()
			if _, ok := serverMsg.(*pgproto3.ReadyForQuery); ok {
				util.Logf(3, "got ReadyForQuery")
				return nil
			}
		}
	}
}

func handleStartup(conn *MuxClientConn) (*pgproto3.StartupMessage, error) {
	msg, err := conn.ReceiveStartupMessage()
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
		return handleStartup(conn)
	default:
		return nil, fmt.Errorf("invalid startup msg")
	}
}
