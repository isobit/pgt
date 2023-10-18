package mux

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/isobit/pgt/internal/util"
)

// Connection multiplexing is implemented as follows:
//
// 1. The client start-up flow is handled entirely by the proxy. Proper
//    authentication is not implemented yet, the proxy will accept any client.
//
// 2. Upstream server connections are established lazily per session.
//    Backend parameters from the upstream connection are forwarded during
//    client start-up (things like e.g. client_encoding are critical for some
//    clients).
//
// 3. When a command cycle initiating message is received from a client, it
//    will take exclusive control of the upstream connection for the duration
//    of the command cycle. If another client is performing a command cycle, it
//    will wait for that client to finish. The ReadyForQuery backend message
//    signals the end of a command cycle, so that's the yield point where
//    clients can alternate between using a shared session.
//
// 4. There is a separate mechanism which sends (broadcasts) asynchronous
//    operation backend messages like NoticeResponse, ParameterStatus, and
//    NotificationResponse to all clients as soon as they are received; this
//    happens regardless of any command cycles.
//
// 5. When clients disconnect, the upstream server connection reference count
//    is decremented. When the refcount reaches 0, the connection is not needed
//    by any clients, so it is closed.

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
			util.Logf(3, "accepted: %s", remoteAddr)
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
	recvAckCh     chan bool
	recvBroadcast *Broadcast[pgproto3.BackendMessage]
}

func NewMuxServerConn(conn *pgconn.HijackedConn) *MuxServerConn {
	frontend := conn.Frontend
	recvCh := make(chan pgproto3.BackendMessage)
	recvAckCh := make(chan bool)
	recvBroadcast := NewBroadcast[pgproto3.BackendMessage]()

	go func() {
		defer util.Logf(3, "server receiver close")
		defer close(recvCh)
		for {
			msg, err := frontend.Receive()
			if err != nil {
				if !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.ErrUnexpectedEOF) {
					util.Logf(-1, "server receive error: %s", err)
				}
				return
			}
			if util.LogLevel >= 3 {
				util.Logf(3, "server msg: %s %+v", reflect.TypeOf(msg), msg)
			}
			switch msg.(type) {
			case
				*pgproto3.NotificationResponse,
				*pgproto3.ParameterStatus,
				*pgproto3.NoticeResponse:

				util.Logf(3, "server broadcast")
				recvBroadcast.Send(msg)
			default:
				util.Logf(3, "server unicast")
				recvCh <- msg
				if _, ok := <-recvAckCh; !ok {
					return
				}
				util.Logf(3, "after server msg chan send")
			}
		}
	}()

	return &MuxServerConn{
		HijackedConn:  conn,
		Frontend:      frontend,
		refcount:      1,
		recvCh:        recvCh,
		recvAckCh:     recvAckCh,
		recvBroadcast: recvBroadcast,
	}
}

type MuxClientConn struct {
	net.Conn
	*pgproto3.Backend
	recvCh    chan pgproto3.FrontendMessage
	recvAckCh chan bool
}

func NewMuxClientConn(conn net.Conn) *MuxClientConn {
	backend := pgproto3.NewBackend(conn, conn)
	if util.LogLevel >= 4 {
		backend.Trace(util.Log, pgproto3.TracerOptions{})
	}

	recvCh := make(chan pgproto3.FrontendMessage)
	recvAckCh := make(chan bool)

	return &MuxClientConn{
		Conn:      conn,
		Backend:   backend,
		recvCh:    recvCh,
		recvAckCh: recvAckCh,
	}
}

func (conn *MuxClientConn) handleStartup() (*pgproto3.StartupMessage, error) {
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
		return conn.handleStartup()
	default:
		return nil, fmt.Errorf("invalid startup msg")
	}
}

func (c *MuxClientConn) startReceiver() {
	go func() {
		defer close(c.recvCh)
		defer util.Logf(3, "receiver closed: %s", c.Conn.RemoteAddr())
		for {
			msg, err := c.Receive()
			if err != nil {
				if !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.ErrUnexpectedEOF) {
					util.Logf(-1, "client receive error: %s", err)
				}
				return
			}
			c.recvCh <- msg
			if _, ok := <-c.recvAckCh; !ok {
				return
			}
		}
	}()
}

type Mux struct {
	sync.RWMutex
	connConfig *pgx.ConnConfig
	conns      map[string]*MuxServerConn
}

func (m *Mux) acquire(ctx context.Context, key string) (*MuxServerConn, error) {
	m.Lock()
	defer m.Unlock()
	conn, ok := m.conns[key]

	if ok {
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

	conn = NewMuxServerConn(hijackedPgConn)

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
		close(conn.recvAckCh)
		conn.Frontend.Send(&pgproto3.Terminate{})
		conn.Frontend.Flush()
		conn.Conn.Close()
		delete(m.conns, key)
	}
}

func (m *Mux) HandleConn(ctx context.Context, clientNetConn net.Conn) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	clientConn := NewMuxClientConn(clientNetConn)

	util.Logf(3, "receiving startup message")
	startupMsg, err := clientConn.handleStartup()
	if err != nil {
		return fmt.Errorf("error in startup: %w", err)
	}

	var key string
	if dbname, ok := startupMsg.Parameters["database"]; ok {
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
	clientConn.Send(&pgproto3.ReadyForQuery{TxStatus: serverConn.TxStatus})

	if err := clientConn.Flush(); err != nil {
		return fmt.Errorf("error sending response to startup message: %w", err)
	}

	broadcastCh := serverConn.recvBroadcast.Subscribe()
	defer serverConn.recvBroadcast.Unsubscribe(broadcastCh)
	go func() {
		defer util.Logf(3, "broadcast closed: %s", clientConn.RemoteAddr())
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
				serverConn.recvBroadcast.ack <- true
				return
			}
			serverConn.recvBroadcast.ack <- true
		}
	}()
	clientConn.startReceiver()
	defer close(clientConn.recvAckCh)

	for {
		msg, ok := <-clientConn.recvCh
		if !ok {
			util.Logf(3, "client closed")
			return nil
		}
		if msg, ok := msg.(*pgproto3.Terminate); ok {
			util.Logf(3, "got terminate message from client: %+v", msg)
			return nil
		}
		clientConn.recvAckCh <- true
		handleCommandCycle(clientConn, serverConn, msg)
	}
}

func handleCommandCycle(clientConn *MuxClientConn, serverConn *MuxServerConn, initialMsg pgproto3.FrontendMessage) error {
	serverConn.Lock()
	defer serverConn.Unlock()

	util.Logf(3, "command cycle: begin for client %s", clientConn.RemoteAddr())
	defer util.Logf(3, "command cycle: end for client %s", clientConn.RemoteAddr())

	if util.LogLevel >= 3 {
		util.Logf(3, "client[%s] -> server[%d]: %s %+v", clientConn.RemoteAddr(), serverConn.PID, reflect.TypeOf(initialMsg), initialMsg)
	}
	serverConn.Send(initialMsg)
	if err := serverConn.Flush(); err != nil {
		return err
	}

	for {
		select {
		case clientMsg, ok := <-clientConn.recvCh:
			if !ok {
				util.Logf(3, "command cycle: client closed")
				break
			}
			if util.LogLevel >= 3 {
				util.Logf(3, "client[%s] -> server[%d]: %s %+v", clientConn.RemoteAddr(), serverConn.PID, reflect.TypeOf(clientMsg), clientMsg)
			}
			serverConn.Send(clientMsg)
			if err := serverConn.Flush(); err != nil {
				clientConn.recvAckCh <- true
				return err
			}
			clientConn.recvAckCh <- true
		case serverMsg, ok := <-serverConn.recvCh:
			if !ok {
				util.Logf(3, "command cycle: server closed")
				return nil
			}
			if util.LogLevel >= 3 {
				util.Logf(3, "server[%d] -> client[%s]: %s %+v", serverConn.PID, clientConn.RemoteAddr(), reflect.TypeOf(serverMsg), serverMsg)
			}
			clientConn.Send(serverMsg)
			if err := clientConn.Flush(); err != nil {
				serverConn.recvAckCh <- true
				return err
			}
			serverConn.recvAckCh <- true
			if _, ok := serverMsg.(*pgproto3.ReadyForQuery); ok {
				return nil
			}
		}
	}
}
