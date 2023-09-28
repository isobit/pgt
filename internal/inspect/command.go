package inspect

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/isobit/cli"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/sourcegraph/conc/pool"

	"github.com/isobit/pgt/internal/util"
)

type InspectCommand struct {
	ConnectAddr string `cli:"required,short=c"`
	ListenAddr  string `cli:"required,short=l"`
}

func NewInspectCommand() *InspectCommand {
	return &InspectCommand{}
}

func (*InspectCommand) SetupCommand(cmd *cli.Command) {
	cmd.SetHelp("proxy for inspecting the PostgreSQL protocol")
	cmd.SetDescription(`
		Inspects and logs PostgreSQL protocol messages for connections proxied
		between the listen address and the connect address.

		Example:

		    # start an inspect proxy
		    pgt inspect -l localhost:5433 -c localhost:5432
		    # connect via inspect proxy
		    psql postgres://localhost:5433
	`)
}

func (cmd *InspectCommand) Run(ctx context.Context) error {
	connectAddr, err := net.ResolveTCPAddr("tcp", cmd.ConnectAddr)
	if err != nil {
		return fmt.Errorf("invalid connect address: %w", err)
	}

	listenAddr, err := net.ResolveTCPAddr("tcp", cmd.ListenAddr)
	if err != nil {
		return fmt.Errorf("invalid listen address: %w", err)
	}

	proxy := inspectProxy{
		connectAddr: connectAddr,
		listenAddr:  listenAddr,
	}

	listener, err := net.ListenTCP("tcp", listenAddr)
	if err != nil {
		return err
	}
	go func() {
		<-ctx.Done()
		listener.Close()
	}()
	util.Logf(0, "listening: %s", listener.Addr())

	for {
		clientConn, err := listener.AcceptTCP()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			if clientConn != nil {
				util.Logf(-2, "accept error: %s: %s", clientConn.RemoteAddr(), err)
			} else {
				util.Logf(-2, "accept error: %s", err)
			}
			continue
		}
		go func() {
			if err := proxy.handleConn(clientConn); err != nil {
				util.Logf(-2, "handle error: %s: %s", clientConn.RemoteAddr(), err)
			}
		}()
	}
}

type inspectProxy struct {
	connectAddr *net.TCPAddr
	listenAddr  *net.TCPAddr
}

func (p *inspectProxy) handleConn(clientConn *net.TCPConn) error {
	defer clientConn.Close()

	clientAddr := clientConn.RemoteAddr()
	defer util.Logf(1, "closed: %s", clientAddr)

	backend := pgproto3.NewBackend(clientConn, clientConn)

	serverConn, err := net.DialTCP("tcp", nil, p.connectAddr)
	if err != nil {
		return fmt.Errorf("error connecting to server: %s", err)
	}

	frontend := pgproto3.NewFrontend(serverConn, serverConn)

	// Configure pgproto3's tracing to send trace messages using our logger.
	logWriter := newLogWriter(fmt.Sprintf("%s: ", clientConn.RemoteAddr()))
	defer logWriter.Close()
	frontend.Trace(logWriter, pgproto3.TracerOptions{
		SuppressTimestamps: true,
	})

	for inStartup := true; inStartup; {
		msg, err := backend.ReceiveStartupMessage()
		if err != nil {
			return fmt.Errorf("error receiving backend startup message: %s", err)
		}
		switch msg.(type) {
		case *pgproto3.SSLRequest:
			util.Logf(3, "client sent SSLRequest, responding not supported")
			if _, err := clientConn.Write([]byte{'N'}); err != nil {
				return fmt.Errorf("error sending deny SSL request: %w", err)
			}
		default:
			inStartup = false
			frontend.Send(msg)
			if err := frontend.Flush(); err != nil {
				return fmt.Errorf("error sending frontend startup message: %s", err)
			}
		}
	}

	inAuthExchange := true
	authTypeCh := make(chan uint32)

	// bidirectional copy loop
	tp := pool.New().WithErrors().WithFirstError()
	tp.Go(func() error {
		for {
			util.Logf(3, "server: receive")
			msg, err := frontend.Receive()
			if err != nil {
				return fmt.Errorf("error receiving frontend message: %s", err)
			}
			backend.Send(msg)
			if err := backend.Flush(); err != nil {
				return fmt.Errorf("error sending backend message: %s", err)
			}

			if inAuthExchange {
				authType := frontend.GetAuthType()
				util.Logf(3, "server: authtype: %+v", authType)
				if authType == pgproto3.AuthTypeOk {
					inAuthExchange = false
					close(authTypeCh)
				} else {
					authTypeCh <- authType
				}
			}
		}
	})
	tp.Go(func() error {
		for {
			if inAuthExchange {
				if authType, ok := <-authTypeCh; ok {
					util.Logf(3, "client: setting authtype %v", authType)
					backend.SetAuthType(authType)
				}
			}

			util.Logf(3, "client: receive")
			msg, err := backend.Receive()
			if err != nil {
				return fmt.Errorf("error receiving backend message: %s", err)
			}
			frontend.Send(msg)
			if err := frontend.Flush(); err != nil {
				return fmt.Errorf("error sending frontend message: %s", err)
			}
		}
	})
	return tp.Wait()
}

func newLogWriter(prefix string) io.WriteCloser {
	r, w := io.Pipe()
	go func() {
		s := bufio.NewScanner(r)
		for s.Scan() {
			util.Logf(0, "%s%s", prefix, s.Text())
		}
	}()
	return w
}
