package inspect

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/isobit/pgt/internal/util"
)

type InspectCommand struct {
	ConnectAddr string `cli:"required,short=c"`
	ListenAddr  string `cli:"required,short=l"`
	Timestamps  bool   `cli:"short=t"`
}

func NewInspectCommand() *InspectCommand {
	return &InspectCommand{}
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

	listener, err := net.ListenTCP("tcp", listenAddr)
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
			defer clientConn.Close()

			clientAddr := clientConn.RemoteAddr()
			defer util.Logf(1, "closed: %s", clientAddr)

			backend := pgproto3.NewBackend(clientConn, clientConn)

			serverConn, err := net.DialTCP("tcp", nil, connectAddr)
			if err != nil {
				util.Logf(-2, "error connecting to server: %s", err)
				return
			}

			frontend := pgproto3.NewFrontend(serverConn, serverConn)
			frontend.Trace(util.Log, pgproto3.TracerOptions{
				SuppressTimestamps: !cmd.Timestamps,
			})

		startup:
			msg, err := backend.ReceiveStartupMessage()
			if err != nil {
				util.Logf(-2, "error receiving backend startup message: %s", err)
				return
			}

			if msg, ok := msg.(*pgproto3.SSLRequest); ok {
				util.Logf(3, "got SSLRequest: %+v", msg)
				if _, err := clientConn.Write([]byte{'N'}); err != nil {
					util.Logf(-2, "error sending deny SSL request: %w", err)
					return
				}
				goto startup
			}

			frontend.Send(msg)
			if err := frontend.Flush(); err != nil {
				util.Logf(-2, "error sending frontend startup message: %s", err)
				return
			}

			// bidirectional copy loop
			go func() {
				for {
					msg, err := frontend.Receive()
					if err != nil {
						util.Logf(-2, "error receiving frontend message: %s", err)
						return
					}
					backend.Send(msg)
					if err := backend.Flush(); err != nil {
						util.Logf(-2, "error sending backend message: %s", err)
						return
					}
				}
			}()
			{
				for {
					msg, err := backend.Receive()
					if err != nil {
						util.Logf(-2, "error receiving backend message: %s", err)
						return
					}
					frontend.Send(msg)
					if err := frontend.Flush(); err != nil {
						util.Logf(-2, "error sending frontend message: %s", err)
						return
					}
				}
			}
		}()
	}
}
