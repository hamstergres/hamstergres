package proxy

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync/atomic"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/jruszo/hamstergres/internal/backend"
)

// Server exposes the PostgreSQL frontend protocol for Hamstergres.
type Server struct {
	backends *backend.Manager
	logger   *slog.Logger

	connections       atomic.Int64
	activeConnections atomic.Int64
}

func New(backends *backend.Manager, logger *slog.Logger) *Server {
	return &Server{backends: backends, logger: logger}
}

// Serve accepts PostgreSQL protocol connections until listener is closed.
func (s *Server) Serve(listener net.Listener) error {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		s.connections.Add(1)
		s.activeConnections.Add(1)
		go func() {
			defer s.activeConnections.Add(-1)
			defer conn.Close()
			if err := s.serveConn(conn); err != nil {
				s.logger.Debug("postgres frontend session ended", "remote", conn.RemoteAddr(), "error", err)
			}
		}()
	}
}

func (s *Server) serveConn(conn net.Conn) error {
	frontend := pgproto3.NewBackend(conn, conn)
	for {
		message, err := frontend.ReceiveStartupMessage()
		if err != nil {
			return err
		}
		switch message.(type) {
		case *pgproto3.SSLRequest:
			if _, err := conn.Write([]byte("N")); err != nil {
				return err
			}
			continue
		case *pgproto3.CancelRequest:
			return nil
		case *pgproto3.StartupMessage:
			if err := s.sendStartup(frontend); err != nil {
				return err
			}
			return s.serveQueries(frontend)
		default:
			return fmt.Errorf("unexpected startup message %T", message)
		}
	}
}

func (s *Server) sendStartup(frontend *pgproto3.Backend) error {
	frontend.Send(&pgproto3.AuthenticationOk{})
	frontend.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
	frontend.Send(&pgproto3.ParameterStatus{Name: "DateStyle", Value: "ISO, MDY"})
	frontend.Send(&pgproto3.ParameterStatus{Name: "integer_datetimes", Value: "on"})
	frontend.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "17.0"})
	frontend.Send(&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"})
	frontend.Send(&pgproto3.BackendKeyData{ProcessID: randomUint32(), SecretKey: randomUint32()})
	frontend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	return frontend.Flush()
}

func (s *Server) serveQueries(frontend *pgproto3.Backend) error {
	for {
		message, err := frontend.Receive()
		if err != nil {
			return err
		}
		switch message := message.(type) {
		case *pgproto3.Query:
			s.handleQuery(frontend, message.String)
		case *pgproto3.Terminate:
			return nil
		default:
			s.sendError(frontend, "0A000", "only PostgreSQL simple-query messages are supported")
		}
		if err := frontend.Flush(); err != nil {
			return err
		}
	}
}

func (s *Server) handleQuery(frontend *pgproto3.Backend, sql string) {
	if strings.TrimSpace(sql) == "" {
		frontend.Send(&pgproto3.EmptyQueryResponse{})
		frontend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		return
	}
	result, err := s.backends.QueryAll(context.Background(), sql)
	if err != nil {
		s.sendError(frontend, "XX000", err.Error())
		return
	}
	if len(result.Fields) > 0 {
		frontend.Send(&pgproto3.RowDescription{Fields: result.Fields})
		for _, values := range result.Rows {
			frontend.Send(&pgproto3.DataRow{Values: values})
		}
	}
	frontend.Send(&pgproto3.CommandComplete{CommandTag: []byte(result.CommandTag)})
	frontend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
}

func (s *Server) sendError(frontend *pgproto3.Backend, code, message string) {
	frontend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: code, Message: message})
	frontend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
}

func randomUint32() uint32 {
	var bytes [4]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return 0
	}
	return binary.BigEndian.Uint32(bytes[:])
}

// Statistics reports frontend connection counts for the status service.
type Statistics struct {
	Connections       int64 `json:"connections"`
	ActiveConnections int64 `json:"active_connections"`
}

func (s *Server) Statistics() Statistics {
	return Statistics{Connections: s.connections.Load(), ActiveConnections: s.activeConnections.Load()}
}
