// SPDX-License-Identifier: AGPL-3.0-only

package proxy

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	pg_query "github.com/pganalyze/pg_query_go/v6"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/jruszo/hamstergres/internal/backend"
	"github.com/jruszo/hamstergres/internal/copyrouter"
	"github.com/jruszo/hamstergres/internal/ddl"
	"github.com/jruszo/hamstergres/internal/router"
	"github.com/jruszo/hamstergres/internal/schema"
)

// Server exposes the PostgreSQL frontend protocol for Hamstergres.
type Server struct {
	backends       *backend.Manager
	logger         *slog.Logger
	twoPhaseCommit bool

	connections       atomic.Int64
	activeConnections atomic.Int64
	topologyReadIndex atomic.Uint64
}

func New(backends *backend.Manager, logger *slog.Logger, twoPhaseCommit ...bool) *Server {
	enabled := true
	if len(twoPhaseCommit) > 0 {
		enabled = twoPhaseCommit[0]
	}
	return &Server{backends: backends, logger: logger, twoPhaseCommit: enabled}
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
				s.logger.Debug("postgres frontend session ended", "event", "frontend_session_ended", "component", "hamstergres-proxy", "error_category", "client_disconnect", "remote", conn.RemoteAddr(), "error", err)
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
	frontend.Send(&pgproto3.BackendKeyData{
		ProcessID: randomUint32(),
		SecretKey: binary.BigEndian.AppendUint32(nil, randomUint32()),
	})
	frontend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	return frontend.Flush()
}

func (s *Server) serveQueries(frontend *pgproto3.Backend) error {
	sessionContext, cancelSession := context.WithCancel(context.Background())
	defer cancelSession()
	type frontendReceive struct {
		message pgproto3.FrontendMessage
		err     error
	}
	received := make(chan frontendReceive, 1)
	go func() {
		defer close(received)
		for {
			message, err := frontend.Receive()
			if err != nil {
				cancelSession()
				select {
				case received <- frontendReceive{err: err}:
				default:
				}
				return
			}
			// pgproto3 returns flyweight frontend messages that are valid only
			// until the next Receive call. This goroutine reads ahead, so take
			// ownership before handing a message to the query loop.
			message = cloneFrontendMessage(message)
			select {
			case received <- frontendReceive{message: message}:
			case <-sessionContext.Done():
				return
			}
		}
	}()

	state := extendedState{statements: make(map[string]statementState), portals: make(map[string]portalState), writeParticipants: make(map[string]struct{})}
	defer finishCopyTrace(&state, fmt.Errorf("frontend session ended during COPY"))
	var session *backend.Session
	defer func() {
		if session != nil {
			session.Close(context.Background())
		}
	}()

	ensureSession := func() (*backend.Session, bool) {
		if session != nil {
			return session, true
		}
		created, err := s.backends.NewSession(sessionContext)
		if err != nil {
			s.sendExtendedError(frontend, "08006", err.Error())
			state.failed = true
			return nil, false
		}
		session = created
		return session, true
	}

	for {
		next, ok := <-received
		if !ok {
			return context.Canceled
		}
		if next.err != nil {
			return next.err
		}
		message := next.message
		switch message := message.(type) {
		case *pgproto3.Parse:
			if !state.failed {
				prepared, err := prepareStatement(message, s.backends.Schema())
				if err != nil {
					s.sendExtendedError(frontend, "42601", err.Error())
					state.failed = true
					break
				}
				decision, err := s.routePortal(prepared.routing, placeholderRoutingParameters(prepared.routing.MaxParameter()), s.backends.ShardNames())
				if err != nil {
					s.sendExtendedError(frontend, "42601", err.Error())
					state.failed = true
					break
				}
				if decision.scatterError != "" {
					s.sendExtendedError(frontend, "0A000", decision.scatterError)
					state.failed = true
					break
				}
				if decision.keyedWrite && !decision.routed {
					s.sendExtendedError(frontend, "0A000", "write to a sharded table must include one unambiguous annotated shard key")
					state.failed = true
					break
				}
				if s.handleCachedParse(frontend, prepared) {
					state.statements[message.Name] = prepared
				} else {
					state.failed = true
				}
			}
		case *pgproto3.Query:
			if !state.failed {
				if firstSQLKeyword(message.String) == "COPY" {
					if active, ok := ensureSession(); ok {
						s.handleCopyQuery(frontend, active, message.String, &state)
					}
				} else if containsCopyStatement(message.String) {
					s.sendSessionError(frontend, state.txStatus(), "0A000", "COPY in a multi-statement query is not supported; send COPY as a standalone statement")
				} else if session != nil || requiresSessionAffinity(message.String) || isTransactionControl(message.String) || requiresFleetWriteOrder(message.String, len(s.backends.ShardNames())) {
					if active, ok := ensureSession(); ok {
						s.handleSessionQuery(frontend, active, message.String, &state)
					}
				} else {
					s.handleQuery(frontend, message.String)
				}
			}
		case *pgproto3.Bind:
			if active, ok := ensureSession(); ok && !state.failed {
				statement := state.statements[message.PreparedStatement]
				bound, parameters, err := s.prepareBind(message, statement)
				if err != nil {
					s.sendExtendedError(frontend, "55000", err.Error())
					state.failed = true
					break
				}
				decision, err := s.routePortal(statement.routing, parameters, s.backends.ShardNames())
				if err != nil {
					s.sendExtendedError(frontend, "42601", err.Error())
					state.failed = true
					break
				}
				if decision.scatterError != "" {
					s.sendExtendedError(frontend, "0A000", decision.scatterError)
					state.failed = true
					break
				}
				if decision.keyedWrite && !decision.routed {
					s.sendExtendedError(frontend, "0A000", "write to a sharded table must include its annotated shard key")
					state.failed = true
					break
				}
				portal := portalState{
					sql:        statement.sql,
					parameters: parameters,
					schema:     statement.schema,
					target:     decision.target,
					routed:     decision.routed,
					keyedWrite: decision.keyedWrite,
				}
				if state.pending == nil {
					state.portals[message.DestinationPortal] = portal
					targets := s.backends.ShardNames()
					if decision.routed {
						targets = []string{decision.target}
					}
					state.pending = &pendingExtended{targets: targets, bind: bound, portalName: message.DestinationPortal, portal: portal, statement: statement}
				} else if s.handleBind(frontend, active, bound, decision.target, decision.routed, statement) {
					state.portals[message.DestinationPortal] = portal
				} else {
					state.failed = true
				}
			}
		case *pgproto3.Describe:
			if active, ok := ensureSession(); ok && !state.failed {
				clientObjectName := message.Name
				if state.pending != nil && message.ObjectType == 'P' && state.pending.portalName == message.Name {
					state.pending.describe = message
					break
				}
				if state.pending != nil && !s.flushPendingExtended(frontend, active, &state, false) {
					state.failed = true
					break
				}
				generated := message.ObjectType == 'S' && state.statements[message.Name].generated
				describeTarget := ""
				describeRouted := false
				if message.ObjectType == 'S' {
					statement, exists := state.statements[message.Name]
					if !exists {
						s.sendExtendedError(frontend, "26000", fmt.Sprintf("prepared statement %q does not exist", message.Name))
						state.failed = true
						break
					}
					targets := s.backends.ShardNames()
					if len(targets) == 0 {
						s.sendExtendedError(frontend, "08006", "no Burrows configured")
						state.failed = true
						break
					}
					describeTarget, describeRouted = targets[0], true
					if !s.materializeStatement(frontend, active, statement, []string{describeTarget}) {
						state.failed = true
						break
					}
					rewritten := *message
					rewritten.Name = statement.backendName
					message = &rewritten
				}
				portal := state.portals[message.Name]
				if message.ObjectType == 'P' {
					describeTarget, describeRouted = portal.target, portal.routed
				}
				described, parameterOIDs := s.handleDescribe(frontend, active, message, generated, describeTarget, describeRouted)
				if !described {
					state.failed = true
				} else if message.ObjectType == 'S' && len(parameterOIDs) > 0 {
					statement := state.statements[clientObjectName]
					parsed := *statement.message
					parsed.ParameterOIDs = append([]uint32(nil), parameterOIDs...)
					statement.message = &parsed
					state.statements[clientObjectName] = statement
				}
			}
		case *pgproto3.Execute:
			if active, ok := ensureSession(); ok && !state.failed {
				if state.pending != nil && state.pending.portalName == message.Portal {
					state.pending.execute = message
				} else if !s.handleExecute(frontend, active, message, state.portals[message.Portal], &state) {
					state.failed = true
				}
			}
		case *pgproto3.Close:
			if active, ok := ensureSession(); ok && !state.failed {
				if state.pending != nil && !s.flushPendingExtended(frontend, active, &state, false) {
					state.failed = true
					break
				}
				if message.ObjectType == 'S' {
					delete(state.statements, message.Name)
					frontend.Send(&pgproto3.CloseComplete{})
					break
				}
				portal := state.portals[message.Name]
				if s.handleClose(frontend, active, message, portal.target, message.ObjectType == 'P' && portal.routed) {
					if message.ObjectType == 'S' {
						delete(state.statements, message.Name)
					} else {
						delete(state.portals, message.Name)
					}
				} else {
					state.failed = true
				}
			}
		case *pgproto3.Sync:
			if session == nil {
				frontend.Send(&pgproto3.ReadyForQuery{TxStatus: state.txStatus()})
				state.failed = false
			} else if state.pending != nil {
				if s.flushPendingExtended(frontend, session, &state, true) {
					state.failed = false
				}
			} else if state.syncConsumed {
				frontend.Send(&pgproto3.ReadyForQuery{TxStatus: state.txStatus()})
				state.syncConsumed = false
				state.failed = false
			} else if s.handleSync(frontend, session, &state) {
				state.failed = false
			}
		case *pgproto3.Flush:
			if session != nil && state.pending != nil && !s.flushPendingExtended(frontend, session, &state, false) {
				state.failed = true
			}
		case *pgproto3.CopyData:
			if state.copyAborted {
				// The frontend may already have queued more CopyData before it
				// observes the Proxy's asynchronous COPY error. The Burrows have
				// been drained, so discard input through CopyDone or CopyFail.
			} else if !state.copyIn || session == nil {
				s.sendExtendedError(frontend, "08P01", "CopyData received outside COPY FROM STDIN")
				state.failed = true
			} else if !s.handleCopyData(frontend, session, message, &state) {
				// COPY is initiated by a simple Query message. Error paths emit
				// ReadyForQuery after draining CopyFail, so the next simple query
				// starts a fresh protocol cycle without waiting for Sync.
				state.failed = false
			}
		case *pgproto3.CopyDone, *pgproto3.CopyFail:
			if state.copyAborted {
				state.copyAborted = false
				state.failed = false
			} else if !state.copyIn || session == nil {
				s.sendExtendedError(frontend, "08P01", "COPY completion received outside COPY FROM STDIN")
				state.failed = true
			} else if !s.handleCopyCompletion(frontend, session, message, &state) {
				state.failed = false
			}
		case *pgproto3.Terminate:
			return nil
		default:
			s.sendError(frontend, "0A000", fmt.Sprintf("unsupported PostgreSQL frontend message %T", message))
		}
		if _, sync := message.(*pgproto3.Sync); sync && session != nil && !state.transaction && !state.sessionAffinity && !state.copyIn && state.pending == nil && !state.failed {
			session.Close(context.Background(), true)
			session = nil
			s.backends.RecordOperation("backend_connection_multiplex", "release")
		}
		if err := frontend.Flush(); err != nil {
			return err
		}
	}
}

func cloneFrontendMessage(message pgproto3.FrontendMessage) pgproto3.FrontendMessage {
	switch message := message.(type) {
	case *pgproto3.Bind:
		clone := *message
		clone.ParameterFormatCodes = append([]int16(nil), message.ParameterFormatCodes...)
		clone.Parameters = make([][]byte, len(message.Parameters))
		for index, parameter := range message.Parameters {
			clone.Parameters[index] = append([]byte(nil), parameter...)
		}
		clone.ResultFormatCodes = append([]int16(nil), message.ResultFormatCodes...)
		return &clone
	case *pgproto3.Close:
		clone := *message
		return &clone
	case *pgproto3.CopyData:
		clone := *message
		clone.Data = append([]byte(nil), message.Data...)
		return &clone
	case *pgproto3.CopyDone:
		clone := *message
		return &clone
	case *pgproto3.CopyFail:
		clone := *message
		return &clone
	case *pgproto3.Describe:
		clone := *message
		return &clone
	case *pgproto3.Execute:
		clone := *message
		return &clone
	case *pgproto3.Flush:
		clone := *message
		return &clone
	case *pgproto3.FunctionCall:
		clone := *message
		clone.ArgFormatCodes = append([]uint16(nil), message.ArgFormatCodes...)
		clone.Arguments = make([][]byte, len(message.Arguments))
		for index, argument := range message.Arguments {
			clone.Arguments[index] = append([]byte(nil), argument...)
		}
		return &clone
	case *pgproto3.Parse:
		clone := *message
		clone.ParameterOIDs = append([]uint32(nil), message.ParameterOIDs...)
		return &clone
	case *pgproto3.Query:
		clone := *message
		return &clone
	case *pgproto3.Sync:
		clone := *message
		return &clone
	case *pgproto3.Terminate:
		clone := *message
		return &clone
	default:
		return message
	}
}

type extendedState struct {
	statements        map[string]statementState
	portals           map[string]portalState
	failed            bool
	transaction       bool
	transactionFailed bool
	mutated           bool
	target            string
	schemaDirty       bool
	copyIn            bool
	copyTargets       []string
	copyPlan          copyrouter.Plan
	copyStream        *copyrouter.Stream
	copyReplicated    bool
	copyAborted       bool
	copyTraceSpan     trace.Span
	copyTunnelSpans   []trace.Span
	writeParticipants map[string]struct{}
	pending           *pendingExtended
	syncConsumed      bool
	sessionAffinity   bool
}

type pendingExtended struct {
	targets    []string
	bind       *pgproto3.Bind
	describe   *pgproto3.Describe
	execute    *pgproto3.Execute
	portalName string
	portal     portalState
	statement  statementState
}

type statementState struct {
	sql         string
	generated   bool
	schema      bool
	backendName string
	message     *pgproto3.Parse
	routing     *router.Prepared
}

func prepareStatement(message *pgproto3.Parse, registry schema.Registry) (statementState, error) {
	normalized, err := normalizeDDL(message.Query)
	if err != nil {
		return statementState{}, err
	}
	routing, err := router.Prepare(normalized.sql)
	if err != nil {
		return statementState{}, err
	}
	parameter := routing.MaxParameter() + 1
	var rewritten router.GeneratedInsert
	generated := false
	if firstSQLKeyword(normalized.sql) == "INSERT" {
		if _, ok := registry.GeneratedPrimaryKey(routing.Table()); ok {
			rewritten, generated = router.RewriteGeneratedInsert(normalized.sql, registry, fmt.Sprintf("$%d", parameter))
		}
	}
	if !generated {
		if normalized.sql == message.Query {
			prepared := *message
			prepared.Name = canonicalStatementName(message.Query, message.ParameterOIDs)
			return statementState{sql: message.Query, schema: normalized.schema, backendName: prepared.Name, message: &prepared, routing: routing}, nil
		}
		prepared := *message
		prepared.Query = normalized.sql
		prepared.Name = canonicalStatementName(normalized.sql, prepared.ParameterOIDs)
		return statementState{sql: normalized.sql, schema: normalized.schema, backendName: prepared.Name, message: &prepared, routing: routing}, nil
	}
	oids := append([]uint32(nil), message.ParameterOIDs...)
	for len(oids) < parameter {
		oids = append(oids, 0)
	}
	prepared := *message
	prepared.Query = rewritten.SQL
	prepared.ParameterOIDs = oids
	prepared.Name = canonicalStatementName(rewritten.SQL, oids)
	routing, err = router.Prepare(rewritten.SQL)
	if err != nil {
		return statementState{}, err
	}
	return statementState{sql: rewritten.SQL, generated: true, schema: normalized.schema, backendName: prepared.Name, message: &prepared, routing: routing}, nil
}

func canonicalStatementName(sql string, oids []uint32) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte(sql))
	var encoded [4]byte
	for _, oid := range oids {
		binary.BigEndian.PutUint32(encoded[:], oid)
		_, _ = hash.Write(encoded[:])
	}
	return "hamstergres_" + hex.EncodeToString(hash.Sum(nil)[:12])
}

type normalizedSQL struct {
	sql    string
	schema bool
}

func normalizeDDL(sql string) (normalizedSQL, error) {
	keyword := firstSQLKeyword(sql)
	if keyword == "COMMENT" || keyword == "DROP" {
		return normalizedSQL{sql: sql, schema: true}, nil
	}
	if keyword != "CREATE" && keyword != "ALTER" {
		return normalizedSQL{sql: sql}, nil
	}
	result, err := ddl.Normalize(sql)
	if err != nil {
		return normalizedSQL{}, err
	}
	return normalizedSQL{sql: result.SQL, schema: result.Schema}, nil
}

func maxParameter(sql string) int {
	maximum, _ := router.MaxParameter(sql)
	return maximum
}

func placeholderRoutingParameters(maximum int) [][]byte {
	parameters := make([][]byte, maximum)
	for index := range parameters {
		parameters[index] = []byte("0")
	}
	return parameters
}

func (s *Server) prepareBind(message *pgproto3.Bind, statement statementState) (*pgproto3.Bind, [][]byte, error) {
	var parameterOIDs []uint32
	if statement.message != nil {
		parameterOIDs = statement.message.ParameterOIDs
	}
	if !statement.generated {
		bound := *message
		bound.PreparedStatement = statement.backendName
		return &bound, routingParameters(&bound, parameterOIDs), nil
	}
	id, err := s.backends.NextGlobalID(context.Background())
	if err != nil {
		return nil, nil, fmt.Errorf("allocate globally unique primary key: %w", err)
	}
	bound := *message
	bound.PreparedStatement = statement.backendName
	bound.Parameters = cloneParameters(message.Parameters)
	if len(message.ParameterFormatCodes) == 1 {
		bound.ParameterFormatCodes = make([]int16, len(message.Parameters), len(message.Parameters)+1)
		for index := range bound.ParameterFormatCodes {
			bound.ParameterFormatCodes[index] = message.ParameterFormatCodes[0]
		}
		bound.ParameterFormatCodes = append(bound.ParameterFormatCodes, 0)
	} else if len(message.ParameterFormatCodes) > 1 {
		bound.ParameterFormatCodes = append(append([]int16(nil), message.ParameterFormatCodes...), 0)
	}
	bound.Parameters = append(bound.Parameters, []byte(strconv.FormatInt(id, 10)))
	return &bound, routingParameters(&bound, parameterOIDs), nil
}

func routingParameters(message *pgproto3.Bind, parameterOIDs []uint32) [][]byte {
	parameters := cloneParameters(message.Parameters)
	var typeMap *pgtype.Map
	for index, value := range parameters {
		format := int16(0)
		if len(message.ParameterFormatCodes) == 1 {
			format = message.ParameterFormatCodes[0]
		} else if index < len(message.ParameterFormatCodes) {
			format = message.ParameterFormatCodes[index]
		}
		if format != pgtype.BinaryFormatCode || index >= len(parameterOIDs) || parameterOIDs[index] == 0 || value == nil {
			continue
		}
		if typeMap == nil {
			typeMap = pgtype.NewMap()
		}
		var decoded string
		if err := typeMap.Scan(parameterOIDs[index], format, value, &decoded); err == nil {
			parameters[index] = []byte(decoded)
		}
	}
	return parameters
}

func (s extendedState) txStatus() byte {
	if s.transaction && s.transactionFailed {
		return 'E'
	}
	if s.transaction {
		return 'T'
	}
	return 'I'
}

type portalState struct {
	sql        string
	parameters [][]byte
	schema     bool
	target     string
	routed     bool
	keyedWrite bool
}

func cloneParameters(parameters [][]byte) [][]byte {
	cloned := make([][]byte, len(parameters))
	for index, parameter := range parameters {
		if parameter == nil {
			continue
		}
		cloned[index] = append([]byte(nil), parameter...)
	}
	return cloned
}

func (s *Server) handleCachedParse(frontend *pgproto3.Backend, _ statementState) bool {
	// Parse is acknowledged after Proxy-side AST validation. The canonical
	// statement is materialized lazily when a Describe or Bind selects an actual
	// Burrow, avoiding one connection and Parse per configured Burrow.
	frontend.Send(&pgproto3.ParseComplete{})
	return true
}

func (s *Server) materializeStatement(frontend *pgproto3.Backend, session *backend.Session, statement statementState, targets []string) bool {
	missing := make([]string, 0, len(targets))
	for _, target := range targets {
		if err := session.Ensure(target); err != nil {
			s.sendExtendedError(frontend, "08006", err.Error())
			return false
		}
		if !session.Prepared(target, statement.backendName) {
			missing = append(missing, target)
		}
	}
	for _, target := range missing {
		responses, err := exchangeOne(session, target, statement.message, isParseDone)
		if err != nil {
			s.sendExtendedError(frontend, "08006", err.Error())
			return false
		}
		if response := firstError(responses); response != nil {
			// Canonical names are a stable hash of SQL and parameter types. A
			// retained PostgreSQL statement can outlive local cache knowledge
			// after pool reconciliation, so duplicate Parse is idempotent.
			if response.Code == "42P05" {
				continue
			}
			frontend.Send(response)
			return false
		}
	}
	if len(missing) > 0 {
		session.MarkPrepared(missing, statement.backendName)
		s.backends.RecordOperation("prepared_statement_cache", "miss")
	} else {
		s.backends.RecordOperation("prepared_statement_cache", "hit")
	}
	return true
}

func (s *Server) handleBind(frontend *pgproto3.Backend, session *backend.Session, message *pgproto3.Bind, target string, routed bool, statement statementState) bool {
	targets := s.backends.ShardNames()
	if routed {
		targets = []string{target}
	}
	if !s.materializeStatement(frontend, session, statement, targets) {
		return false
	}
	var responses [][]pgproto3.BackendMessage
	var err error
	if routed {
		responses, err = exchangeOne(session, target, message, isBindDone)
	} else {
		responses, err = exchange(session, targets, message, isBindDone)
	}
	return s.relayUniform(frontend, responses, err, "Bind")
}

func (s *Server) handleDescribe(frontend *pgproto3.Backend, session *backend.Session, message *pgproto3.Describe, generated bool, target string, routed bool) (bool, []uint32) {
	var responses [][]pgproto3.BackendMessage
	var err error
	if routed {
		responses, err = exchangeOne(session, target, message, isDescribeDone)
	} else {
		responses, err = exchange(session, s.backends.ShardNames(), message, isDescribeDone)
	}
	var parameterOIDs []uint32
	for _, response := range responses {
		for _, wireMessage := range response {
			if parameters, ok := wireMessage.(*pgproto3.ParameterDescription); ok && parameterOIDs == nil {
				parameterOIDs = append([]uint32(nil), parameters.ParameterOIDs...)
			}
		}
	}
	if generated {
		for _, response := range responses {
			for _, wireMessage := range response {
				if parameters, ok := wireMessage.(*pgproto3.ParameterDescription); ok && len(parameters.ParameterOIDs) > 0 {
					parameters.ParameterOIDs = parameters.ParameterOIDs[:len(parameters.ParameterOIDs)-1]
				}
			}
		}
	}
	return s.relayUniform(frontend, responses, err, "Describe"), parameterOIDs
}

func (s *Server) handleClose(frontend *pgproto3.Backend, session *backend.Session, message *pgproto3.Close, target string, routed bool) bool {
	var responses [][]pgproto3.BackendMessage
	var err error
	if routed {
		responses, err = exchangeOne(session, target, message, isCloseDone)
	} else {
		responses, err = exchange(session, s.backends.ShardNames(), message, isCloseDone)
	}
	return s.relayUniform(frontend, responses, err, "Close")
}

func (s *Server) handleCopyQuery(frontend *pgproto3.Backend, session *backend.Session, sql string, state *extendedState) bool {
	success := false
	retained := false
	copyPlan, err := copyrouter.Parse(sql, s.backends.Schema())
	if err != nil {
		s.sendSessionError(frontend, state.txStatus(), "0A000", err.Error())
		return false
	}
	if copyPlan.Program {
		s.sendSessionError(frontend, state.txStatus(), "0A000", "COPY PROGRAM is not supported by Hamstergres Proxy; use COPY FROM STDIN or COPY TO STDOUT")
		return false
	}
	if copyPlan.ServerSide {
		return s.handleServerSideCopy(frontend, session, sql, copyPlan, state)
	}
	targets, err := s.copyTargets(sql)
	if err != nil {
		s.sendSessionError(frontend, state.txStatus(), "42601", err.Error())
		return false
	}
	var copyStream *copyrouter.Stream
	if copyPlan.From && copyPlan.Sharded {
		copyStream, err = copyrouter.NewStream(copyPlan, s.backends.Schema(), s.backends.ShardNames())
		if err != nil {
			s.sendSessionError(frontend, state.txStatus(), "0A000", err.Error())
			return false
		}
	}
	route := "scatter"
	if len(targets) == 1 {
		route = "single_burrow"
	}
	traceContext, span := otel.Tracer("github.com/jruszo/hamstergres/proxy").Start(context.Background(), "proxy.query", trace.WithAttributes(
		attribute.String("db.operation.name", "COPY"), attribute.String("hamstergres.route", route)))
	state.copyTraceSpan = span
	state.copyTunnelSpans = startTunnelSpans(traceContext, targets)
	defer func() {
		outcome := "failure"
		if success {
			outcome = "success"
		}
		s.backends.RecordOperation("copy", outcome)
		if !success {
			s.logger.Error("COPY operation failed", "event", "copy_failed", "component", "hamstergres-proxy", "error_category", "copy")
		}
		if !retained {
			var traceErr error
			if !success {
				traceErr = fmt.Errorf("COPY operation failed")
			}
			finishCopyTrace(state, traceErr)
		}
	}()
	responses, err := exchange(session, targets, &pgproto3.Query{String: sql}, isCopyStarted)
	if err != nil {
		state.transactionFailed = state.transaction
		s.sendSessionError(frontend, state.txStatus(), "08006", err.Error())
		return false
	}
	if response := firstError(responses); response != nil {
		state.transactionFailed = state.transaction
		frontend.Send(response)
		frontend.Send(&pgproto3.ReadyForQuery{TxStatus: state.txStatus()})
		return false
	}
	if len(responses) == 0 || len(responses[0]) == 0 {
		s.sendSessionError(frontend, state.txStatus(), "XX000", "no Burrow COPY response")
		return false
	}
	first := responses[0][len(responses[0])-1]
	for _, response := range responses[1:] {
		if len(response) == 0 || reflect.TypeOf(response[len(response)-1]) != reflect.TypeOf(first) {
			s.sendSessionError(frontend, state.txStatus(), "XX000", "incompatible COPY responses from Burrows")
			return false
		}
	}
	switch response := first.(type) {
	case *pgproto3.CopyInResponse:
		frontend.Send(response)
		state.copyIn = true
		state.copyAborted = false
		state.copyTargets = append(state.copyTargets[:0], targets...)
		state.copyPlan = copyPlan
		state.copyStream = copyStream
		state.copyReplicated = copyPlan.From && !copyPlan.Sharded && len(targets) > 1
		s.markWriteParticipants(state, targets)
		if state.transaction {
			state.mutated = true
		}
		success = true
		retained = true
		return true
	case *pgproto3.CopyOutResponse:
		frontend.Send(response)
		success = s.handleCopyOut(frontend, session, targets, copyPlan, state)
		return success
	case *pgproto3.CopyBothResponse:
		s.sendExtendedError(frontend, "0A000", "COPY BOTH is not supported by Hamstergres Proxy")
		return false
	default:
		s.sendExtendedError(frontend, "08P01", fmt.Sprintf("unexpected COPY response %T", first))
		return false
	}
}

func (s *Server) handleServerSideCopy(frontend *pgproto3.Backend, session *backend.Session, sql string, plan copyrouter.Plan, state *extendedState) bool {
	registry := s.backends.Schema()
	if plan.Table == "" || !registry.HasTable(plan.Table) {
		s.sendSessionError(frontend, state.txStatus(), "0A000", "server-side COPY requires a known unsharded table; use COPY FROM STDIN or COPY TO STDOUT")
		return false
	}
	if plan.Sharded {
		s.sendSessionError(frontend, state.txStatus(), "0A000", "server-side COPY for a sharded table is not supported; use COPY FROM STDIN or COPY TO STDOUT")
		return false
	}
	if s.backends.UnshardedMode() != "primary" {
		s.sendSessionError(frontend, state.txStatus(), "0A000", "server-side COPY is not supported for replicated unsharded tables because file paths are Burrow-local; use COPY FROM STDIN or COPY TO STDOUT")
		return false
	}
	target := s.backends.PrimaryBurrow()
	if target == "" {
		s.sendSessionError(frontend, state.txStatus(), "08006", "no primary Burrow configured")
		return false
	}

	started := time.Now()
	success := false
	errorCategory := "query_execution"
	traceContext, span := otel.Tracer("github.com/jruszo/hamstergres/proxy").Start(context.Background(), "proxy.query", trace.WithAttributes(
		attribute.String("db.operation.name", "COPY"), attribute.String("hamstergres.route", "single_burrow")))
	tunnelSpans := startTunnelSpans(traceContext, []string{target})
	defer func() {
		outcome := "failure"
		if success {
			outcome = "success"
			span.SetStatus(codes.Ok, "")
		} else {
			span.SetStatus(codes.Error, "COPY failed")
		}
		span.End()
		s.backends.RecordOperation("copy", outcome)
		s.backends.RecordQueryTargetsCategory(sql, success, time.Since(started), []string{target}, errorCategory)
	}()

	responses, err := exchangeOne(session, target, &pgproto3.Query{String: sql}, isQueryDone)
	if err != nil {
		errorCategory = "burrow_transport"
		endTunnelSpans(tunnelSpans, err)
		state.transactionFailed = state.transaction
		s.sendSessionError(frontend, state.txStatus(), "08006", err.Error())
		return false
	}
	if len(responses) != 1 || len(responses[0]) == 0 {
		err := fmt.Errorf("no primary Burrow COPY response")
		errorCategory = "protocol"
		endTunnelSpans(tunnelSpans, err)
		s.sendSessionError(frontend, state.txStatus(), "XX000", err.Error())
		return false
	}
	backendError := firstError(responses)
	if backendError != nil {
		errorCategory = postgresErrorCategory(backendError.Code)
		endTunnelSpans(tunnelSpans, fmt.Errorf("PostgreSQL error %s", backendError.Code))
	} else {
		endTunnelSpans(tunnelSpans, nil)
	}
	for _, message := range responses[0] {
		frontend.Send(message)
	}
	if backendError != nil {
		state.transactionFailed = readyTxStatus(responses) == 'E'
		return false
	}
	if plan.From {
		s.markWriteParticipants(state, []string{target})
		if state.transaction {
			state.mutated = true
		}
	}
	success = true
	return true
}

func (s *Server) handleCopyData(frontend *pgproto3.Backend, session *backend.Session, message *pgproto3.CopyData, state *extendedState) bool {
	if state.copyStream == nil {
		if err := session.SendCopyTo(state.copyTargets, message); err != nil {
			s.abortCopy(frontend, session, state, "08006", err)
			return false
		}
		return true
	}
	chunks, err := state.copyStream.Write(message.Data)
	if err != nil {
		s.abortCopy(frontend, session, state, "22023", err)
		return false
	}
	if err := sendCopyChunks(session, state.copyTargets, chunks); err != nil {
		s.abortCopy(frontend, session, state, "08006", err)
		return false
	}
	return true
}

func sendCopyChunks(session *backend.Session, targets []string, chunks []copyrouter.Chunk) error {
	byTarget := make(map[string][]byte, len(targets))
	for _, chunk := range chunks {
		if chunk.Target == "" {
			for _, target := range targets {
				byTarget[target] = append(byTarget[target], chunk.Data...)
			}
			continue
		}
		byTarget[chunk.Target] = append(byTarget[chunk.Target], chunk.Data...)
	}
	for _, target := range targets {
		if len(byTarget[target]) == 0 {
			continue
		}
		if err := session.SendCopyTo([]string{target}, &pgproto3.CopyData{Data: byTarget[target]}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) abortCopy(frontend *pgproto3.Backend, session *backend.Session, state *extendedState, code string, cause error) {
	targets := append([]string(nil), state.copyTargets...)
	_ = session.SendCopyTo(targets, &pgproto3.CopyFail{Message: cause.Error()})
	_, _ = session.ReceiveUntilFromMany(context.Background(), targets, isQueryDone)
	state.copyIn = false
	state.copyTargets = nil
	state.copyStream = nil
	state.copyReplicated = false
	state.copyAborted = true
	state.transactionFailed = state.transaction
	finishCopyTrace(state, cause)
	frontend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: code, Message: cause.Error()})
	frontend.Send(&pgproto3.ReadyForQuery{TxStatus: state.txStatus()})
}

func (s *Server) copyTargets(sql string) ([]string, error) {
	burrows := s.backends.ShardNames()
	decision, err := s.routeSQL(sql, burrows)
	if err != nil {
		return nil, err
	}
	if decision.routed {
		return []string{decision.target}, nil
	}
	return burrows, nil
}

func (s *Server) markWriteParticipants(state *extendedState, targets []string) {
	if !state.transaction {
		return
	}
	for _, name := range targets {
		state.writeParticipants[name] = struct{}{}
	}
}

func (s *Server) handleCopyOut(frontend *pgproto3.Backend, session *backend.Session, targets []string, plan copyrouter.Plan, state *extendedState) bool {
	var tags []string
	for index, target := range targets {
		output := copyrouter.NewOutputStream(plan, index, len(targets))
		var backendError *pgproto3.ErrorResponse
		err := session.ReceiveEachFrom(context.Background(), target, isQueryDone, func(message pgproto3.BackendMessage) error {
			switch message := message.(type) {
			case *pgproto3.CopyData:
				parts, err := output.Write(message.Data)
				if err != nil {
					return err
				}
				for _, data := range parts {
					frontend.Send(&pgproto3.CopyData{Data: data})
					if err := frontend.Flush(); err != nil {
						return err
					}
				}
			case *pgproto3.CommandComplete:
				tags = append(tags, string(message.CommandTag))
			case *pgproto3.ErrorResponse:
				copy := *message
				backendError = &copy
			case *pgproto3.CopyDone, *pgproto3.ReadyForQuery, *pgproto3.NoticeResponse:
			default:
				return fmt.Errorf("unexpected COPY TO response %T", message)
			}
			return nil
		})
		if err != nil {
			state.transactionFailed = state.transaction
			s.sendSessionError(frontend, state.txStatus(), "08006", err.Error())
			return false
		}
		if backendError != nil {
			state.transactionFailed = state.transaction
			frontend.Send(backendError)
			frontend.Send(&pgproto3.ReadyForQuery{TxStatus: state.txStatus()})
			return false
		}
		parts, err := output.Finish()
		if err != nil {
			s.sendSessionError(frontend, state.txStatus(), "08P01", err.Error())
			return false
		}
		for _, data := range parts {
			frontend.Send(&pgproto3.CopyData{Data: data})
			if err := frontend.Flush(); err != nil {
				return false
			}
		}
	}
	frontend.Send(&pgproto3.CopyDone{})
	if len(tags) > 0 {
		frontend.Send(&pgproto3.CommandComplete{CommandTag: []byte(mergedCommandTag(tags, 0))})
	}
	frontend.Send(&pgproto3.ReadyForQuery{TxStatus: state.txStatus()})
	return true
}

func (s *Server) handleCopyCompletion(frontend *pgproto3.Backend, session *backend.Session, message pgproto3.FrontendMessage, state *extendedState) bool {
	success := false
	defer func() {
		var err error
		if !success {
			err = fmt.Errorf("COPY completion failed")
		}
		finishCopyTrace(state, err)
	}()
	targets := append([]string(nil), state.copyTargets...)
	stream := state.copyStream
	sharded := state.copyPlan.Sharded
	replicated := state.copyReplicated
	if _, failed := message.(*pgproto3.CopyFail); failed {
		state.transactionFailed = state.transaction
	}
	if _, done := message.(*pgproto3.CopyDone); done && stream != nil {
		chunks, err := stream.Finish()
		if err != nil {
			s.abortCopy(frontend, session, state, "22023", err)
			state.copyAborted = false
			return false
		}
		if err := sendCopyChunks(session, targets, chunks); err != nil {
			s.abortCopy(frontend, session, state, "08006", err)
			state.copyAborted = false
			return false
		}
	}
	state.copyIn = false
	state.copyTargets = nil
	state.copyStream = nil
	state.copyReplicated = false
	if err := session.SendCopyTo(targets, message); err != nil {
		state.transactionFailed = state.transaction
		s.sendSessionError(frontend, state.txStatus(), "08006", err.Error())
		return false
	}
	responses, err := session.ReceiveUntilFromMany(context.Background(), targets, isQueryDone)
	if err != nil {
		state.transactionFailed = state.transaction
		s.sendSessionError(frontend, state.txStatus(), "08006", err.Error())
		return false
	}
	if response := firstError(responses); response != nil {
		state.transactionFailed = state.transaction
		frontend.Send(response)
		frontend.Send(&pgproto3.ReadyForQuery{TxStatus: state.txStatus()})
		return false
	}
	var tags []string
	for _, response := range responses {
		for _, wireMessage := range response {
			if complete, ok := wireMessage.(*pgproto3.CommandComplete); ok {
				tags = append(tags, string(complete.CommandTag))
			}
		}
	}
	if len(tags) > 0 {
		rows := int64(-1)
		if stream != nil {
			rows = stream.Rows()
		}
		tag, err := mergedCopyFromTag(tags, sharded, replicated, rows)
		if err != nil {
			s.sendSessionError(frontend, state.txStatus(), "XX000", err.Error())
			return false
		}
		frontend.Send(&pgproto3.CommandComplete{CommandTag: []byte(tag)})
	}
	frontend.Send(&pgproto3.ReadyForQuery{TxStatus: state.txStatus()})
	success = true
	return true
}

func mergedCopyFromTag(tags []string, sharded, replicated bool, expectedRows int64) (string, error) {
	if len(tags) == 0 {
		return "", fmt.Errorf("no Burrow COPY completion tag")
	}
	if replicated {
		for _, tag := range tags[1:] {
			if tag != tags[0] {
				return "", fmt.Errorf("replicated COPY row counts differ across Burrows: %s", strings.Join(tags, ", "))
			}
		}
		return tags[0], nil
	}
	tag := mergedCommandTag(tags, 0)
	if sharded && expectedRows >= 0 {
		expected := fmt.Sprintf("COPY %d", expectedRows)
		if tag != expected {
			return "", fmt.Errorf("sharded COPY row count is %q, expected %q", tag, expected)
		}
	}
	return tag, nil
}

func finishCopyTrace(state *extendedState, err error) {
	if state == nil || state.copyTraceSpan == nil {
		return
	}
	endTunnelSpans(state.copyTunnelSpans, err)
	if err != nil {
		state.copyTraceSpan.RecordError(err)
		state.copyTraceSpan.SetStatus(codes.Error, "COPY failed")
	} else {
		state.copyTraceSpan.SetStatus(codes.Ok, "")
	}
	state.copyTraceSpan.End()
	state.copyTraceSpan = nil
	state.copyTunnelSpans = nil
}

func (s *Server) handleSync(frontend *pgproto3.Backend, session *backend.Session, state *extendedState) bool {
	targets := session.ConnectedNames()
	if len(targets) == 0 {
		frontend.Send(&pgproto3.ReadyForQuery{TxStatus: state.txStatus()})
		return true
	}
	responses, err := exchange(session, targets, &pgproto3.Sync{}, isSyncDone)
	if err != nil {
		s.sendExtendedError(frontend, "08006", err.Error())
		return false
	}
	if state.schemaDirty {
		if err := s.backends.RefreshSchema(context.Background()); err != nil {
			frontend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "55000", Message: fmt.Sprintf("refresh schema registry after DDL: %v", err)})
			frontend.Send(&pgproto3.ReadyForQuery{TxStatus: state.txStatus()})
			return true
		}
		state.schemaDirty = false
	}
	if !s.relaySync(frontend, responses, state.txStatus()) {
		return false
	}
	if !state.transaction {
		session.UnlockFleetWrites()
	}
	return true
}

// flushPendingExtended preserves the client's Bind/Execute/Sync request as one
// backend request. The selected Burrow receives one socket flush and the Proxy
// drains the resulting ordered response stream once.
func (s *Server) flushPendingExtended(frontend *pgproto3.Backend, session *backend.Session, state *extendedState, emitReady bool) bool {
	started := time.Now()
	pending := state.pending
	if pending == nil {
		return true
	}
	state.pending = nil
	fleetWriteGateAcquired := false
	fail := func(code, message string) bool {
		if fleetWriteGateAcquired && !state.transaction {
			session.UnlockFleetWrites()
			fleetWriteGateAcquired = false
		}
		return s.failPendingExtended(frontend, state, emitReady, code, message)
	}

	messages := []pgproto3.FrontendMessage{pending.bind}
	if pending.describe != nil {
		messages = append(messages, pending.describe)
	}
	if pending.execute != nil {
		if requiresFleetWriteOrder(pending.portal.sql, len(pending.targets)) && !session.LockFleetWritesContext(session.Context()) {
			return s.failPendingExtended(frontend, state, emitReady, "57014", "frontend session ended while waiting to execute a write")
		}
		fleetWriteGateAcquired = requiresFleetWriteOrder(pending.portal.sql, len(pending.targets))
		messages = append(messages, pending.execute)
	}
	messages = append(messages, &pgproto3.Sync{})
	preparedMisses := make([]string, 0, len(pending.targets))
	preparedMissing := make(map[string]struct{}, len(pending.targets))
	for _, target := range pending.targets {
		if err := session.Ensure(target); err != nil {
			return fail("08006", err.Error())
		}
		targetMessages := messages
		if pending.statement.backendName != "" && !session.Prepared(target, pending.statement.backendName) {
			targetMessages = append([]pgproto3.FrontendMessage{pending.statement.message}, messages...)
			preparedMisses = append(preparedMisses, target)
			preparedMissing[target] = struct{}{}
		}
		if err := session.SendBatchTo(target, targetMessages...); err != nil {
			return fail("08006", err.Error())
		}
	}
	success := pending.execute != nil
	errorCategory := "query_execution"
	backendFailed := false
	var description *pgproto3.RowDescription
	var parameterDescription *pgproto3.ParameterDescription
	var tags []string
	bindComplete := false
	noData := false
	emptyQuery := false
	portalSuspended := false
	rowCount := 0
	preparedMaterialized := false
	for _, target := range pending.targets {
		err := session.ReceiveEachFrom(context.Background(), target, isSyncDone, func(wireMessage pgproto3.BackendMessage) error {
			if _, parsed := wireMessage.(*pgproto3.ParseComplete); parsed {
				if _, expected := preparedMissing[target]; expected {
					session.MarkPrepared([]string{target}, pending.statement.backendName)
					delete(preparedMissing, target)
					preparedMaterialized = true
				}
				return nil
			}
			if backendFailed {
				return nil
			}
			switch wireMessage := wireMessage.(type) {
			case *pgproto3.BindComplete:
				if !bindComplete {
					bindComplete = true
					frontend.Send(wireMessage)
				}
			case *pgproto3.RowDescription:
				if description == nil {
					description = ownedRowDescription(wireMessage)
					frontend.Send(wireMessage)
				} else if !sameRowDescription(description, wireMessage) {
					backendFailed = true
					success = false
					errorCategory = "protocol"
					s.sendExtendedError(frontend, "XX000", "incompatible row descriptions from Burrows")
				}
			case *pgproto3.ParameterDescription:
				if parameterDescription == nil {
					parameterDescription = &pgproto3.ParameterDescription{ParameterOIDs: append([]uint32(nil), wireMessage.ParameterOIDs...)}
					frontend.Send(wireMessage)
				} else if !reflect.DeepEqual(parameterDescription.ParameterOIDs, wireMessage.ParameterOIDs) {
					backendFailed = true
					success = false
					errorCategory = "protocol"
					s.sendExtendedError(frontend, "XX000", "incompatible parameter descriptions from Burrows")
				}
			case *pgproto3.NoData:
				if !noData {
					noData = true
					frontend.Send(wireMessage)
				}
			case *pgproto3.DataRow:
				rowCount++
				frontend.Send(wireMessage)
			case *pgproto3.CommandComplete:
				tags = append(tags, string(wireMessage.CommandTag))
			case *pgproto3.EmptyQueryResponse:
				if !emptyQuery {
					emptyQuery = true
					frontend.Send(wireMessage)
				}
			case *pgproto3.PortalSuspended:
				portalSuspended = true
			case *pgproto3.ErrorResponse:
				backendFailed = true
				success = false
				errorCategory = postgresErrorCategory(wireMessage.Code)
				frontend.Send(wireMessage)
			case *pgproto3.ReadyForQuery:
				// The internally forwarded Sync is exposed only when the frontend
				// reaches its own Sync boundary.
			case *pgproto3.NoticeResponse, *pgproto3.ParameterStatus, *pgproto3.NotificationResponse:
				// Burrow-local asynchronous messages are not duplicated.
			default:
				return fmt.Errorf("unexpected batched response %T", wireMessage)
			}
			return nil
		})
		if err != nil {
			return fail("08006", err.Error())
		}
	}
	if preparedMaterialized {
		s.backends.RecordOperation("prepared_statement_cache", "miss")
	} else if len(preparedMisses) == 0 && pending.statement.backendName != "" {
		s.backends.RecordOperation("prepared_statement_cache", "hit")
	}
	if !backendFailed {
		if invalidatesPreparedStatements(pending.portal.sql) {
			s.backends.InvalidatePreparedStatements(pending.targets)
		}
		if portalSuspended {
			frontend.Send(&pgproto3.PortalSuspended{})
		} else if len(tags) > 0 {
			frontend.Send(&pgproto3.CommandComplete{CommandTag: []byte(mergedCommandTag(tags, rowCount))})
		}
	}

	if pending.execute != nil && pending.portal.sql != "" {
		s.backends.RecordQueryTargetsCategory(pending.portal.sql, success, time.Since(started), pending.targets, errorCategory)
		if success {
			recordWriteParticipants(state, pending.portal.sql, pending.targets)
			if pending.portal.schema {
				state.schemaDirty = true
			}
			if requiresSessionAffinity(pending.portal.sql) {
				state.sessionAffinity = true
			}
			updateTransactionState(state, pending.portal.sql)
		}
	}
	if state.schemaDirty && !backendFailed {
		if err := s.backends.RefreshSchema(context.Background()); err != nil {
			frontend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "55000", Message: fmt.Sprintf("refresh schema registry after DDL: %v", err)})
			backendFailed = true
		} else {
			state.schemaDirty = false
		}
	}
	if emitReady {
		frontend.Send(&pgproto3.ReadyForQuery{TxStatus: state.txStatus()})
	} else {
		state.syncConsumed = true
	}
	state.failed = backendFailed && !emitReady
	if !state.transaction {
		session.UnlockFleetWrites()
	}
	return true
}

func (s *Server) failPendingExtended(frontend *pgproto3.Backend, state *extendedState, emitReady bool, code, message string) bool {
	s.sendExtendedError(frontend, code, message)
	state.failed = !emitReady
	if emitReady {
		// The failure happened while processing the frontend's Sync, so there
		// is no later recovery boundary to wait for.
		frontend.Send(&pgproto3.ReadyForQuery{TxStatus: state.txStatus()})
	}
	return false
}

// handleExecute merges rows from the fan-out execution. Data values are
// already encoded by PostgreSQL, so text and binary result formats pass through
// unchanged.
func (s *Server) handleExecute(frontend *pgproto3.Backend, session *backend.Session, message *pgproto3.Execute, portal portalState, state *extendedState) bool {
	started := time.Now()
	success := false
	errorCategory := "query_execution"
	traceContext, querySpan := otel.Tracer("github.com/jruszo/hamstergres/proxy").Start(context.Background(), "proxy.query",
		trace.WithAttributes(attribute.String("db.operation.name", firstSQLKeyword(portal.sql)), attribute.String("hamstergres.route", "scatter")))
	defer func() {
		if success {
			querySpan.SetStatus(codes.Ok, "")
		} else {
			querySpan.SetStatus(codes.Error, "query failed")
		}
		querySpan.End()
		if !success && portal.sql != "" {
			s.logger.Error("frontend query failed", "event", "query_failed", "component", "hamstergres-proxy", "correlation_id", fmt.Sprintf("query-%08x", randomUint32()), "error_category", errorCategory)
		}
	}()
	targets := s.backends.ShardNames()
	defer func() {
		if portal.sql != "" {
			s.backends.RecordQueryTargetsCategory(portal.sql, success, time.Since(started), targets, errorCategory)
		}
	}()
	if state.transaction && state.mutated && len(state.writeParticipants) > 1 && s.twoPhaseCommit && (firstSQLKeyword(portal.sql) == "COMMIT" || firstSQLKeyword(portal.sql) == "END") {
		if response := s.commitTwoPhase(traceContext, session, participantNames(state.writeParticipants)); response != nil {
			frontend.Send(response)
			return false
		}
		frontend.Send(&pgproto3.CommandComplete{CommandTag: []byte("COMMIT")})
		state.transaction = false
		state.mutated = false
		clear(state.writeParticipants)
		success = true
		return true
	}
	target, routed := portal.target, portal.routed
	if portal.keyedWrite && !routed {
		errorCategory = "unsafe_routing"
		s.sendExtendedError(frontend, "0A000", "write to a sharded table must include its annotated shard key")
		return false
	}
	var responses [][]pgproto3.BackendMessage
	var err error
	if routed && !isTransactionControl(portal.sql) {
		targets = []string{target}
		querySpan.SetAttributes(attribute.String("hamstergres.route", "single_burrow"))
	} else {
		querySpan.SetAttributes(attribute.String("hamstergres.route", "scatter"))
	}
	if requiresFleetWriteOrder(portal.sql, len(targets)) && !session.LockFleetWritesContext(session.Context()) {
		errorCategory = "client_disconnect"
		s.sendExtendedError(frontend, "57014", "frontend session ended while waiting to execute a write")
		return false
	}
	tunnelSpans := startTunnelSpans(traceContext, targets)
	if len(targets) == 1 {
		responses, err = exchangeOne(session, targets[0], message, isExecuteDone)
	} else {
		responses, err = exchange(session, targets, message, isExecuteDone)
	}
	if err != nil {
		errorCategory = "burrow_transport"
		endTunnelSpans(tunnelSpans, err)
		s.sendExtendedError(frontend, "08006", err.Error())
		return false
	}
	if response := firstError(responses); response != nil {
		errorCategory = postgresErrorCategory(response.Code)
		endTunnelSpans(tunnelSpans, fmt.Errorf("PostgreSQL error %s", response.Code))
		frontend.Send(response)
		return false
	}
	endTunnelSpans(tunnelSpans, nil)

	var description *pgproto3.RowDescription
	var rows []*pgproto3.DataRow
	var tags []string
	portalSuspended := false
	for _, response := range responses {
		for _, wireMessage := range response {
			switch wireMessage := wireMessage.(type) {
			case *pgproto3.RowDescription:
				if description == nil {
					description = wireMessage
				} else if !sameRowDescription(description, wireMessage) {
					s.sendExtendedError(frontend, "XX000", "incompatible row descriptions from Burrows")
					return false
				}
			case *pgproto3.DataRow:
				rows = append(rows, wireMessage)
			case *pgproto3.CommandComplete:
				tags = append(tags, string(wireMessage.CommandTag))
			case *pgproto3.PortalSuspended:
				portalSuspended = true
			case *pgproto3.EmptyQueryResponse, *pgproto3.NoticeResponse, *pgproto3.ParameterStatus, *pgproto3.NotificationResponse:
				// The first three carry no result data. Notices and notifications
				// are Burrow-local and must not be duplicated to the frontend.
			case *pgproto3.CopyInResponse, *pgproto3.CopyOutResponse, *pgproto3.CopyBothResponse:
				s.sendExtendedError(frontend, "0A000", "COPY is not supported by Hamstergres Proxy")
				return false
			default:
				s.sendExtendedError(frontend, "08P01", fmt.Sprintf("unexpected Execute response %T", wireMessage))
				return false
			}
		}
	}
	if description != nil {
		frontend.Send(description)
	}
	for _, row := range rows {
		frontend.Send(row)
	}
	if portalSuspended {
		frontend.Send(&pgproto3.PortalSuspended{})
	} else if len(tags) > 0 {
		frontend.Send(&pgproto3.CommandComplete{CommandTag: []byte(mergedCommandTag(tags, len(rows)))})
	} else {
		frontend.Send(&pgproto3.EmptyQueryResponse{})
	}
	success = true
	if invalidatesPreparedStatements(portal.sql) {
		s.backends.InvalidatePreparedStatements(targets)
	}
	if portal.schema {
		state.schemaDirty = true
	}
	if requiresSessionAffinity(portal.sql) {
		state.sessionAffinity = true
	}
	recordWriteParticipants(state, portal.sql, targets)
	updateTransactionState(state, portal.sql)
	return true
}

func exchange(session *backend.Session, targets []string, message pgproto3.FrontendMessage, done func(pgproto3.BackendMessage) bool) ([][]pgproto3.BackendMessage, error) {
	for _, target := range targets {
		if err := session.SendTo(target, message); err != nil {
			return nil, err
		}
	}
	return session.ReceiveUntilFromMany(context.Background(), targets, done)
}

func exchangeOne(session *backend.Session, target string, message pgproto3.FrontendMessage, done func(pgproto3.BackendMessage) bool) ([][]pgproto3.BackendMessage, error) {
	return exchange(session, []string{target}, message, done)
}

func (s *Server) relayUniform(frontend *pgproto3.Backend, responses [][]pgproto3.BackendMessage, err error, operation string) bool {
	if err != nil {
		s.sendExtendedError(frontend, "08006", err.Error())
		return false
	}
	if response := firstError(responses); response != nil {
		frontend.Send(response)
		return false
	}
	if len(responses) == 0 {
		s.sendExtendedError(frontend, "XX000", "no Burrow responses")
		return false
	}
	first := protocolMessages(responses[0])
	for _, response := range responses[1:] {
		if !sameProtocolMessages(first, protocolMessages(response)) {
			s.sendExtendedError(frontend, "XX000", fmt.Sprintf("incompatible %s responses from Burrows", operation))
			return false
		}
	}
	for _, message := range first {
		frontend.Send(message)
	}
	return true
}

// relaySync accepts the backend's physical state but reports the proxy's
// logical transaction state. A transaction is deliberately pinned to one
// Burrow; the other affinity connections remain idle.
func (s *Server) relaySync(frontend *pgproto3.Backend, responses [][]pgproto3.BackendMessage, status byte) bool {
	if len(responses) == 0 || len(responses[0]) == 0 {
		s.sendExtendedError(frontend, "XX000", "no Burrow responses")
		return false
	}
	if response := firstError(responses); response != nil {
		frontend.Send(response)
		return false
	}
	frontend.Send(&pgproto3.ReadyForQuery{TxStatus: status})
	return true
}

func sameProtocolMessages(left, right []pgproto3.BackendMessage) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		switch left := left[index].(type) {
		case *pgproto3.RowDescription:
			right, ok := right[index].(*pgproto3.RowDescription)
			if !ok || !sameRowDescription(left, right) {
				return false
			}
		case *pgproto3.ParameterDescription:
			right, ok := right[index].(*pgproto3.ParameterDescription)
			if !ok || !sameOIDs(left.ParameterOIDs, right.ParameterOIDs) {
				return false
			}
		case *pgproto3.ReadyForQuery:
			right, ok := right[index].(*pgproto3.ReadyForQuery)
			if !ok || left.TxStatus != right.TxStatus {
				return false
			}
		default:
			if reflect.TypeOf(left) != reflect.TypeOf(right[index]) {
				return false
			}
		}
	}
	return true
}

func sameRowDescription(left, right *pgproto3.RowDescription) bool {
	if len(left.Fields) != len(right.Fields) {
		return false
	}
	for index := range left.Fields {
		leftField, rightField := left.Fields[index], right.Fields[index]
		// Table OIDs and attribute numbers are local to a Burrow and therefore
		// cannot be compared across independently-created shard databases.
		if string(leftField.Name) != string(rightField.Name) ||
			leftField.DataTypeOID != rightField.DataTypeOID ||
			leftField.DataTypeSize != rightField.DataTypeSize ||
			leftField.TypeModifier != rightField.TypeModifier ||
			leftField.Format != rightField.Format {
			return false
		}
	}
	return true
}

func ownedRowDescription(message *pgproto3.RowDescription) *pgproto3.RowDescription {
	owned := &pgproto3.RowDescription{Fields: make([]pgproto3.FieldDescription, len(message.Fields))}
	for index, field := range message.Fields {
		owned.Fields[index] = field
		owned.Fields[index].Name = append([]byte(nil), field.Name...)
	}
	return owned
}

func sameOIDs(left, right []uint32) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func firstError(responses [][]pgproto3.BackendMessage) *pgproto3.ErrorResponse {
	for _, response := range responses {
		for _, message := range response {
			if response, ok := message.(*pgproto3.ErrorResponse); ok {
				return response
			}
		}
	}
	return nil
}

func protocolMessages(messages []pgproto3.BackendMessage) []pgproto3.BackendMessage {
	filtered := make([]pgproto3.BackendMessage, 0, len(messages))
	for _, message := range messages {
		switch message.(type) {
		case *pgproto3.NoticeResponse, *pgproto3.ParameterStatus, *pgproto3.NotificationResponse:
			continue
		default:
			filtered = append(filtered, message)
		}
	}
	return filtered
}

func isParseDone(message pgproto3.BackendMessage) bool {
	_, complete := message.(*pgproto3.ParseComplete)
	_, failed := message.(*pgproto3.ErrorResponse)
	return complete || failed
}

func isBindDone(message pgproto3.BackendMessage) bool {
	_, complete := message.(*pgproto3.BindComplete)
	_, failed := message.(*pgproto3.ErrorResponse)
	return complete || failed
}

func isDescribeDone(message pgproto3.BackendMessage) bool {
	switch message.(type) {
	case *pgproto3.RowDescription, *pgproto3.NoData, *pgproto3.ErrorResponse:
		return true
	default:
		return false
	}
}

func isCloseDone(message pgproto3.BackendMessage) bool {
	_, complete := message.(*pgproto3.CloseComplete)
	_, failed := message.(*pgproto3.ErrorResponse)
	return complete || failed
}

func isSyncDone(message pgproto3.BackendMessage) bool {
	_, complete := message.(*pgproto3.ReadyForQuery)
	return complete
}

func isExecuteDone(message pgproto3.BackendMessage) bool {
	switch message.(type) {
	case *pgproto3.CommandComplete, *pgproto3.PortalSuspended, *pgproto3.EmptyQueryResponse, *pgproto3.ErrorResponse, *pgproto3.CopyInResponse:
		return true
	default:
		return false
	}
}

func isCopyStarted(message pgproto3.BackendMessage) bool {
	switch message.(type) {
	case *pgproto3.CopyInResponse, *pgproto3.CopyOutResponse, *pgproto3.CopyBothResponse, *pgproto3.ReadyForQuery:
		return true
	default:
		return false
	}
}

func mergedCommandTag(tags []string, rowCount int) string {
	if len(tags) == 0 {
		return ""
	}
	if strings.HasPrefix(tags[0], "SELECT") {
		return fmt.Sprintf("SELECT %d", rowCount)
	}
	if tag, ok := mergeRowCountTags(tags); ok {
		return tag
	}
	for _, tag := range tags[1:] {
		if tag != tags[0] {
			return "FANOUT"
		}
	}
	return tags[0]
}

func mergeRowCountTags(tags []string) (string, bool) {
	if len(tags) == 0 {
		return "", false
	}
	prefix, rows, ok := splitRowCountTag(tags[0])
	if !ok {
		return "", false
	}
	for _, tag := range tags[1:] {
		nextPrefix, nextRows, ok := splitRowCountTag(tag)
		if !ok || nextPrefix != prefix {
			return "", false
		}
		rows += nextRows
	}
	return fmt.Sprintf("%s %d", prefix, rows), true
}

func splitRowCountTag(tag string) (string, int64, bool) {
	parts := strings.Fields(tag)
	if len(parts) < 2 {
		return "", 0, false
	}
	rows, err := strconv.ParseInt(parts[len(parts)-1], 10, 64)
	if err != nil {
		return "", 0, false
	}
	return strings.Join(parts[:len(parts)-1], " "), rows, true
}

func (s *Server) handleSessionQuery(frontend *pgproto3.Backend, session *backend.Session, sql string, state *extendedState) bool {
	if strings.TrimSpace(sql) == "" {
		frontend.Send(&pgproto3.EmptyQueryResponse{})
		frontend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		return true
	}
	if requiresSessionAffinity(sql) {
		state.sessionAffinity = true
	}
	if state.transaction && !state.transactionFailed && state.mutated && s.twoPhaseCommit && (firstSQLKeyword(sql) == "COMMIT" || firstSQLKeyword(sql) == "END") && len(state.writeParticipants) > 1 {
		return s.handleTwoPhaseCommit(frontend, session, state)
	}
	normalized, err := normalizeDDL(sql)
	if err != nil {
		s.sendSessionError(frontend, state.txStatus(), "42601", err.Error())
		return false
	}
	sql = normalized.sql
	var generationErr error
	registry := s.backends.Schema()
	routing, err := router.Prepare(sql)
	if err != nil {
		s.sendSessionError(frontend, state.txStatus(), "42601", err.Error())
		return false
	}
	if firstSQLKeyword(sql) == "INSERT" {
		if _, ok := registry.GeneratedPrimaryKey(routing.Table()); ok {
			if rewritten, generated := router.RewriteGeneratedInsert(sql, registry, "0"); generated {
				id, err := s.backends.NextGlobalID(context.Background())
				if err != nil {
					generationErr = err
				} else {
					rewritten, _ = router.RewriteGeneratedInsert(sql, registry, strconv.FormatInt(id, 10))
					sql = rewritten.SQL
					routing, err = router.Prepare(sql)
					if err != nil {
						generationErr = err
					}
				}
			}
		}
	}
	if generationErr != nil {
		s.sendSessionError(frontend, state.txStatus(), "55000", fmt.Sprintf("allocate globally unique primary key: %v", generationErr))
		return false
	}

	started := time.Now()
	success := false
	errorCategory := "query_execution"
	traceContext, querySpan := otel.Tracer("github.com/jruszo/hamstergres/proxy").Start(context.Background(), "proxy.query",
		trace.WithAttributes(attribute.String("db.operation.name", firstSQLKeyword(sql))))
	defer func() {
		if success {
			querySpan.SetStatus(codes.Ok, "")
		} else {
			querySpan.SetStatus(codes.Error, "query failed")
		}
		querySpan.End()
		if !success {
			s.logger.Error("frontend query failed", "event", "query_failed", "component", "hamstergres-proxy", "correlation_id", fmt.Sprintf("query-%08x", randomUint32()), "error_category", errorCategory)
		}
	}()
	targets := s.backends.ShardNames()
	defer func() {
		s.backends.RecordQueryTargetsCategory(sql, success, time.Since(started), targets, errorCategory)
	}()

	var responses [][]pgproto3.BackendMessage
	var tunnelSpans []trace.Span
	decision, err := s.routePortal(routing, nil, targets)
	if err != nil {
		errorCategory = "sql_error"
		s.sendSessionError(frontend, state.txStatus(), "42601", err.Error())
		return false
	}
	target, routed := decision.target, decision.routed
	if decision.scatterError != "" {
		errorCategory = "unsupported_global_result"
		s.sendSessionError(frontend, state.txStatus(), "0A000", decision.scatterError)
		return false
	}
	if decision.keyedWrite && !routed {
		errorCategory = "unsafe_routing"
		s.sendSessionError(frontend, state.txStatus(), "0A000", "write to a sharded table must include its annotated shard key")
		return false
	}
	if routed && !isTransactionControl(sql) {
		targets = []string{target}
		querySpan.SetAttributes(attribute.String("hamstergres.route", "single_burrow"))
	} else {
		querySpan.SetAttributes(attribute.String("hamstergres.route", "scatter"))
	}
	if requiresFleetWriteOrder(sql, len(targets)) && !session.LockFleetWritesContext(session.Context()) {
		errorCategory = "client_disconnect"
		s.sendSessionError(frontend, state.txStatus(), "57014", "frontend session ended while waiting to execute a write")
		return false
	}
	tunnelSpans = startTunnelSpans(traceContext, targets)
	if len(targets) == 1 {
		responses, err = exchangeOne(session, targets[0], &pgproto3.Query{String: sql}, isQueryDone)
	} else {
		responses, err = exchange(session, targets, &pgproto3.Query{String: sql}, isQueryDone)
	}
	if err != nil {
		errorCategory = "burrow_transport"
		endTunnelSpans(tunnelSpans, err)
		s.sendError(frontend, "08006", err.Error())
		return false
	}
	status := readyTxStatus(responses)
	if response := firstError(responses); response != nil {
		errorCategory = postgresErrorCategory(response.Code)
		endTunnelSpans(tunnelSpans, fmt.Errorf("PostgreSQL error %s", response.Code))
		frontend.Send(response)
		frontend.Send(&pgproto3.ReadyForQuery{TxStatus: status})
		if status == 'I' {
			session.UnlockFleetWrites()
		}
		return false
	}
	endTunnelSpans(tunnelSpans, nil)

	var description *pgproto3.RowDescription
	var rows []*pgproto3.DataRow
	var tags []string
	var notices []*pgproto3.NoticeResponse
	for responseIndex, response := range responses {
		for _, wireMessage := range response {
			switch wireMessage := wireMessage.(type) {
			case *pgproto3.RowDescription:
				if description == nil {
					description = wireMessage
				} else if !sameRowDescription(description, wireMessage) {
					s.sendError(frontend, "XX000", "incompatible row descriptions from Burrows")
					return false
				}
			case *pgproto3.DataRow:
				rows = append(rows, wireMessage)
			case *pgproto3.CommandComplete:
				tags = append(tags, string(wireMessage.CommandTag))
			case *pgproto3.NoticeResponse:
				// A fleet-wide statement produces the same logical notice on every
				// Burrow. Relay one representative stream so PostgreSQL behavior is
				// visible without exposing the physical fan-out.
				if responseIndex == 0 {
					notice := *wireMessage
					notices = append(notices, &notice)
				}
			case *pgproto3.EmptyQueryResponse, *pgproto3.ReadyForQuery, *pgproto3.ParameterStatus, *pgproto3.NotificationResponse:
				// ReadyForQuery is merged after result data. Parameter status and
				// notifications remain Burrow-local asynchronous state.
			case *pgproto3.CopyInResponse, *pgproto3.CopyOutResponse, *pgproto3.CopyBothResponse:
				s.sendError(frontend, "0A000", "COPY is not supported by Hamstergres Proxy")
				return false
			default:
				s.sendError(frontend, "08P01", fmt.Sprintf("unexpected Query response %T", wireMessage))
				return false
			}
		}
	}
	if normalized.schema {
		if err := s.backends.RefreshSchema(context.Background()); err != nil {
			s.sendSessionError(frontend, state.txStatus(), "55000", fmt.Sprintf("refresh schema registry after DDL: %v", err))
			return false
		}
	}
	for _, notice := range notices {
		frontend.Send(notice)
	}
	if description != nil {
		frontend.Send(description)
	}
	for _, row := range rows {
		frontend.Send(row)
	}
	if len(tags) > 0 {
		frontend.Send(&pgproto3.CommandComplete{CommandTag: []byte(mergedCommandTag(tags, len(rows)))})
	} else {
		frontend.Send(&pgproto3.EmptyQueryResponse{})
	}
	if routed {
		recordWriteParticipants(state, sql, []string{target})
	} else {
		recordWriteParticipants(state, sql, targets)
	}
	updateTransactionState(state, sql)
	if invalidatesPreparedStatements(sql) {
		s.backends.InvalidatePreparedStatements(targets)
	}
	frontend.Send(&pgproto3.ReadyForQuery{TxStatus: state.txStatus()})
	if !state.transaction {
		session.UnlockFleetWrites()
	}
	success = true
	return true
}

func postgresErrorCategory(code string) string {
	if len(code) < 2 {
		return "postgres_error"
	}
	switch code[:2] {
	case "08":
		return "burrow_transport"
	case "22", "23":
		return "data_error"
	case "25", "40":
		return "transaction_error"
	case "42":
		return "sql_error"
	case "53":
		return "resource_exhausted"
	default:
		return "postgres_error"
	}
}

func startTunnelSpans(ctx context.Context, burrows []string) []trace.Span {
	tracer := otel.Tracer("github.com/jruszo/hamstergres/proxy")
	spans := make([]trace.Span, 0, len(burrows))
	for _, burrow := range burrows {
		_, span := tracer.Start(ctx, "tunnel.execute", trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(
			attribute.String("hamstergres.burrow", burrow), attribute.String("server.address", burrow)))
		spans = append(spans, span)
	}
	return spans
}

func endTunnelSpans(spans []trace.Span, err error) {
	for _, span := range spans {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "Burrow execution failed")
		} else {
			span.SetStatus(codes.Ok, "")
		}
		span.End()
	}
}

func (s *Server) handleTwoPhaseCommit(frontend *pgproto3.Backend, session *backend.Session, state *extendedState) bool {
	ctx, span := otel.Tracer("github.com/jruszo/hamstergres/proxy").Start(context.Background(), "proxy.query", trace.WithAttributes(
		attribute.String("db.operation.name", "COMMIT"), attribute.String("hamstergres.route", "scatter")))
	defer span.End()
	if response := s.commitTwoPhase(ctx, session, participantNames(state.writeParticipants)); response != nil {
		span.SetStatus(codes.Error, "two-phase commit failed")
		frontend.Send(response)
		frontend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		state.transaction = false
		clear(state.writeParticipants)
		session.UnlockFleetWrites()
		return false
	}
	span.SetStatus(codes.Ok, "")
	state.transaction = false
	state.mutated = false
	state.target = ""
	clear(state.writeParticipants)
	session.UnlockFleetWrites()
	frontend.Send(&pgproto3.CommandComplete{CommandTag: []byte("COMMIT")})
	frontend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	return true
}

func (s *Server) commitTwoPhase(ctx context.Context, session *backend.Session, burrows []string) *pgproto3.ErrorResponse {
	gid := fmt.Sprintf("hamstergres-%08x-%08x", randomUint32(), randomUint32())
	participating := make(map[string]struct{}, len(burrows))
	for _, name := range burrows {
		participating[name] = struct{}{}
	}
	for _, name := range s.backends.ShardNames() {
		if _, ok := participating[name]; !ok {
			if response := runTransactionCommandTraced(ctx, session, name, "ROLLBACK"); response != nil {
				return response
			}
		}
	}
	prepared := make([]string, 0, len(burrows))
	for _, name := range burrows {
		if response := runTransactionCommandTraced(ctx, session, name, "PREPARE TRANSACTION '"+gid+"'"); response != nil {
			s.backends.RecordOperation("two_phase_commit", "prepare_failure")
			for _, preparedName := range prepared {
				_ = runTransactionCommandTraced(ctx, session, preparedName, "ROLLBACK PREPARED '"+gid+"'")
			}
			for _, rollbackName := range burrows[len(prepared):] {
				_ = runTransactionCommandTraced(ctx, session, rollbackName, "ROLLBACK")
			}
			return response
		}
		prepared = append(prepared, name)
	}
	for _, name := range prepared {
		if response := runTransactionCommandTraced(ctx, session, name, "COMMIT PREPARED '"+gid+"'"); response != nil {
			s.backends.RecordOperation("two_phase_commit", "uncertain")
			s.logger.Error("two-phase commit outcome is uncertain", "event", "two_phase_commit_uncertain", "component", "hamstergres-proxy", "burrow", name, "transaction_id", gid, "error_category", "commit_uncertain", "error", response.Message)
			return &pgproto3.ErrorResponse{
				Severity: "ERROR",
				Code:     "40003",
				Message:  fmt.Sprintf("two-phase commit %s is in doubt after Burrow %s failed: %s", gid, name, response.Message),
			}
		}
	}
	s.backends.RecordOperation("two_phase_commit", "success")
	return nil
}

func runTransactionCommandTraced(ctx context.Context, session *backend.Session, burrow, sql string) *pgproto3.ErrorResponse {
	spans := startTunnelSpans(ctx, []string{burrow})
	response := runTransactionCommand(session, burrow, sql)
	if response != nil {
		endTunnelSpans(spans, fmt.Errorf("PostgreSQL error %s", response.Code))
	} else {
		endTunnelSpans(spans, nil)
	}
	return response
}

func runTransactionCommand(session *backend.Session, burrow, sql string) *pgproto3.ErrorResponse {
	responses, err := exchangeOne(session, burrow, &pgproto3.Query{String: sql}, isQueryDone)
	if err != nil {
		return &pgproto3.ErrorResponse{Severity: "ERROR", Code: "08006", Message: err.Error()}
	}
	return firstError(responses)
}

type routeDecision struct {
	target       string
	routed       bool
	keyedWrite   bool
	scatterError string
}

func (s *Server) routeSQL(sql string, burrows []string) (routeDecision, error) {
	plan, err := router.Analyze(sql, nil, s.backends.Schema(), burrows)
	if err != nil {
		return routeDecision{}, err
	}
	return s.routePlan(plan, burrows), nil
}

func (s *Server) routePortal(prepared *router.Prepared, parameters [][]byte, burrows []string) (routeDecision, error) {
	if prepared == nil {
		// Preserve PostgreSQL's normal 26000 error for an unknown or closed
		// statement by leaving it unrouted and letting the backend execute it.
		return s.routePlan(router.Plan{}, burrows), nil
	}
	return s.routePlan(prepared.Analyze(parameters, s.backends.Schema(), burrows), burrows), nil
}

func (s *Server) routePlan(plan router.Plan, burrows []string) routeDecision {
	decision := routeDecision{target: plan.Target, routed: plan.Routed, keyedWrite: plan.Write && (plan.Table == "" || plan.Sharded), scatterError: plan.ScatterError}
	if plan.SingleBurrow {
		if len(burrows) == 0 {
			return decision
		}
		if s.backends.UnshardedMode() == "primary" {
			decision.target = s.backends.PrimaryBurrow()
		} else if plan.Deterministic {
			decision.target = s.balancedBurrow(burrows)
		} else {
			decision.target = randomBurrow(burrows)
		}
		decision.routed = decision.target != ""
		return decision
	}
	if plan.Routed || plan.Table == "" || plan.Sharded {
		return decision
	}
	if s.backends.UnshardedMode() == "primary" {
		decision.target, decision.routed = s.backends.PrimaryBurrow(), true
		return decision
	}
	if plan.Write {
		return decision
	}
	if len(burrows) == 0 {
		return decision
	}
	decision.target = randomBurrow(burrows)
	decision.routed = true
	return decision
}

func (s *Server) balancedBurrow(burrows []string) string {
	if len(burrows) == 0 {
		return ""
	}
	ordered := append([]string(nil), burrows...)
	sort.Strings(ordered)
	index := s.topologyReadIndex.Add(1) - 1
	return ordered[index%uint64(len(ordered))]
}

func randomBurrow(burrows []string) string {
	if len(burrows) == 0 {
		return ""
	}
	var value [8]byte
	if _, err := rand.Read(value[:]); err != nil {
		return burrows[0]
	}
	return burrows[binary.LittleEndian.Uint64(value[:])%uint64(len(burrows))]
}

func requiresRoutedWrite(sql string) bool {
	switch firstSQLKeyword(sql) {
	case "INSERT", "UPDATE", "DELETE", "MERGE":
		return true
	default:
		return false
	}
}

func recordWriteParticipants(state *extendedState, sql string, targets []string) {
	if !state.transaction || !requiresRoutedWrite(sql) {
		return
	}
	for _, target := range targets {
		state.writeParticipants[target] = struct{}{}
	}
	state.mutated = true
}

func invalidatesPreparedStatements(sql string) bool {
	switch firstSQLKeyword(sql) {
	case "DEALLOCATE", "DISCARD":
		return true
	default:
		return false
	}
}

func updateTransactionState(state *extendedState, sql string) {
	switch firstSQLKeyword(sql) {
	case "BEGIN", "START":
		state.transaction = true
		state.transactionFailed = false
		state.target = ""
		state.mutated = false
		clear(state.writeParticipants)
	case "COMMIT", "END", "ROLLBACK", "ABORT":
		state.transaction = false
		state.transactionFailed = false
		state.target = ""
		state.mutated = false
		clear(state.writeParticipants)
	}
}

func participantNames(participants map[string]struct{}) []string {
	names := make([]string, 0, len(participants))
	for name := range participants {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func readyTxStatus(responses [][]pgproto3.BackendMessage) byte {
	status := byte('I')
	for _, response := range responses {
		for _, message := range response {
			ready, ok := message.(*pgproto3.ReadyForQuery)
			if !ok {
				continue
			}
			if status == 'I' {
				status = ready.TxStatus
			} else if ready.TxStatus != status {
				return 'E'
			}
		}
	}
	return status
}

func isQueryDone(message pgproto3.BackendMessage) bool {
	_, complete := message.(*pgproto3.ReadyForQuery)
	return complete
}

func isTransactionControl(sql string) bool {
	keyword := firstSQLKeyword(sql)
	return keyword == "BEGIN" || keyword == "START" || keyword == "COMMIT" || keyword == "END" || keyword == "ROLLBACK" || keyword == "ABORT"
}

func requiresFleetWriteOrder(sql string, targetCount int) bool {
	if targetCount < 2 {
		return false
	}
	switch firstSQLKeyword(sql) {
	case "CREATE", "ALTER", "COMMENT", "DROP", "TRUNCATE":
		return true
	default:
		return false
	}
}

func firstSQLKeyword(sql string) string {
	trimmed := strings.TrimSpace(sql)
	for {
		if strings.HasPrefix(trimmed, "--") {
			if index := strings.IndexByte(trimmed, '\n'); index >= 0 {
				trimmed = strings.TrimSpace(trimmed[index+1:])
				continue
			}
			return ""
		}
		if strings.HasPrefix(trimmed, "/*") {
			index := strings.Index(trimmed, "*/")
			if index < 0 {
				return ""
			}
			trimmed = strings.TrimSpace(trimmed[index+2:])
			continue
		}
		break
	}
	for index, r := range trimmed {
		if r < 'A' || r > 'Z' && r < 'a' || r > 'z' {
			return strings.ToUpper(trimmed[:index])
		}
	}
	return strings.ToUpper(trimmed)
}

func containsCopyStatement(sql string) bool {
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return false
	}
	for _, raw := range tree.Stmts {
		if raw.Stmt != nil && raw.Stmt.GetCopyStmt() != nil {
			return true
		}
	}
	return false
}

func requiresSessionAffinity(sql string) bool {
	tree, err := pg_query.Parse(sql)
	if err != nil {
		switch firstSQLKeyword(sql) {
		case "SET", "RESET", "DISCARD", "LISTEN", "UNLISTEN", "PREPARE":
			return true
		}
		return false
	}
	for _, raw := range tree.Stmts {
		statement := raw.Stmt
		if statement == nil {
			continue
		}
		if statement.GetVariableSetStmt() != nil || statement.GetDiscardStmt() != nil || statement.GetListenStmt() != nil || statement.GetUnlistenStmt() != nil || statement.GetPrepareStmt() != nil {
			return true
		}
	}
	return false
}

func (s *Server) handleQuery(frontend *pgproto3.Backend, sql string) {
	if strings.TrimSpace(sql) == "" {
		frontend.Send(&pgproto3.EmptyQueryResponse{})
		frontend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		return
	}
	success := false
	errorCategory := "query_execution"
	traceContext, querySpan := otel.Tracer("github.com/jruszo/hamstergres/proxy").Start(context.Background(), "proxy.query",
		trace.WithAttributes(attribute.String("db.operation.name", firstSQLKeyword(sql))))
	defer func() {
		if success {
			querySpan.SetStatus(codes.Ok, "")
		} else {
			querySpan.SetStatus(codes.Error, "query failed")
		}
		querySpan.End()
		if !success {
			s.logger.Error("frontend query failed", "event", "query_failed", "component", "hamstergres-proxy", "correlation_id", fmt.Sprintf("query-%08x", randomUint32()), "error_category", errorCategory)
		}
	}()
	normalized, err := normalizeDDL(sql)
	if err != nil {
		errorCategory = "sql_error"
		s.sendError(frontend, "42601", err.Error())
		return
	}
	sql = normalized.sql
	registry := s.backends.Schema()
	routing, err := router.Prepare(sql)
	if err != nil {
		errorCategory = "sql_error"
		s.sendError(frontend, "42601", err.Error())
		return
	}
	if firstSQLKeyword(sql) == "INSERT" {
		if _, ok := registry.GeneratedPrimaryKey(routing.Table()); ok {
			if _, generated := router.RewriteGeneratedInsert(sql, registry, "0"); generated {
				id, err := s.backends.NextGlobalID(context.Background())
				if err != nil {
					errorCategory = "nest_unavailable"
					s.sendError(frontend, "55000", fmt.Sprintf("allocate globally unique primary key: %v", err))
					return
				}
				rewritten, _ := router.RewriteGeneratedInsert(sql, registry, strconv.FormatInt(id, 10))
				sql = rewritten.SQL
				routing, err = router.Prepare(sql)
				if err != nil {
					errorCategory = "sql_error"
					s.sendError(frontend, "42601", err.Error())
					return
				}
			}
		}
	}
	if requiresFleetWriteOrder(sql, len(s.backends.ShardNames())) {
		unlock := s.backends.LockFleetWrites()
		defer unlock()
	}
	var result backend.Result
	decision, err := s.routePortal(routing, nil, s.backends.ShardNames())
	if err != nil {
		errorCategory = "sql_error"
		s.sendError(frontend, "42601", err.Error())
		return
	}
	target, routed := decision.target, decision.routed
	if decision.scatterError != "" {
		errorCategory = "unsupported_global_result"
		s.sendError(frontend, "0A000", decision.scatterError)
		return
	}
	if decision.keyedWrite && !routed {
		errorCategory = "unsafe_routing"
		s.sendError(frontend, "0A000", "write to a sharded table must include its annotated shard key")
		return
	}
	targets := s.backends.ShardNames()
	if routed {
		targets = []string{target}
		querySpan.SetAttributes(attribute.String("hamstergres.route", "single_burrow"))
	} else {
		querySpan.SetAttributes(attribute.String("hamstergres.route", "scatter"))
	}
	tunnelSpans := startTunnelSpans(traceContext, targets)
	if routed {
		result, err = s.backends.QueryOne(context.Background(), sql, target)
	} else {
		result, err = s.backends.QueryAll(context.Background(), sql)
	}
	if err != nil {
		errorCategory = classifyProxyError(err)
		endTunnelSpans(tunnelSpans, err)
		s.sendError(frontend, "XX000", err.Error())
		return
	}
	endTunnelSpans(tunnelSpans, nil)
	if normalized.schema {
		if err := s.backends.RefreshSchema(context.Background()); err != nil {
			errorCategory = "schema_registry"
			s.sendError(frontend, "55000", fmt.Sprintf("refresh schema registry after DDL: %v", err))
			return
		}
	}
	if invalidatesPreparedStatements(sql) {
		s.backends.InvalidatePreparedStatements(targets)
	}
	if len(result.Fields) > 0 {
		frontend.Send(&pgproto3.RowDescription{Fields: result.Fields})
		for _, values := range result.Rows {
			frontend.Send(&pgproto3.DataRow{Values: values})
		}
	}
	frontend.Send(&pgproto3.CommandComplete{CommandTag: []byte(result.CommandTag)})
	frontend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	success = true
}

func classifyProxyError(err error) string {
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) {
		return postgresErrorCategory(postgresError.Code)
	}
	return "burrow_transport"
}

func (s *Server) sendError(frontend *pgproto3.Backend, code, message string) {
	s.sendSessionError(frontend, 'I', code, message)
}

func (s *Server) sendSessionError(frontend *pgproto3.Backend, status byte, code, message string) {
	frontend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: code, Message: message})
	frontend.Send(&pgproto3.ReadyForQuery{TxStatus: status})
}

// sendExtendedError follows the extended-query recovery rule: the frontend
// receives ReadyForQuery only in response to its later Sync message.
func (s *Server) sendExtendedError(frontend *pgproto3.Backend, code, message string) {
	frontend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: code, Message: message})
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
