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
	"sync"
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
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/jruszo/hamstergres/internal/backend"
	"github.com/jruszo/hamstergres/internal/copyrouter"
	"github.com/jruszo/hamstergres/internal/ddl"
	"github.com/jruszo/hamstergres/internal/router"
	"github.com/jruszo/hamstergres/internal/schema"
)

// Server exposes the PostgreSQL frontend protocol for Hamstergres.
type Server struct {
	backends        *backend.Manager
	logger          *slog.Logger
	twoPhaseCommit  bool
	schemaRefreshMu sync.Mutex

	connections          atomic.Int64
	activeConnections    atomic.Int64
	topologyReadIndex    atomic.Uint64
	schemaRefreshPending atomic.Bool
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
		switch message := message.(type) {
		case *pgproto3.SSLRequest:
			if _, err := conn.Write([]byte("N")); err != nil {
				return err
			}
			continue
		case *pgproto3.CancelRequest:
			return nil
		case *pgproto3.StartupMessage:
			runtimeParams := startupRuntimeParameters(message)
			if err := s.sendStartup(frontend, runtimeParams); err != nil {
				return err
			}
			return s.serveQueries(frontend, runtimeParams)
		default:
			return fmt.Errorf("unexpected startup message %T", message)
		}
	}
}

func (s *Server) sendStartup(frontend *pgproto3.Backend, runtimeParams map[string]string) error {
	dateStyle := runtimeParamOrDefault(runtimeParams, "DateStyle", "ISO, MDY")
	intervalStyle := runtimeParamOrDefault(runtimeParams, "IntervalStyle", "postgres")
	standardConformingStrings := runtimeParamOrDefault(runtimeParams, "standard_conforming_strings", "on")
	timezone := runtimeParamOrDefault(runtimeParams, "TimeZone", "UTC")
	frontend.Send(&pgproto3.AuthenticationOk{})
	frontend.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
	frontend.Send(&pgproto3.ParameterStatus{Name: "DateStyle", Value: dateStyle})
	frontend.Send(&pgproto3.ParameterStatus{Name: "integer_datetimes", Value: "on"})
	frontend.Send(&pgproto3.ParameterStatus{Name: "IntervalStyle", Value: intervalStyle})
	frontend.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "17.0"})
	frontend.Send(&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: standardConformingStrings})
	frontend.Send(&pgproto3.ParameterStatus{Name: "TimeZone", Value: timezone})
	frontend.Send(&pgproto3.BackendKeyData{
		ProcessID: randomUint32(),
		SecretKey: binary.BigEndian.AppendUint32(nil, randomUint32()),
	})
	frontend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	return frontend.Flush()
}

func runtimeParamOrDefault(runtimeParams map[string]string, name, defaultValue string) string {
	if value := runtimeParams[name]; value != "" {
		return value
	}
	return defaultValue
}

func startupRuntimeParameters(startup *pgproto3.StartupMessage) map[string]string {
	parameters := make(map[string]string)
	if startup == nil {
		return parameters
	}
	if options, ok := startupParameter(startup.Parameters, "options"); ok {
		parseStartupOptions(parameters, options)
	}
	for _, direct := range []struct{ startup, runtime string }{
		{startup: "datestyle", runtime: "DateStyle"},
		{startup: "timezone", runtime: "TimeZone"},
		{startup: "application_name", runtime: "application_name"},
	} {
		if value, ok := startupParameter(startup.Parameters, direct.startup); ok {
			parameters[direct.runtime] = value
		}
	}
	return parameters
}

func startupParameter(parameters map[string]string, wanted string) (string, bool) {
	for name, value := range parameters {
		if strings.EqualFold(name, wanted) {
			return value, true
		}
	}
	return "", false
}

func parseStartupOptions(parameters map[string]string, options string) {
	fields := strings.Fields(options)
	for index := 0; index < len(fields); index++ {
		assignment := ""
		switch {
		case fields[index] == "-c" && index+1 < len(fields):
			index++
			assignment = fields[index]
		case strings.HasPrefix(fields[index], "-c"):
			assignment = strings.TrimPrefix(fields[index], "-c")
		case strings.HasPrefix(fields[index], "--"):
			assignment = strings.TrimPrefix(fields[index], "--")
		}
		name, value, found := strings.Cut(assignment, "=")
		if !found || !validSettingName(name) {
			continue
		}
		switch strings.ToLower(name) {
		case "datestyle":
			name = "DateStyle"
		case "timezone":
			name = "TimeZone"
		case "intervalstyle":
			name = "IntervalStyle"
		case "standard_conforming_strings":
			name = "standard_conforming_strings"
		case "application_name":
			name = "application_name"
		}
		parameters[name] = value
	}
}

func validSettingName(name string) bool {
	if name == "" {
		return false
	}
	for _, character := range name {
		if character != '_' && (character < 'a' || character > 'z') && (character < 'A' || character > 'Z') && (character < '0' || character > '9') {
			return false
		}
	}
	return true
}

func (s *Server) serveQueries(frontend *pgproto3.Backend, runtimeParams map[string]string) error {
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

	state := extendedState{statements: make(map[string]statementState), portals: make(map[string]portalState), writeParticipants: make(map[string]struct{}), temporaryRelations: make(map[string]struct{})}
	defer finishCopyTrace(&state, fmt.Errorf("frontend session ended during COPY"))
	var session *backend.Session
	defer func() {
		if session != nil {
			session.Close(context.Background(), !state.sessionDestroy, state.sessionDiscardPrepared)
		}
	}()

	ensureSession := func() (*backend.Session, bool) {
		if session != nil {
			return session, true
		}
		created, err := s.backends.NewSession(sessionContext, runtimeParams, state.sessionReplaySQL())
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
		if _, terminate := message.(*pgproto3.Terminate); !terminate {
			if _, syncMessage := message.(*pgproto3.Sync); !syncMessage && (state.schemaDirty || s.schemaRefreshPending.Load()) {
				if err := s.refreshSchemaBarrier(context.Background(), state.schemaDirty); err != nil {
					if _, simpleQuery := message.(*pgproto3.Query); simpleQuery {
						s.sendSessionError(frontend, state.txStatus(), "55000", fmt.Sprintf("refresh schema registry after DDL: %v", err))
					} else {
						s.sendExtendedError(frontend, "55000", fmt.Sprintf("refresh schema registry after DDL: %v", err))
						state.failed = true
					}
					if flushErr := frontend.Flush(); flushErr != nil {
						return flushErr
					}
					continue
				}
				state.schemaDirty = false
			}
		}
		switch message := message.(type) {
		case *pgproto3.Parse:
			if !state.failed {
				prepared, err := prepareStatement(message, s.backends.Schema())
				if err != nil {
					s.sendExtendedError(frontend, "42601", err.Error())
					state.failed = true
					break
				}
				if _, err := state.validateAtomicFleetDDL(s, prepared.sql, len(s.backends.ShardNames())); err != nil {
					s.sendExtendedError(frontend, "0A000", err.Error())
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
				} else if session != nil || len(runtimeParams) > 0 || len(state.sessionSettings) > 0 || requiresSessionBackend(message.String) || isTransactionControl(message.String) || isServerLocalDDL(message.String) || requiresFleetWriteOrder(message.String, len(s.backends.ShardNames())) {
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
				temporaryDDL := state.referencesTemporaryDDL(statement.sql)
				if requiresFleetWriteOrder(statement.sql, len(s.backends.ShardNames())) && !temporaryDDL {
					decision.target = ""
					decision.routed = false
				}
				portal := portalState{
					sql:        statement.sql,
					parameters: parameters,
					schema:     statement.schema,
					target:     decision.target,
					routed:     decision.routed,
					keyedWrite: decision.keyedWrite,
				}
				if state.pending == nil && !isTransactionControl(statement.sql) {
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
		if _, query := message.(*pgproto3.Query); query && session != nil && !state.transaction && !state.sessionAffinity && !state.copyIn && state.pending == nil && !state.failed {
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
	statements             map[string]statementState
	portals                map[string]portalState
	failed                 bool
	transaction            bool
	transactionFailed      bool
	mutated                bool
	target                 string
	schemaDirty            bool
	copyIn                 bool
	copyTargets            []string
	copyPlan               copyrouter.Plan
	copyStream             *copyrouter.Stream
	copyReplicated         bool
	copyAborted            bool
	copyTraceSpan          trace.Span
	copyTunnelSpans        []trace.Span
	writeParticipants      map[string]struct{}
	pending                *pendingExtended
	syncConsumed           bool
	sessionAffinity        bool
	sessionDestroy         bool
	sessionDiscardPrepared bool
	sessionSettings        []sessionSetting
	temporaryRelations     map[string]struct{}
	temporaryOnCommitDrop  map[string]struct{}
	temporaryIndexParents  map[string]string
	temporarySnapshots     []temporaryRelationSnapshot
}

type temporaryRelationSnapshot struct {
	name         string
	relations    map[string]struct{}
	onCommitDrop map[string]struct{}
	indexParents map[string]string
}

type sessionSetting struct {
	name string
	sql  string
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

// validateTransactionalFleetDDL rejects schema commands that PostgreSQL
// cannot safely execute inside the coordinated transaction used for a
// multi-Burrow schema change. This check runs before a Session acquires any
// Tunnel, so unsupported commands cannot partially mutate the fleet.
func validateTransactionalFleetDDL(sql string) error {
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return fmt.Errorf("parse PostgreSQL statement: %w", err)
	}
	for _, raw := range tree.Stmts {
		if raw.Stmt == nil {
			continue
		}
		if raw.Stmt.GetTransactionStmt() != nil {
			return fmt.Errorf("transaction control cannot be combined with fleet-wide DDL in one simple-query batch")
		}
		switch {
		case raw.Stmt.GetCreatedbStmt() != nil,
			raw.Stmt.GetDropdbStmt() != nil,
			raw.Stmt.GetCreateTableSpaceStmt() != nil,
			raw.Stmt.GetDropTableSpaceStmt() != nil,
			raw.Stmt.GetAlterSystemStmt() != nil:
			return fmt.Errorf("non-transactional fleet-wide DDL is not supported; use hamstergres-migrations")
		case raw.Stmt.GetCreateFunctionStmt() != nil:
			return fmt.Errorf("user-defined functions and procedures are not supported across Burrows; use hamstergres-migrations")
		}
		if index := raw.Stmt.GetIndexStmt(); index != nil && index.Concurrent {
			return fmt.Errorf("concurrent index DDL is not supported across Burrows; use hamstergres-migrations")
		}
		if drop := raw.Stmt.GetDropStmt(); drop != nil && drop.Concurrent {
			return fmt.Errorf("concurrent index DDL is not supported across Burrows; use hamstergres-migrations")
		}
	}
	return nil
}

func (s *Server) validateAtomicFleetDDL(sql string, targetCount int) error {
	// Function bodies can hide writes or dynamic DDL. Reject their creation
	// even with one current Burrow so later topology growth cannot expose an
	// uncoordinated executable catalog object.
	tree, err := pg_query.Parse(sql)
	if err == nil {
		for _, raw := range tree.Stmts {
			if raw.Stmt != nil && raw.Stmt.GetCreateFunctionStmt() != nil {
				return fmt.Errorf("user-defined functions and procedures are not supported across Burrows; use hamstergres-migrations")
			}
		}
	}
	if isServerLocalDDL(sql) {
		return validateTransactionalFleetDDL(sql)
	}
	if !requiresFleetWriteOrder(sql, targetCount) {
		return nil
	}
	if err := validateTransactionalFleetDDL(sql); err != nil {
		return err
	}
	if !s.twoPhaseCommit {
		return fmt.Errorf("fleet-wide DDL requires two-phase commit")
	}
	return nil
}

func normalizeDDL(sql string) (normalizedSQL, error) {
	keyword := firstSQLKeyword(sql)
	if keyword == "COMMENT" || keyword == "DROP" {
		return normalizedSQL{sql: sql, schema: true}, nil
	}
	if keyword != "CREATE" && keyword != "ALTER" {
		return normalizedSQL{sql: sql, schema: containsFleetDDL(sql)}, nil
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

// refreshSchemaAfterDDL marks the process-wide routing registry stale before
// attempting publication. The pending bit survives frontend disconnects and
// is cleared only after a successful refresh.
func (s *Server) refreshSchemaAfterDDL(ctx context.Context) error {
	s.schemaRefreshPending.Store(true)
	return s.refreshSchemaIfPending(ctx)
}

func (s *Server) refreshSchemaIfPending(ctx context.Context) error {
	if !s.schemaRefreshPending.Load() {
		return nil
	}
	s.schemaRefreshMu.Lock()
	defer s.schemaRefreshMu.Unlock()
	if !s.schemaRefreshPending.Load() {
		return nil
	}
	if err := s.backends.RefreshSchema(ctx); err != nil {
		return err
	}
	s.schemaRefreshPending.Store(false)
	return nil
}

func (s *Server) refreshSchemaBarrier(ctx context.Context, localDirty bool) error {
	if localDirty {
		return s.refreshSchemaAfterDDL(ctx)
	}
	return s.refreshSchemaIfPending(ctx)
}

func (s *Server) handleSync(frontend *pgproto3.Backend, session *backend.Session, state *extendedState) bool {
	if !state.transaction && (state.schemaDirty || s.schemaRefreshPending.Load()) {
		if err := s.refreshSchemaBarrier(context.Background(), state.schemaDirty); err != nil {
			frontend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "55000", Message: fmt.Sprintf("refresh schema registry after DDL: %v", err)})
			frontend.Send(&pgproto3.ReadyForQuery{TxStatus: state.txStatus()})
			session.UnlockFleetWrites()
			return true
		}
		state.schemaDirty = false
	}
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
	atomicDDLStarted := false
	fail := func(code, message string) bool {
		if atomicDDLStarted {
			s.rollbackAtomicFleetDDL(context.Background(), session, pending.targets)
			atomicDDLStarted = false
		}
		if fleetWriteGateAcquired && !state.transaction {
			session.UnlockFleetWrites()
			fleetWriteGateAcquired = false
		}
		return s.failPendingExtended(frontend, state, emitReady, code, message)
	}

	messages := []pgproto3.FrontendMessage{pending.bind}
	sessionPolicy := sessionStatePolicy{}
	if pending.describe != nil {
		messages = append(messages, pending.describe)
	}
	if pending.execute != nil {
		sessionPolicy = state.classifySessionState(pending.portal.sql)
		state.beginSessionState(sessionPolicy)
		temporaryDDL, err := state.validateAtomicFleetDDL(s, pending.portal.sql, len(pending.targets))
		if err != nil {
			return fail("0A000", err.Error())
		}
		if requiresFleetWriteOrder(pending.portal.sql, len(pending.targets)) && !temporaryDDL && !session.LockFleetWritesContext(session.Context()) {
			return s.failPendingExtended(frontend, state, emitReady, "57014", "frontend session ended while waiting to execute a write")
		}
		fleetWriteGateAcquired = requiresFleetWriteOrder(pending.portal.sql, len(pending.targets)) && !temporaryDDL
		if !state.transaction && fleetWriteGateAcquired && !temporaryDDL {
			if response := s.beginAtomicFleetDDL(context.Background(), session, pending.targets); response != nil {
				return fail(response.Code, response.Message)
			}
			atomicDDLStarted = true
		}
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
			case *pgproto3.ParameterStatus:
				if target == pending.targets[0] {
					frontend.Send(wireMessage)
				}
			case *pgproto3.NoticeResponse, *pgproto3.NotificationResponse:
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
	if atomicDDLStarted {
		if backendFailed {
			s.rollbackAtomicFleetDDL(context.Background(), session, pending.targets)
		} else if response := s.commitAtomicFleetDDL(context.Background(), session, pending.targets); response != nil {
			frontend.Send(response)
			backendFailed = true
			success = false
			errorCategory = postgresErrorCategory(response.Code)
		}
		atomicDDLStarted = false
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
			temporaryDDL := state.referencesTemporaryDDL(pending.portal.sql)
			if !temporaryDDL {
				recordWriteParticipants(state, pending.portal.sql, pending.targets)
			}
			if pending.portal.schema && !temporaryDDL {
				state.schemaDirty = true
			}
			state.applySuccessfulSQLState(pending.portal.sql)
			state.commitSessionState(sessionPolicy)
			if !state.transaction && (firstSQLKeyword(pending.portal.sql) == "ROLLBACK" || firstSQLKeyword(pending.portal.sql) == "ABORT") {
				state.schemaDirty = false
			}
		}
	}
	if state.schemaDirty && !backendFailed && !state.transaction {
		if err := s.refreshSchemaAfterDDL(context.Background()); err != nil {
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
	sessionPolicy := state.classifySessionState(portal.sql)
	state.beginSessionState(sessionPolicy)
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
		if state.schemaDirty {
			if err := s.refreshSchemaAfterDDL(context.Background()); err != nil {
				s.sendExtendedError(frontend, "55000", fmt.Sprintf("refresh schema registry after DDL: %v", err))
				state.transaction = false
				state.mutated = false
				clear(state.writeParticipants)
				session.UnlockFleetWrites()
				return false
			}
			state.schemaDirty = false
		}
		frontend.Send(&pgproto3.CommandComplete{CommandTag: []byte("COMMIT")})
		state.finishTemporaryTransaction(true, false)
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
	temporaryDDL, err := state.validateAtomicFleetDDL(s, portal.sql, len(targets))
	if err != nil {
		errorCategory = "unsupported_ddl"
		s.sendExtendedError(frontend, "0A000", err.Error())
		return false
	}
	atomicDDL := !state.transaction && requiresFleetWriteOrder(portal.sql, len(targets)) && !temporaryDDL
	if requiresFleetWriteOrder(portal.sql, len(targets)) && !temporaryDDL && !session.LockFleetWritesContext(session.Context()) {
		errorCategory = "client_disconnect"
		s.sendExtendedError(frontend, "57014", "frontend session ended while waiting to execute a write")
		return false
	}
	if atomicDDL {
		if response := s.beginAtomicFleetDDL(traceContext, session, targets); response != nil {
			errorCategory = postgresErrorCategory(response.Code)
			frontend.Send(response)
			session.UnlockFleetWrites()
			return false
		}
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
		if atomicDDL {
			s.rollbackAtomicFleetDDL(traceContext, session, targets)
			session.UnlockFleetWrites()
		}
		s.sendExtendedError(frontend, "08006", err.Error())
		return false
	}
	if response := firstError(responses); response != nil {
		errorCategory = postgresErrorCategory(response.Code)
		endTunnelSpans(tunnelSpans, fmt.Errorf("PostgreSQL error %s", response.Code))
		if atomicDDL {
			s.rollbackAtomicFleetDDL(traceContext, session, targets)
			session.UnlockFleetWrites()
		}
		frontend.Send(response)
		return false
	}
	endTunnelSpans(tunnelSpans, nil)
	if atomicDDL {
		if response := s.commitAtomicFleetDDL(traceContext, session, targets); response != nil {
			errorCategory = postgresErrorCategory(response.Code)
			frontend.Send(response)
			session.UnlockFleetWrites()
			return false
		}
	}

	var description *pgproto3.RowDescription
	var rows []*pgproto3.DataRow
	var tags []string
	portalSuspended := false
	for responseIndex, response := range responses {
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
			case *pgproto3.ParameterStatus:
				if responseIndex == 0 {
					frontend.Send(wireMessage)
				}
			case *pgproto3.EmptyQueryResponse, *pgproto3.NoticeResponse, *pgproto3.NotificationResponse:
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
	if portal.schema && !temporaryDDL {
		state.schemaDirty = true
	}
	state.commitSessionState(sessionPolicy)
	if !temporaryDDL {
		recordWriteParticipants(state, portal.sql, targets)
	}
	state.applySuccessfulSQLState(portal.sql)
	if !state.transaction && (firstSQLKeyword(portal.sql) == "ROLLBACK" || firstSQLKeyword(portal.sql) == "ABORT") {
		state.schemaDirty = false
	}
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
	sessionPolicy := state.classifySessionState(sql)
	state.beginSessionState(sessionPolicy)
	if state.transaction && !state.transactionFailed && state.mutated && s.twoPhaseCommit && (firstSQLKeyword(sql) == "COMMIT" || firstSQLKeyword(sql) == "END") && len(state.writeParticipants) > 1 {
		return s.handleTwoPhaseCommit(frontend, session, state)
	}
	normalized, err := normalizeDDL(sql)
	if err != nil {
		s.sendSessionError(frontend, state.txStatus(), "42601", err.Error())
		return false
	}
	sql = normalized.sql
	temporaryDDL, err := state.validateAtomicFleetDDL(s, sql, len(s.backends.ShardNames()))
	if err != nil {
		s.sendSessionError(frontend, state.txStatus(), "0A000", err.Error())
		return false
	}
	var generationErr error
	registry := s.backends.Schema()
	routing, err := router.Prepare(sql)
	parserFallback := err != nil
	if !parserFallback && firstSQLKeyword(sql) == "INSERT" {
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
	decision, err := s.resolveRouteDecision(routing, err, targets)
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
	if requiresFleetWriteOrder(sql, len(targets)) && !temporaryDDL {
		routed = false
	} else if routed && !isTransactionControl(sql) {
		targets = []string{target}
		querySpan.SetAttributes(attribute.String("hamstergres.route", "single_burrow"))
	} else {
		querySpan.SetAttributes(attribute.String("hamstergres.route", "scatter"))
	}
	if !temporaryDDL {
		if err := s.validateAtomicFleetDDL(sql, len(targets)); err != nil {
			errorCategory = "unsupported_ddl"
			s.sendSessionError(frontend, state.txStatus(), "0A000", err.Error())
			return false
		}
	}
	atomicDDL := !state.transaction && requiresFleetWriteOrder(sql, len(targets)) && !temporaryDDL
	if requiresFleetWriteOrder(sql, len(targets)) && !temporaryDDL && !session.LockFleetWritesContext(session.Context()) {
		errorCategory = "client_disconnect"
		s.sendSessionError(frontend, state.txStatus(), "57014", "frontend session ended while waiting to execute a write")
		return false
	}
	if atomicDDL {
		if response := s.beginAtomicFleetDDL(traceContext, session, targets); response != nil {
			errorCategory = postgresErrorCategory(response.Code)
			frontend.Send(response)
			frontend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			session.UnlockFleetWrites()
			return false
		}
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
		if atomicDDL {
			s.rollbackAtomicFleetDDL(traceContext, session, targets)
			session.UnlockFleetWrites()
		}
		s.sendError(frontend, "08006", err.Error())
		return false
	}
	status := readyTxStatus(responses)
	if response := firstError(responses); response != nil {
		errorCategory = postgresErrorCategory(response.Code)
		endTunnelSpans(tunnelSpans, fmt.Errorf("PostgreSQL error %s", response.Code))
		if atomicDDL {
			s.rollbackAtomicFleetDDL(traceContext, session, targets)
			status = 'I'
		}
		frontend.Send(response)
		frontend.Send(&pgproto3.ReadyForQuery{TxStatus: status})
		if status == 'I' || atomicDDL {
			session.UnlockFleetWrites()
		}
		return false
	}
	endTunnelSpans(tunnelSpans, nil)
	if atomicDDL {
		if response := s.commitAtomicFleetDDL(traceContext, session, targets); response != nil {
			errorCategory = postgresErrorCategory(response.Code)
			frontend.Send(response)
			frontend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			session.UnlockFleetWrites()
			return false
		}
	}

	var description *pgproto3.RowDescription
	var rows []*pgproto3.DataRow
	var tags []string
	var notices []*pgproto3.NoticeResponse
	var parameterStatuses []*pgproto3.ParameterStatus
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
			case *pgproto3.ParameterStatus:
				if responseIndex == 0 {
					status := *wireMessage
					parameterStatuses = append(parameterStatuses, &status)
				}
			case *pgproto3.EmptyQueryResponse, *pgproto3.ReadyForQuery, *pgproto3.NotificationResponse:
				// ReadyForQuery is merged after result data. Notifications remain
				// Burrow-local asynchronous state.
			case *pgproto3.CopyInResponse, *pgproto3.CopyOutResponse, *pgproto3.CopyBothResponse:
				s.sendError(frontend, "0A000", "COPY is not supported by Hamstergres Proxy")
				return false
			default:
				s.sendError(frontend, "08P01", fmt.Sprintf("unexpected Query response %T", wireMessage))
				return false
			}
		}
	}
	if normalized.schema && !temporaryDDL && !state.transaction {
		state.schemaDirty = true
		if err := s.refreshSchemaAfterDDL(context.Background()); err != nil {
			s.sendSessionError(frontend, state.txStatus(), "55000", fmt.Sprintf("refresh schema registry after DDL: %v", err))
			if atomicDDL {
				session.UnlockFleetWrites()
			}
			return false
		}
		state.schemaDirty = false
	} else if normalized.schema && !temporaryDDL {
		state.schemaDirty = true
	}
	for _, notice := range notices {
		frontend.Send(notice)
	}
	for _, parameterStatus := range parameterStatuses {
		frontend.Send(parameterStatus)
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
	if !temporaryDDL {
		if routed {
			recordWriteParticipants(state, sql, []string{target})
		} else {
			recordWriteParticipants(state, sql, targets)
		}
	}
	state.applySuccessfulSQLState(sql)
	if state.schemaDirty && !state.transaction {
		if firstSQLKeyword(sql) == "ROLLBACK" || firstSQLKeyword(sql) == "ABORT" {
			state.schemaDirty = false
		} else if err := s.refreshSchemaAfterDDL(context.Background()); err != nil {
			s.sendSessionError(frontend, state.txStatus(), "55000", fmt.Sprintf("refresh schema registry after DDL: %v", err))
			session.UnlockFleetWrites()
			return false
		} else {
			state.schemaDirty = false
		}
	}
	if invalidatesPreparedStatements(sql) {
		s.backends.InvalidatePreparedStatements(targets)
	}
	state.commitSessionState(sessionPolicy)
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
	if state.schemaDirty {
		if err := s.refreshSchemaAfterDDL(context.Background()); err != nil {
			span.SetStatus(codes.Error, "schema registry refresh failed")
			s.sendSessionError(frontend, 'I', "55000", fmt.Sprintf("refresh schema registry after DDL: %v", err))
			state.transaction = false
			state.mutated = false
			state.target = ""
			clear(state.writeParticipants)
			session.UnlockFleetWrites()
			return false
		}
		state.schemaDirty = false
	}
	span.SetStatus(codes.Ok, "")
	state.finishTemporaryTransaction(true, false)
	state.transaction = false
	state.mutated = false
	state.target = ""
	clear(state.writeParticipants)
	session.UnlockFleetWrites()
	frontend.Send(&pgproto3.CommandComplete{CommandTag: []byte("COMMIT")})
	frontend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	return true
}

// beginAtomicFleetDDL opens one transaction on every target before the DDL is
// sent. If any BEGIN fails, every participant reached so far is rolled back.
func (s *Server) beginAtomicFleetDDL(ctx context.Context, session *backend.Session, burrows []string) *pgproto3.ErrorResponse {
	begun := make([]string, 0, len(burrows))
	for _, name := range burrows {
		if response := runTransactionCommandTraced(ctx, session, name, "BEGIN"); response != nil {
			s.rollbackAtomicFleetDDL(ctx, session, begun)
			s.backends.RecordOperation("fleet_ddl", "begin_failure")
			return response
		}
		begun = append(begun, name)
	}
	return nil
}

// rollbackAtomicFleetDDL is best-effort because the original PostgreSQL error
// is the deterministic application-visible failure. Cleanup failures remain
// operational evidence and force the affected Session connections closed.
func (s *Server) rollbackAtomicFleetDDL(ctx context.Context, session *backend.Session, burrows []string) {
	failed := false
	for _, name := range burrows {
		if response := runTransactionCommandTraced(ctx, session, name, "ROLLBACK"); response != nil {
			failed = true
			s.logger.Error("fleet-wide DDL rollback failed", "event", "fleet_ddl_rollback_failed", "component", "hamstergres-proxy", "burrow", name, "error_category", "rollback_failure", "error", response.Message)
		}
	}
	outcome := "rolled_back"
	if failed {
		outcome = "rollback_failure"
	}
	s.backends.RecordOperation("fleet_ddl", outcome)
}

func (s *Server) commitAtomicFleetDDL(ctx context.Context, session *backend.Session, burrows []string) *pgproto3.ErrorResponse {
	response := s.commitTwoPhase(ctx, session, burrows)
	if response != nil {
		s.backends.RecordOperation("fleet_ddl", "commit_failure")
		return response
	}
	s.backends.RecordOperation("fleet_ddl", "committed")
	return nil
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

func (s *Server) resolveRouteDecision(prepared *router.Prepared, prepareErr error, burrows []string) (routeDecision, error) {
	if prepareErr != nil {
		target := s.parserFallbackBurrow(burrows)
		return routeDecision{target: target, routed: target != ""}, nil
	}
	return s.routePortal(prepared, nil, burrows)
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

func (s *Server) parserFallbackBurrow(burrows []string) string {
	if len(burrows) == 0 {
		return ""
	}
	if primary := s.backends.PrimaryBurrow(); primary != "" {
		for _, burrow := range burrows {
			if burrow == primary {
				return primary
			}
		}
	}
	ordered := append([]string(nil), burrows...)
	sort.Strings(ordered)
	return ordered[0]
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
	if !state.transaction || (!requiresRoutedWrite(sql) && !requiresFleetWriteOrder(sql, len(targets))) {
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
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return
	}
	for _, raw := range tree.Stmts {
		if raw.Stmt == nil {
			continue
		}
		state.updateTransactionStatement(raw.Stmt.GetTransactionStmt())
	}
}

func (state *extendedState) updateTransactionStatement(transaction *pg_query.TransactionStmt) {
	if transaction == nil {
		return
	}
	switch transaction.Kind {
	case pg_query.TransactionStmtKind_TRANS_STMT_BEGIN, pg_query.TransactionStmtKind_TRANS_STMT_START:
		state.beginTemporaryTransaction()
		state.transaction = true
		state.transactionFailed = false
		state.target = ""
		state.mutated = false
		clear(state.writeParticipants)
	case pg_query.TransactionStmtKind_TRANS_STMT_SAVEPOINT:
		state.rememberTemporarySavepoint(transaction.SavepointName)
	case pg_query.TransactionStmtKind_TRANS_STMT_ROLLBACK_TO:
		state.restoreTemporarySavepoint(transaction.SavepointName)
		state.transaction = true
		state.transactionFailed = false
	case pg_query.TransactionStmtKind_TRANS_STMT_RELEASE:
		state.releaseTemporarySavepoint(transaction.SavepointName)
	case pg_query.TransactionStmtKind_TRANS_STMT_COMMIT,
		pg_query.TransactionStmtKind_TRANS_STMT_ROLLBACK:
		state.finishTemporaryTransaction(transaction.Kind == pg_query.TransactionStmtKind_TRANS_STMT_COMMIT, transaction.Chain)
		state.transaction = transaction.Chain
		state.transactionFailed = false
		state.target = ""
		state.mutated = false
		clear(state.writeParticipants)
	case pg_query.TransactionStmtKind_TRANS_STMT_PREPARE,
		pg_query.TransactionStmtKind_TRANS_STMT_COMMIT_PREPARED,
		pg_query.TransactionStmtKind_TRANS_STMT_ROLLBACK_PREPARED:
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
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return false
	}
	for _, raw := range tree.Stmts {
		if raw.Stmt != nil && isFleetDDLStatement(raw.Stmt) && !protobufContainsTemporaryRelation(raw.Stmt.ProtoReflect()) && !isServerLocalDDLStatement(raw.Stmt) {
			return true
		}
	}
	return false
}

// containsFleetDDL classifies top-level statements from PostgreSQL's AST.
// SELECT INTO is schema DDL despite beginning with SELECT. The cases below
// are data, transaction, session, or maintenance commands; other top-level
// utility statements change a shared catalog and require coordination.
func containsFleetDDL(sql string) bool {
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return false
	}
	for _, raw := range tree.Stmts {
		if raw.Stmt != nil && isFleetDDLStatement(raw.Stmt) {
			return true
		}
	}
	return false
}

func isFleetDDLStatement(statement *pg_query.Node) bool {
	if statement == nil {
		return false
	}
	switch node := statement.GetNode().(type) {
	case *pg_query.Node_SelectStmt:
		if node.SelectStmt.IntoClause != nil {
			return true
		}
	case *pg_query.Node_InsertStmt,
		*pg_query.Node_UpdateStmt,
		*pg_query.Node_DeleteStmt,
		*pg_query.Node_MergeStmt,
		*pg_query.Node_CopyStmt,
		*pg_query.Node_VariableSetStmt,
		*pg_query.Node_VariableShowStmt,
		*pg_query.Node_DeclareCursorStmt,
		*pg_query.Node_ClosePortalStmt,
		*pg_query.Node_FetchStmt,
		*pg_query.Node_NotifyStmt,
		*pg_query.Node_ListenStmt,
		*pg_query.Node_UnlistenStmt,
		*pg_query.Node_TransactionStmt,
		*pg_query.Node_VacuumStmt,
		*pg_query.Node_ExplainStmt,
		*pg_query.Node_CheckPointStmt,
		*pg_query.Node_DiscardStmt,
		*pg_query.Node_LockStmt,
		*pg_query.Node_ConstraintsSetStmt,
		*pg_query.Node_PrepareStmt,
		*pg_query.Node_ExecuteStmt,
		*pg_query.Node_DeallocateStmt,
		*pg_query.Node_CallStmt:
		// Not schema DDL.
	default:
		return true
	}
	return false
}

func isServerLocalDDLStatement(statement *pg_query.Node) bool {
	return statement != nil && (statement.GetCreatedbStmt() != nil || statement.GetDropdbStmt() != nil ||
		statement.GetCreateTableSpaceStmt() != nil || statement.GetDropTableSpaceStmt() != nil ||
		statement.GetAlterSystemStmt() != nil)
}

func isTemporaryDDL(sql string) bool {
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return false
	}
	for _, raw := range tree.Stmts {
		if raw.Stmt != nil && protobufContainsTemporaryRelation(raw.Stmt.ProtoReflect()) {
			return true
		}
	}
	return false
}

func isServerLocalDDL(sql string) bool {
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return false
	}
	for _, raw := range tree.Stmts {
		if raw.Stmt == nil {
			continue
		}
		if isServerLocalDDLStatement(raw.Stmt) {
			return true
		}
	}
	return false
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

type sessionStatePolicy struct {
	requiresBackend bool
	pin             bool
	destroy         bool
	discardPrepared bool
	resetAll        bool
	replayName      string
	replaySQL       string
}

func requiresSessionBackend(sql string) bool {
	return classifySessionState(sql).requiresBackend
}

func requiresSessionAffinity(sql string) bool {
	return classifySessionState(sql).pin
}

func classifySessionState(sql string) sessionStatePolicy {
	tree, err := pg_query.Parse(sql)
	if err != nil {
		switch firstSQLKeyword(sql) {
		case "SET", "RESET", "DISCARD", "LISTEN", "UNLISTEN", "PREPARE":
			return sessionStatePolicy{requiresBackend: true, pin: true, destroy: true}
		}
		return sessionStatePolicy{}
	}
	policies := make([]sessionStatePolicy, 0, len(tree.Stmts))
	for _, raw := range tree.Stmts {
		policies = append(policies, classifyRawSessionState(raw, strings.TrimSpace(sql)))
	}
	if len(policies) == 1 {
		return policies[0]
	}
	combined := sessionStatePolicy{}
	for _, policy := range policies {
		combined.requiresBackend = combined.requiresBackend || policy.requiresBackend
		combined.pin = combined.pin || policy.pin || policy.replayName != "" || policy.resetAll
		combined.destroy = combined.destroy || policy.destroy
		combined.discardPrepared = combined.discardPrepared || policy.discardPrepared
	}
	return combined
}

func classifyRawSessionState(raw *pg_query.RawStmt, replaySQL string) sessionStatePolicy {
	if raw == nil || raw.Stmt == nil {
		return sessionStatePolicy{}
	}
	statement := raw.Stmt
	if variable := statement.GetVariableSetStmt(); variable != nil {
		policy := sessionStatePolicy{requiresBackend: true}
		if variable.IsLocal {
			return policy
		}
		if variable.Kind == pg_query.VariableSetKind_VAR_RESET_ALL {
			policy.resetAll = true
			return policy
		}
		switch variable.Kind {
		case pg_query.VariableSetKind_VAR_SET_VALUE,
			pg_query.VariableSetKind_VAR_SET_DEFAULT,
			pg_query.VariableSetKind_VAR_SET_CURRENT,
			pg_query.VariableSetKind_VAR_RESET:
			if variable.Name != "" {
				policy.replayName = strings.ToLower(variable.Name)
				policy.replaySQL = replaySQL
				return policy
			}
		}
		policy.pin = true
		return policy
	}
	if discard := statement.GetDiscardStmt(); discard != nil {
		policy := sessionStatePolicy{requiresBackend: true}
		if discard.Target == pg_query.DiscardMode_DISCARD_ALL {
			policy.resetAll = true
		} else {
			policy.pin = true
		}
		return policy
	}
	if statement.GetPrepareStmt() != nil {
		return sessionStatePolicy{requiresBackend: true, pin: true, discardPrepared: true}
	}
	if update := statement.GetUpdateStmt(); update != nil && isPGSettingsRelation(update.Relation) {
		return sessionStatePolicy{requiresBackend: true, pin: true}
	}
	if statement.GetListenStmt() != nil || statement.GetUnlistenStmt() != nil || statement.GetDeclareCursorStmt() != nil {
		return sessionStatePolicy{requiresBackend: true, pin: true}
	}
	if statement.GetDoStmt() != nil {
		return sessionStatePolicy{requiresBackend: true, pin: true, destroy: true}
	}
	if statement.GetLoadStmt() != nil {
		return sessionStatePolicy{requiresBackend: true, pin: true, destroy: true}
	}
	if protobufContainsSessionState(statement.ProtoReflect()) {
		return sessionStatePolicy{requiresBackend: true, pin: true}
	}
	return sessionStatePolicy{}
}

func isPGSettingsRelation(relation *pg_query.RangeVar) bool {
	if relation == nil || !strings.EqualFold(relation.Relname, "pg_settings") {
		return false
	}
	return relation.Schemaname == "" || strings.EqualFold(relation.Schemaname, "pg_catalog")
}

func protobufContainsSessionState(message protoreflect.Message) bool {
	if !message.IsValid() {
		return false
	}
	switch value := message.Interface().(type) {
	case *pg_query.RangeVar:
		if value.Relpersistence == "t" {
			return true
		}
	case *pg_query.FuncCall:
		name := functionName(value)
		switch name {
		case "set_config", "pg_advisory_lock", "pg_advisory_lock_shared", "pg_try_advisory_lock", "pg_try_advisory_lock_shared":
			return true
		}
	}
	found := false
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		switch {
		case field.IsList() && field.Message() != nil:
			list := value.List()
			for index := 0; index < list.Len(); index++ {
				if protobufContainsSessionState(list.Get(index).Message()) {
					found = true
					return false
				}
			}
		case field.IsMap() && field.MapValue().Message() != nil:
			value.Map().Range(func(_ protoreflect.MapKey, mapValue protoreflect.Value) bool {
				found = protobufContainsSessionState(mapValue.Message())
				return !found
			})
		case field.Message() != nil:
			found = protobufContainsSessionState(value.Message())
		}
		return !found
	})
	return found
}

func protobufContainsTemporaryRelation(message protoreflect.Message) bool {
	if !message.IsValid() {
		return false
	}
	if value, ok := message.Interface().(*pg_query.RangeVar); ok && value.Relpersistence == "t" {
		return true
	}
	found := false
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		switch {
		case field.IsList() && field.Message() != nil:
			list := value.List()
			for index := 0; index < list.Len(); index++ {
				if protobufContainsTemporaryRelation(list.Get(index).Message()) {
					found = true
					return false
				}
			}
		case field.IsMap() && field.MapValue().Message() != nil:
			value.Map().Range(func(_ protoreflect.MapKey, mapValue protoreflect.Value) bool {
				found = protobufContainsTemporaryRelation(mapValue.Message())
				return !found
			})
		case field.Message() != nil:
			found = protobufContainsTemporaryRelation(value.Message())
		}
		return !found
	})
	return found
}

func (state *extendedState) classifyTemporaryDDL(sql string) (temporaryOnly, mixed bool) {
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return false, false
	}
	working := extendedState{
		// A simple-query batch is one implicit transaction, so ON COMMIT DROP
		// relations remain addressable by later statements in the same batch.
		transaction:           true,
		temporaryRelations:    cloneStringSet(state.temporaryRelations),
		temporaryOnCommitDrop: cloneStringSet(state.temporaryOnCommitDrop),
		temporaryIndexParents: cloneStringMap(state.temporaryIndexParents),
	}
	hasTemporary := false
	hasDurableFleetDDL := false
	for _, raw := range tree.Stmts {
		if raw.Stmt == nil {
			continue
		}
		if discard := raw.Stmt.GetDiscardStmt(); discard != nil && (discard.Target == pg_query.DiscardMode_DISCARD_TEMP || discard.Target == pg_query.DiscardMode_DISCARD_ALL) {
			hasTemporary = true
			working.rememberTemporaryStatement(raw.Stmt)
			continue
		}
		if drop := raw.Stmt.GetDropStmt(); drop != nil && dropTargetsTemporaryRelations(drop) {
			names := dropObjectNames(drop)
			if len(names) > 0 {
				for _, name := range names {
					if temporaryNameSetContains(working.temporaryRelations, name) {
						hasTemporary = true
					} else {
						hasDurableFleetDDL = true
					}
				}
				working.rememberTemporaryStatement(raw.Stmt)
				continue
			}
		}
		statementTemporary := protobufContainsTemporaryRelation(raw.Stmt.ProtoReflect()) || protobufReferencesRelation(raw.Stmt.ProtoReflect(), working.temporaryRelations)
		if statementTemporary {
			hasTemporary = true
		}
		if isFleetDDLStatement(raw.Stmt) && !statementTemporary {
			hasDurableFleetDDL = true
		}
		working.rememberTemporaryStatement(raw.Stmt)
	}
	return hasTemporary && !hasDurableFleetDDL, hasTemporary && hasDurableFleetDDL
}

func (state *extendedState) referencesTemporaryDDL(sql string) bool {
	temporaryOnly, _ := state.classifyTemporaryDDL(sql)
	return temporaryOnly
}

func (state *extendedState) validateAtomicFleetDDL(server *Server, sql string, targetCount int) (bool, error) {
	temporaryDDL, mixed := state.classifyTemporaryDDL(sql)
	if mixed {
		return false, fmt.Errorf("temporary and durable fleet DDL cannot be combined in one simple-query batch")
	}
	if temporaryDDL {
		return true, nil
	}
	return false, server.validateAtomicFleetDDL(sql, targetCount)
}

func (state *extendedState) rememberTemporaryDDL(sql string) {
	state.ensureTemporaryState()
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return
	}
	for _, raw := range tree.Stmts {
		state.rememberTemporaryStatement(raw.Stmt)
	}
}

func (state *extendedState) applySuccessfulSQLState(sql string) {
	state.ensureTemporaryState()
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return
	}
	for _, raw := range tree.Stmts {
		if raw.Stmt == nil {
			continue
		}
		if transaction := raw.Stmt.GetTransactionStmt(); transaction != nil {
			state.updateTransactionStatement(transaction)
			continue
		}
		state.rememberTemporaryStatement(raw.Stmt)
	}
}

func (state *extendedState) rememberTemporaryStatement(statement *pg_query.Node) {
	if statement == nil {
		return
	}
	if create := statement.GetCreateStmt(); create != nil && create.Relation != nil && create.Relation.Relpersistence == "t" {
		state.rememberTemporaryRelation(create.Relation, create.Oncommit)
	}
	if createAs := statement.GetCreateTableAsStmt(); createAs != nil && createAs.Into != nil && createAs.Into.Rel != nil && createAs.Into.Rel.Relpersistence == "t" {
		state.rememberTemporaryRelation(createAs.Into.Rel, createAs.Into.OnCommit)
	}
	if selectStatement := statement.GetSelectStmt(); selectStatement != nil && selectStatement.IntoClause != nil && selectStatement.IntoClause.Rel != nil && selectStatement.IntoClause.Rel.Relpersistence == "t" {
		state.rememberTemporaryRelation(selectStatement.IntoClause.Rel, selectStatement.IntoClause.OnCommit)
	}
	if index := statement.GetIndexStmt(); index != nil && index.Relation != nil && relationSetContains(state.temporaryRelations, index.Relation) && index.Idxname != "" {
		state.rememberTemporaryIndex(index)
	}
	if rename := statement.GetRenameStmt(); rename != nil {
		state.renameTemporaryRelation(rename)
	}
	if drop := statement.GetDropStmt(); drop != nil && dropTargetsTemporaryRelations(drop) {
		cascade := drop.RemoveType != pg_query.ObjectType_OBJECT_INDEX
		for _, name := range dropObjectNames(drop) {
			state.forgetTemporaryName(name, cascade)
		}
	}
	if discard := statement.GetDiscardStmt(); discard != nil && (discard.Target == pg_query.DiscardMode_DISCARD_TEMP || discard.Target == pg_query.DiscardMode_DISCARD_ALL) {
		clear(state.temporaryRelations)
		clear(state.temporaryOnCommitDrop)
		clear(state.temporaryIndexParents)
	}
}

func (state *extendedState) ensureTemporaryState() {
	if state.temporaryRelations == nil {
		state.temporaryRelations = make(map[string]struct{})
	}
	if state.temporaryOnCommitDrop == nil {
		state.temporaryOnCommitDrop = make(map[string]struct{})
	}
	if state.temporaryIndexParents == nil {
		state.temporaryIndexParents = make(map[string]string)
	}
}

func (state *extendedState) rememberTemporaryRelation(relation *pg_query.RangeVar, onCommit pg_query.OnCommitAction) {
	if relation == nil || relation.Relname == "" || onCommit == pg_query.OnCommitAction_ONCOMMIT_DROP && !state.transaction {
		return
	}
	keys := temporaryRelationKeys(relation)
	for _, key := range keys {
		state.temporaryRelations[key] = struct{}{}
		if onCommit == pg_query.OnCommitAction_ONCOMMIT_DROP {
			state.temporaryOnCommitDrop[key] = struct{}{}
		}
	}
}

func (state *extendedState) rememberTemporaryIndex(index *pg_query.IndexStmt) {
	name := temporaryObjectName{name: index.Idxname}
	if strings.EqualFold(index.Relation.Schemaname, "pg_temp") {
		name.schema = "pg_temp"
	}
	parent := temporaryRelationCanonicalName(index.Relation)
	for _, key := range temporaryNameKeys(name) {
		state.temporaryRelations[key] = struct{}{}
		state.temporaryIndexParents[key] = parent
	}
}

func (state *extendedState) renameTemporaryRelation(rename *pg_query.RenameStmt) {
	if rename.Relation == nil || rename.Newname == "" || !renameTargetsRelation(rename.RenameType) || !relationSetContains(state.temporaryRelations, rename.Relation) {
		return
	}
	oldKeys := temporaryRelationKeys(rename.Relation)
	newRelation := *rename.Relation
	newRelation.Relname = rename.Newname
	newKeys := temporaryRelationKeys(&newRelation)
	indexParent := ""
	for _, key := range oldKeys {
		if parent, ok := state.temporaryIndexParents[key]; ok {
			indexParent = parent
		}
		delete(state.temporaryRelations, key)
		delete(state.temporaryIndexParents, key)
	}
	if indexParent != "" {
		for _, key := range newKeys {
			state.temporaryRelations[key] = struct{}{}
			state.temporaryIndexParents[key] = indexParent
		}
		return
	}
	oldCanonical := temporaryRelationCanonicalName(rename.Relation)
	newCanonical := temporaryRelationCanonicalName(&newRelation)
	onCommitDrop := false
	for _, key := range oldKeys {
		if _, ok := state.temporaryOnCommitDrop[key]; ok {
			onCommitDrop = true
		}
		delete(state.temporaryOnCommitDrop, key)
	}
	for key, parent := range state.temporaryIndexParents {
		if parent == oldCanonical {
			state.temporaryIndexParents[key] = newCanonical
		}
	}
	for _, key := range newKeys {
		state.temporaryRelations[key] = struct{}{}
		if onCommitDrop {
			state.temporaryOnCommitDrop[key] = struct{}{}
		}
	}
}

func (state *extendedState) forgetTemporaryName(name temporaryObjectName, cascade bool) {
	canonical := temporaryNameCanonical(name)
	for _, key := range temporaryNameKeys(name) {
		delete(state.temporaryRelations, key)
		delete(state.temporaryOnCommitDrop, key)
		delete(state.temporaryIndexParents, key)
	}
	if cascade {
		for key, parent := range state.temporaryIndexParents {
			if parent == canonical {
				delete(state.temporaryRelations, key)
				delete(state.temporaryIndexParents, key)
			}
		}
	}
}

func relationSetContains(relations map[string]struct{}, relation *pg_query.RangeVar) bool {
	if relation == nil || relation.Relname == "" {
		return false
	}
	if relation.Schemaname == "" {
		_, ok := relations[relation.Relname]
		return ok
	}
	if strings.EqualFold(relation.Schemaname, "pg_temp") {
		if _, ok := relations["pg_temp."+relation.Relname]; ok {
			return true
		}
		_, ok := relations[relation.Relname]
		return ok
	}
	_, ok := relations[relation.Schemaname+"."+relation.Relname]
	return ok
}

func temporaryRelationKeys(relation *pg_query.RangeVar) []string {
	if relation == nil {
		return nil
	}
	return temporaryNameKeys(temporaryObjectName{schema: relation.Schemaname, name: relation.Relname})
}

func temporaryRelationCanonicalName(relation *pg_query.RangeVar) string {
	if relation == nil {
		return ""
	}
	return temporaryNameCanonical(temporaryObjectName{schema: relation.Schemaname, name: relation.Relname})
}

func protobufReferencesRelation(message protoreflect.Message, relations map[string]struct{}) bool {
	if !message.IsValid() {
		return false
	}
	if value, ok := message.Interface().(*pg_query.RangeVar); ok && relationSetContains(relations, value) {
		return true
	}
	found := false
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		switch {
		case field.IsList() && field.Message() != nil:
			list := value.List()
			for index := 0; index < list.Len(); index++ {
				if protobufReferencesRelation(list.Get(index).Message(), relations) {
					found = true
					return false
				}
			}
		case field.IsMap() && field.MapValue().Message() != nil:
			value.Map().Range(func(_ protoreflect.MapKey, mapValue protoreflect.Value) bool {
				found = protobufReferencesRelation(mapValue.Message(), relations)
				return !found
			})
		case field.Message() != nil:
			found = protobufReferencesRelation(value.Message(), relations)
		}
		return !found
	})
	return found
}

type temporaryObjectName struct {
	schema string
	name   string
}

func dropObjectNames(drop *pg_query.DropStmt) []temporaryObjectName {
	var names []temporaryObjectName
	for _, object := range drop.Objects {
		list := object.GetList()
		if list == nil || len(list.Items) == 0 {
			continue
		}
		parts := make([]string, 0, len(list.Items))
		for _, item := range list.Items {
			if value := item.GetString_(); value != nil {
				parts = append(parts, value.Sval)
			}
		}
		if len(parts) == 0 {
			continue
		}
		name := temporaryObjectName{name: parts[len(parts)-1]}
		if len(parts) > 1 {
			name.schema = parts[len(parts)-2]
		}
		names = append(names, name)
	}
	return names
}

func temporaryNameKeys(name temporaryObjectName) []string {
	if name.name == "" {
		return nil
	}
	if name.schema == "" || strings.EqualFold(name.schema, "pg_temp") {
		return []string{name.name, "pg_temp." + name.name}
	}
	return []string{name.schema + "." + name.name}
}

func temporaryNameCanonical(name temporaryObjectName) string {
	if name.schema == "" || strings.EqualFold(name.schema, "pg_temp") {
		return "pg_temp." + name.name
	}
	return name.schema + "." + name.name
}

func temporaryNameSetContains(relations map[string]struct{}, name temporaryObjectName) bool {
	for _, key := range temporaryNameKeys(name) {
		if _, ok := relations[key]; ok {
			return true
		}
	}
	return false
}

func dropTargetsTemporaryRelations(drop *pg_query.DropStmt) bool {
	if drop == nil {
		return false
	}
	switch drop.RemoveType {
	case pg_query.ObjectType_OBJECT_TABLE,
		pg_query.ObjectType_OBJECT_INDEX,
		pg_query.ObjectType_OBJECT_SEQUENCE,
		pg_query.ObjectType_OBJECT_VIEW,
		pg_query.ObjectType_OBJECT_MATVIEW,
		pg_query.ObjectType_OBJECT_FOREIGN_TABLE:
		return true
	default:
		return false
	}
}

func renameTargetsRelation(objectType pg_query.ObjectType) bool {
	switch objectType {
	case pg_query.ObjectType_OBJECT_TABLE,
		pg_query.ObjectType_OBJECT_INDEX,
		pg_query.ObjectType_OBJECT_SEQUENCE,
		pg_query.ObjectType_OBJECT_VIEW,
		pg_query.ObjectType_OBJECT_MATVIEW,
		pg_query.ObjectType_OBJECT_FOREIGN_TABLE:
		return true
	default:
		return false
	}
}

func cloneStringSet(source map[string]struct{}) map[string]struct{} {
	clone := make(map[string]struct{}, len(source))
	for key := range source {
		clone[key] = struct{}{}
	}
	return clone
}

func cloneStringMap(source map[string]string) map[string]string {
	clone := make(map[string]string, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}

func (state *extendedState) temporarySnapshot(name string) temporaryRelationSnapshot {
	state.ensureTemporaryState()
	return temporaryRelationSnapshot{
		name:         name,
		relations:    cloneStringSet(state.temporaryRelations),
		onCommitDrop: cloneStringSet(state.temporaryOnCommitDrop),
		indexParents: cloneStringMap(state.temporaryIndexParents),
	}
}

func (state *extendedState) beginTemporaryTransaction() {
	if len(state.temporarySnapshots) == 0 {
		state.temporarySnapshots = append(state.temporarySnapshots, state.temporarySnapshot(""))
	}
}

func (state *extendedState) rememberTemporarySavepoint(name string) {
	state.beginTemporaryTransaction()
	state.temporarySnapshots = append(state.temporarySnapshots, state.temporarySnapshot(name))
}

func (state *extendedState) restoreTemporarySavepoint(name string) {
	for index := len(state.temporarySnapshots) - 1; index >= 1; index-- {
		if state.temporarySnapshots[index].name != name {
			continue
		}
		state.restoreTemporarySnapshot(state.temporarySnapshots[index])
		state.temporarySnapshots = state.temporarySnapshots[:index+1]
		return
	}
}

func (state *extendedState) releaseTemporarySavepoint(name string) {
	for index := len(state.temporarySnapshots) - 1; index >= 1; index-- {
		if state.temporarySnapshots[index].name == name {
			state.temporarySnapshots = state.temporarySnapshots[:index]
			return
		}
	}
}

func (state *extendedState) restoreTemporarySnapshot(snapshot temporaryRelationSnapshot) {
	state.temporaryRelations = cloneStringSet(snapshot.relations)
	state.temporaryOnCommitDrop = cloneStringSet(snapshot.onCommitDrop)
	state.temporaryIndexParents = cloneStringMap(snapshot.indexParents)
}

func (state *extendedState) finishTemporaryTransaction(commit, chain bool) {
	state.ensureTemporaryState()
	if commit {
		state.applyTemporaryOnCommitDrop()
	} else if len(state.temporarySnapshots) > 0 {
		state.restoreTemporarySnapshot(state.temporarySnapshots[0])
	}
	state.temporarySnapshots = nil
	if chain {
		state.beginTemporaryTransaction()
	}
}

func (state *extendedState) applyTemporaryOnCommitDrop() {
	parents := make(map[string]struct{})
	for key := range state.temporaryOnCommitDrop {
		delete(state.temporaryRelations, key)
		delete(state.temporaryIndexParents, key)
		if strings.Contains(key, ".") {
			parents[key] = struct{}{}
		} else {
			parents["pg_temp."+key] = struct{}{}
		}
	}
	clear(state.temporaryOnCommitDrop)
	for key, parent := range state.temporaryIndexParents {
		if _, drop := parents[parent]; drop {
			delete(state.temporaryRelations, key)
			delete(state.temporaryIndexParents, key)
		}
	}
}

func functionName(call *pg_query.FuncCall) string {
	parts := make([]string, 0, len(call.Funcname))
	for _, node := range call.Funcname {
		if value := node.GetString_(); value != nil {
			parts = append(parts, value.Sval)
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return strings.ToLower(parts[len(parts)-1])
}

func (state *extendedState) beginSessionState(policy sessionStatePolicy) {
	state.sessionAffinity = state.sessionAffinity || policy.pin
	state.sessionDestroy = state.sessionDestroy || policy.destroy
	state.sessionDiscardPrepared = state.sessionDiscardPrepared || policy.discardPrepared
}

func (state *extendedState) classifySessionState(sql string) sessionStatePolicy {
	policy := classifySessionState(sql)
	if state.transaction && (policy.replayName != "" || policy.resetAll) {
		// PostgreSQL rolls transaction-scoped SET and RESET changes back with
		// the transaction. Keep that frontend on its current Tunnel instead of
		// guessing whether the eventual COMMIT, ROLLBACK, or savepoint retained
		// the change. DISCARD ALL can later make the session multiplexable again.
		policy.pin = true
		policy.replayName = ""
		policy.replaySQL = ""
		policy.resetAll = false
	}
	return policy
}

func (state *extendedState) commitSessionState(policy sessionStatePolicy) {
	if policy.resetAll {
		state.sessionSettings = nil
		// DISCARD ALL resets PostgreSQL-managed session state, but it cannot
		// undo process-local effects such as LOAD. Such a Tunnel stays marked
		// for destruction rather than returning to the pool.
		state.sessionAffinity = state.sessionDestroy
		state.sessionDiscardPrepared = false
		return
	}
	if policy.replayName == "" {
		return
	}
	settings := state.sessionSettings[:0]
	for _, setting := range state.sessionSettings {
		if setting.name != policy.replayName {
			settings = append(settings, setting)
		}
	}
	state.sessionSettings = append(settings, sessionSetting{name: policy.replayName, sql: policy.replaySQL})
}

func (state *extendedState) sessionReplaySQL() []string {
	replay := make([]string, 0, len(state.sessionSettings))
	for _, setting := range state.sessionSettings {
		replay = append(replay, setting.sql)
	}
	return replay
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
	parserFallback := err != nil
	if !parserFallback && firstSQLKeyword(sql) == "INSERT" {
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
	decision, err := s.resolveRouteDecision(routing, err, s.backends.ShardNames())
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
		if response := postgresErrorResponse(err); response != nil {
			s.sendBackendError(frontend, 'I', response)
		} else {
			s.sendError(frontend, "XX000", err.Error())
		}
		return
	}
	endTunnelSpans(tunnelSpans, nil)
	if normalized.schema {
		if err := s.refreshSchemaAfterDDL(context.Background()); err != nil {
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

func postgresErrorResponse(err error) *pgproto3.ErrorResponse {
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) {
		return nil
	}
	return &pgproto3.ErrorResponse{
		Severity:            postgresError.Severity,
		SeverityUnlocalized: postgresError.SeverityUnlocalized,
		Code:                postgresError.Code,
		Message:             postgresError.Message,
		Detail:              postgresError.Detail,
		Hint:                postgresError.Hint,
		Position:            postgresError.Position,
		InternalPosition:    postgresError.InternalPosition,
		InternalQuery:       postgresError.InternalQuery,
		Where:               postgresError.Where,
		SchemaName:          postgresError.SchemaName,
		TableName:           postgresError.TableName,
		ColumnName:          postgresError.ColumnName,
		DataTypeName:        postgresError.DataTypeName,
		ConstraintName:      postgresError.ConstraintName,
		File:                postgresError.File,
		Line:                postgresError.Line,
		Routine:             postgresError.Routine,
	}
}

func (s *Server) sendError(frontend *pgproto3.Backend, code, message string) {
	s.sendSessionError(frontend, 'I', code, message)
}

func (s *Server) sendSessionError(frontend *pgproto3.Backend, status byte, code, message string) {
	frontend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: code, Message: message})
	frontend.Send(&pgproto3.ReadyForQuery{TxStatus: status})
}

func (s *Server) sendBackendError(frontend *pgproto3.Backend, status byte, response *pgproto3.ErrorResponse) {
	frontend.Send(response)
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
