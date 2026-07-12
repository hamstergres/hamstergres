package proxy

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/jruszo/hamstergres/internal/backend"
	"github.com/jruszo/hamstergres/internal/ddl"
	"github.com/jruszo/hamstergres/internal/router"
	"github.com/jruszo/hamstergres/internal/schema"
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
	state := extendedState{statements: make(map[string]statementState), portals: make(map[string]portalState), participants: make(map[string]struct{})}
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
		created, err := s.backends.NewSession(context.Background())
		if err != nil {
			s.sendExtendedError(frontend, "08006", err.Error())
			state.failed = true
			return nil, false
		}
		session = created
		return session, true
	}

	for {
		message, err := frontend.Receive()
		if err != nil {
			return err
		}
		switch message := message.(type) {
		case *pgproto3.Parse:
			if active, ok := ensureSession(); ok && !state.failed {
				prepared, err := prepareStatement(message, s.backends.Schema())
				if err != nil {
					s.sendExtendedError(frontend, "42601", err.Error())
					state.failed = true
					break
				}
				if s.handleParse(frontend, active, prepared.message) {
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
				} else if session != nil || isTransactionControl(message.String) {
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
				if s.handleBind(frontend, active, bound) {
					state.portals[message.DestinationPortal] = portalState{
						sql:        statement.sql,
						parameters: parameters,
						schema:     statement.schema,
					}
				} else {
					state.failed = true
				}
			}
		case *pgproto3.Describe:
			if active, ok := ensureSession(); ok && !state.failed {
				generated := message.ObjectType == 'S' && state.statements[message.Name].generated
				if !s.handleDescribe(frontend, active, message, generated) {
					state.failed = true
				}
			}
		case *pgproto3.Execute:
			if active, ok := ensureSession(); ok && !state.failed {
				if !s.handleExecute(frontend, active, message, state.portals[message.Portal], &state) {
					state.failed = true
				}
			}
		case *pgproto3.Close:
			if active, ok := ensureSession(); ok && !state.failed {
				if s.handleClose(frontend, active, message) {
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
			} else if s.handleSync(frontend, session, &state) {
				state.failed = false
			}
		case *pgproto3.Flush:
			// Each message above is flushed to the Burrows and its response is
			// returned before the next frontend message is read, so Flush has no
			// additional observable work to do.
		case *pgproto3.CopyData:
			if !state.copyIn || session == nil {
				s.sendExtendedError(frontend, "08P01", "CopyData received outside COPY FROM STDIN")
				state.failed = true
			} else if err := session.SendCopy(message); err != nil {
				s.sendExtendedError(frontend, "08006", err.Error())
				state.failed = true
			}
		case *pgproto3.CopyDone, *pgproto3.CopyFail:
			if !state.copyIn || session == nil {
				s.sendExtendedError(frontend, "08P01", "COPY completion received outside COPY FROM STDIN")
				state.failed = true
			} else if !s.handleCopyCompletion(frontend, session, message, &state) {
				state.failed = true
			}
		case *pgproto3.Terminate:
			return nil
		default:
			s.sendError(frontend, "0A000", fmt.Sprintf("unsupported PostgreSQL frontend message %T", message))
		}
		if err := frontend.Flush(); err != nil {
			return err
		}
	}
}

type extendedState struct {
	statements   map[string]statementState
	portals      map[string]portalState
	failed       bool
	transaction  bool
	target       string
	schemaDirty  bool
	copyIn       bool
	participants map[string]struct{}
}

type statementState struct {
	sql       string
	generated bool
	schema    bool
	message   *pgproto3.Parse
}

func prepareStatement(message *pgproto3.Parse, registry schema.Registry) (statementState, error) {
	normalized, err := normalizeDDL(message.Query)
	if err != nil {
		return statementState{}, err
	}
	parameter := maxParameter(normalized.sql) + 1
	rewritten, generated := router.RewriteGeneratedInsert(normalized.sql, registry, fmt.Sprintf("$%d", parameter))
	if !generated {
		if normalized.sql == message.Query {
			return statementState{sql: message.Query, schema: normalized.schema, message: message}, nil
		}
		prepared := *message
		prepared.Query = normalized.sql
		return statementState{sql: normalized.sql, schema: normalized.schema, message: &prepared}, nil
	}
	oids := append([]uint32(nil), message.ParameterOIDs...)
	for len(oids) < parameter {
		oids = append(oids, 0)
	}
	prepared := *message
	prepared.Query = rewritten.SQL
	prepared.ParameterOIDs = oids
	return statementState{sql: rewritten.SQL, generated: true, schema: normalized.schema, message: &prepared}, nil
}

type normalizedSQL struct {
	sql    string
	schema bool
}

func normalizeDDL(sql string) (normalizedSQL, error) {
	keyword := firstSQLKeyword(sql)
	if keyword != "CREATE" && keyword != "ALTER" {
		return normalizedSQL{sql: sql}, nil
	}
	result, err := ddl.Normalize(sql)
	if err != nil {
		return normalizedSQL{}, err
	}
	return normalizedSQL{sql: result.SQL, schema: result.Schema}, nil
}

var parameterReference = regexp.MustCompile(`\$(\d+)`)

func maxParameter(sql string) int {
	maximum := 0
	for _, match := range parameterReference.FindAllStringSubmatch(sql, -1) {
		value, _ := strconv.Atoi(match[1])
		if value > maximum {
			maximum = value
		}
	}
	return maximum
}

func (s *Server) prepareBind(message *pgproto3.Bind, statement statementState) (*pgproto3.Bind, [][]byte, error) {
	if !statement.generated {
		return message, cloneParameters(message.Parameters), nil
	}
	id, err := s.backends.NextGlobalID(context.Background())
	if err != nil {
		return nil, nil, fmt.Errorf("allocate globally unique primary key: %w", err)
	}
	bound := *message
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
	return &bound, cloneParameters(bound.Parameters), nil
}

func (s extendedState) txStatus() byte {
	if s.transaction {
		return 'T'
	}
	return 'I'
}

type portalState struct {
	sql        string
	parameters [][]byte
	schema     bool
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

func (s *Server) handleParse(frontend *pgproto3.Backend, session *backend.Session, message *pgproto3.Parse) bool {
	responses, err := exchange(session, message, isParseDone)
	return s.relayUniform(frontend, responses, err, "Parse")
}

func (s *Server) handleBind(frontend *pgproto3.Backend, session *backend.Session, message *pgproto3.Bind) bool {
	responses, err := exchange(session, message, isBindDone)
	return s.relayUniform(frontend, responses, err, "Bind")
}

func (s *Server) handleDescribe(frontend *pgproto3.Backend, session *backend.Session, message *pgproto3.Describe, generated bool) bool {
	responses, err := exchange(session, message, isDescribeDone)
	if generated {
		for _, response := range responses {
			for _, wireMessage := range response {
				if parameters, ok := wireMessage.(*pgproto3.ParameterDescription); ok && len(parameters.ParameterOIDs) > 0 {
					parameters.ParameterOIDs = parameters.ParameterOIDs[:len(parameters.ParameterOIDs)-1]
				}
			}
		}
	}
	return s.relayUniform(frontend, responses, err, "Describe")
}

func (s *Server) handleClose(frontend *pgproto3.Backend, session *backend.Session, message *pgproto3.Close) bool {
	responses, err := exchange(session, message, isCloseDone)
	return s.relayUniform(frontend, responses, err, "Close")
}

func (s *Server) handleCopyQuery(frontend *pgproto3.Backend, session *backend.Session, sql string, state *extendedState) bool {
	success := false
	defer func() {
		outcome := "failure"
		if success {
			outcome = "success"
		}
		s.backends.RecordOperation("copy", outcome)
		if !success {
			s.logger.Error("COPY operation failed", "event", "copy_failed", "component", "hamstergres-proxy", "error_category", "copy")
		}
	}()
	responses, err := exchange(session, &pgproto3.Query{String: sql}, isCopyStarted)
	if err != nil {
		s.sendExtendedError(frontend, "08006", err.Error())
		return false
	}
	if response := firstError(responses); response != nil {
		frontend.Send(response)
		frontend.Send(&pgproto3.ReadyForQuery{TxStatus: state.txStatus()})
		return false
	}
	if len(responses) == 0 || len(responses[0]) == 0 {
		s.sendExtendedError(frontend, "XX000", "no Burrow COPY response")
		return false
	}
	first := responses[0][len(responses[0])-1]
	for _, response := range responses[1:] {
		if len(response) == 0 || reflect.TypeOf(response[len(response)-1]) != reflect.TypeOf(first) {
			s.sendExtendedError(frontend, "XX000", "incompatible COPY responses from Burrows")
			return false
		}
	}
	switch response := first.(type) {
	case *pgproto3.CopyInResponse:
		frontend.Send(response)
		state.copyIn = true
		s.markAllParticipants(state)
		success = true
		return true
	case *pgproto3.CopyOutResponse:
		frontend.Send(response)
		s.markAllParticipants(state)
		success = s.handleCopyOut(frontend, session, state)
		return success
	case *pgproto3.CopyBothResponse:
		s.sendExtendedError(frontend, "0A000", "COPY BOTH is not supported by Hamstergres Proxy")
		return false
	default:
		s.sendExtendedError(frontend, "08P01", fmt.Sprintf("unexpected COPY response %T", first))
		return false
	}
}

func (s *Server) markAllParticipants(state *extendedState) {
	if !state.transaction {
		return
	}
	for _, name := range s.backends.ShardNames() {
		state.participants[name] = struct{}{}
	}
}

func (s *Server) handleCopyOut(frontend *pgproto3.Backend, session *backend.Session, state *extendedState) bool {
	responses, err := session.ReceiveUntil(context.Background(), isQueryDone)
	if err != nil {
		s.sendExtendedError(frontend, "08006", err.Error())
		return false
	}
	if response := firstError(responses); response != nil {
		frontend.Send(response)
		frontend.Send(&pgproto3.ReadyForQuery{TxStatus: state.txStatus()})
		return false
	}
	var tags []string
	for _, response := range responses {
		for _, message := range response {
			switch message := message.(type) {
			case *pgproto3.CopyData:
				frontend.Send(message)
			case *pgproto3.CommandComplete:
				tags = append(tags, string(message.CommandTag))
			case *pgproto3.CopyDone, *pgproto3.ReadyForQuery, *pgproto3.NoticeResponse:
			default:
				s.sendExtendedError(frontend, "08P01", fmt.Sprintf("unexpected COPY TO response %T", message))
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
	state.copyIn = false
	if err := session.SendCopy(message); err != nil {
		s.sendExtendedError(frontend, "08006", err.Error())
		return false
	}
	responses, err := session.ReceiveUntil(context.Background(), isQueryDone)
	if err != nil {
		s.sendExtendedError(frontend, "08006", err.Error())
		return false
	}
	if response := firstError(responses); response != nil {
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
		frontend.Send(&pgproto3.CommandComplete{CommandTag: []byte(mergedCommandTag(tags, 0))})
	}
	frontend.Send(&pgproto3.ReadyForQuery{TxStatus: state.txStatus()})
	return true
}

func (s *Server) handleSync(frontend *pgproto3.Backend, session *backend.Session, state *extendedState) bool {
	responses, err := exchange(session, &pgproto3.Sync{}, isSyncDone)
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
		session.UnlockWrites()
	}
	return true
}

// handleExecute merges rows from the fan-out execution. Data values are
// already encoded by PostgreSQL, so text and binary result formats pass through
// unchanged.
func (s *Server) handleExecute(frontend *pgproto3.Backend, session *backend.Session, message *pgproto3.Execute, portal portalState, state *extendedState) bool {
	started := time.Now()
	success := false
	correlationID := fmt.Sprintf("query-%08x", randomUint32())
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
			s.logger.Error("frontend query failed", "event", "query_failed", "component", "hamstergres-proxy", "correlation_id", correlationID, "error_category", "query_execution")
		}
	}()
	targets := s.backends.ShardNames()
	defer func() {
		if portal.sql != "" {
			s.backends.RecordQueryTargets(portal.sql, success, time.Since(started), targets)
		}
	}()
	if state.transaction && (firstSQLKeyword(portal.sql) == "COMMIT" || firstSQLKeyword(portal.sql) == "END") {
		if response := s.commitTwoPhase(session); response != nil {
			frontend.Send(response)
			return false
		}
		frontend.Send(&pgproto3.CommandComplete{CommandTag: []byte("COMMIT")})
		state.transaction = false
		clear(state.participants)
		success = true
		return true
	}

	// The initial extended-query contract is deliberately fan-out. Keeping
	// every affinity connection in the same Parse/Bind/Execute/Sync lifecycle
	// also lets a transaction use multiple shard keys, as sysbench does.
	tunnelSpans := startTunnelSpans(traceContext, targets)
	responses, err := exchange(session, message, isExecuteDone)
	if err != nil {
		endTunnelSpans(tunnelSpans, err)
		s.sendExtendedError(frontend, "08006", err.Error())
		return false
	}
	if response := firstError(responses); response != nil {
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
	if portal.schema {
		state.schemaDirty = true
	}
	updateTransactionState(state, portal.sql)
	return true
}

func exchange(session *backend.Session, message pgproto3.FrontendMessage, done func(pgproto3.BackendMessage) bool) ([][]pgproto3.BackendMessage, error) {
	if err := session.Send(message); err != nil {
		return nil, err
	}
	return session.ReceiveUntil(context.Background(), done)
}

func exchangeOne(session *backend.Session, target string, message pgproto3.FrontendMessage, done func(pgproto3.BackendMessage) bool) ([][]pgproto3.BackendMessage, error) {
	if err := session.SendTo(target, message); err != nil {
		return nil, err
	}
	return session.ReceiveUntilFrom(context.Background(), target, done)
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
	case *pgproto3.CopyInResponse, *pgproto3.CopyOutResponse, *pgproto3.CopyBothResponse, *pgproto3.ErrorResponse, *pgproto3.ReadyForQuery:
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
	if state.transaction && (firstSQLKeyword(sql) == "COMMIT" || firstSQLKeyword(sql) == "END") && len(state.participants) > 1 {
		return s.handleTwoPhaseCommit(frontend, session, state)
	}
	normalized, err := normalizeDDL(sql)
	if err != nil {
		s.sendSessionError(frontend, state.txStatus(), "42601", err.Error())
		return false
	}
	sql = normalized.sql
	var generationErr error
	if rewritten, generated := router.RewriteGeneratedInsert(sql, s.backends.Schema(), "0"); generated {
		id, err := s.backends.NextGlobalID(context.Background())
		if err != nil {
			generationErr = err
		} else {
			rewritten, _ = router.RewriteGeneratedInsert(sql, s.backends.Schema(), strconv.FormatInt(id, 10))
			sql = rewritten.SQL
		}
	}
	if generationErr != nil {
		s.sendSessionError(frontend, state.txStatus(), "55000", fmt.Sprintf("allocate globally unique primary key: %v", generationErr))
		return false
	}

	started := time.Now()
	success := false
	correlationID := fmt.Sprintf("query-%08x", randomUint32())
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
			s.logger.Error("frontend query failed", "event", "query_failed", "component", "hamstergres-proxy", "correlation_id", correlationID, "error_category", "query_execution")
		}
	}()
	targets := s.backends.ShardNames()
	defer func() {
		s.backends.RecordQueryTargets(sql, success, time.Since(started), targets)
	}()

	var responses [][]pgproto3.BackendMessage
	var tunnelSpans []trace.Span
	target, routed := s.routeSQL(sql, targets)
	if requiresRoutedWrite(sql) && !routed {
		s.sendSessionError(frontend, state.txStatus(), "0A000", "write must include a single id or tenant_id shard key")
		return false
	}
	if routed && !isTransactionControl(sql) {
		targets = []string{target}
		querySpan.SetAttributes(attribute.String("hamstergres.route", "single_burrow"))
		tunnelSpans = startTunnelSpans(traceContext, targets)
		responses, err = exchangeOne(session, target, &pgproto3.Query{String: sql}, isQueryDone)
	} else {
		querySpan.SetAttributes(attribute.String("hamstergres.route", "scatter"))
		tunnelSpans = startTunnelSpans(traceContext, targets)
		responses, err = exchange(session, &pgproto3.Query{String: sql}, isQueryDone)
	}
	if err != nil {
		endTunnelSpans(tunnelSpans, err)
		s.sendError(frontend, "08006", err.Error())
		return false
	}
	status := readyTxStatus(responses)
	if response := firstError(responses); response != nil {
		endTunnelSpans(tunnelSpans, fmt.Errorf("PostgreSQL error %s", response.Code))
		frontend.Send(response)
		frontend.Send(&pgproto3.ReadyForQuery{TxStatus: status})
		if status == 'I' {
			session.UnlockWrites()
		}
		return false
	}
	endTunnelSpans(tunnelSpans, nil)

	var description *pgproto3.RowDescription
	var rows []*pgproto3.DataRow
	var tags []string
	for _, response := range responses {
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
			case *pgproto3.EmptyQueryResponse, *pgproto3.ReadyForQuery, *pgproto3.NoticeResponse, *pgproto3.ParameterStatus, *pgproto3.NotificationResponse:
				// ReadyForQuery is merged after result data. Notices and notifications are Burrow-local.
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
	if state.transaction && !isTransactionControl(sql) {
		if routed {
			state.participants[target] = struct{}{}
		} else {
			for _, name := range targets {
				state.participants[name] = struct{}{}
			}
		}
	}
	updateTransactionState(state, sql)
	frontend.Send(&pgproto3.ReadyForQuery{TxStatus: state.txStatus()})
	if !state.transaction {
		session.UnlockWrites()
	}
	success = true
	return true
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
	if response := s.commitTwoPhase(session); response != nil {
		frontend.Send(response)
		frontend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		state.transaction = false
		clear(state.participants)
		session.UnlockWrites()
		return false
	}
	state.transaction = false
	state.target = ""
	clear(state.participants)
	session.UnlockWrites()
	frontend.Send(&pgproto3.CommandComplete{CommandTag: []byte("COMMIT")})
	frontend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	return true
}

func (s *Server) commitTwoPhase(session *backend.Session) *pgproto3.ErrorResponse {
	gid := fmt.Sprintf("hamstergres-%08x-%08x", randomUint32(), randomUint32())
	burrows := s.backends.ShardNames()
	prepared := make([]string, 0, len(burrows))
	for _, name := range burrows {
		if response := runTransactionCommand(session, name, "PREPARE TRANSACTION '"+gid+"'"); response != nil {
			s.backends.RecordOperation("two_phase_commit", "prepare_failure")
			for _, preparedName := range prepared {
				_ = runTransactionCommand(session, preparedName, "ROLLBACK PREPARED '"+gid+"'")
			}
			for _, rollbackName := range burrows[len(prepared):] {
				_ = runTransactionCommand(session, rollbackName, "ROLLBACK")
			}
			return response
		}
		prepared = append(prepared, name)
	}
	for _, name := range prepared {
		if response := runTransactionCommand(session, name, "COMMIT PREPARED '"+gid+"'"); response != nil {
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

func runTransactionCommand(session *backend.Session, burrow, sql string) *pgproto3.ErrorResponse {
	responses, err := exchangeOne(session, burrow, &pgproto3.Query{String: sql}, isQueryDone)
	if err != nil {
		return &pgproto3.ErrorResponse{Severity: "ERROR", Code: "08006", Message: err.Error()}
	}
	return firstError(responses)
}

func (s *Server) routeSQL(sql string, burrows []string) (string, bool) {
	return router.TargetForSchema(sql, nil, s.backends.Schema(), burrows)
}

func (s *Server) routePortal(sql string, parameters [][]byte, burrows []string) (string, bool) {
	return router.TargetForSchema(sql, parameters, s.backends.Schema(), burrows)
}

func requiresRoutedWrite(sql string) bool {
	switch firstSQLKeyword(sql) {
	case "INSERT", "UPDATE", "DELETE", "MERGE":
		return true
	default:
		return false
	}
}

func updateTransactionState(state *extendedState, sql string) {
	switch firstSQLKeyword(sql) {
	case "BEGIN", "START":
		state.transaction = true
		state.target = ""
		clear(state.participants)
	case "COMMIT", "END", "ROLLBACK", "ABORT":
		state.transaction = false
		state.target = ""
		clear(state.participants)
	}
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

func requiresGlobalWriteOrder(sql string) bool {
	switch firstSQLKeyword(sql) {
	case "INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "ALTER", "DROP", "TRUNCATE":
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

func (s *Server) handleQuery(frontend *pgproto3.Backend, sql string) {
	if strings.TrimSpace(sql) == "" {
		frontend.Send(&pgproto3.EmptyQueryResponse{})
		frontend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		return
	}
	normalized, err := normalizeDDL(sql)
	if err != nil {
		s.sendError(frontend, "42601", err.Error())
		return
	}
	sql = normalized.sql
	if _, generated := router.RewriteGeneratedInsert(sql, s.backends.Schema(), "0"); generated {
		id, err := s.backends.NextGlobalID(context.Background())
		if err != nil {
			s.sendError(frontend, "55000", fmt.Sprintf("allocate globally unique primary key: %v", err))
			return
		}
		rewritten, _ := router.RewriteGeneratedInsert(sql, s.backends.Schema(), strconv.FormatInt(id, 10))
		sql = rewritten.SQL
	}
	if requiresGlobalWriteOrder(sql) && !requiresRoutedWrite(sql) {
		unlock := s.backends.LockWrites()
		defer unlock()
	}
	var result backend.Result
	target, routed := s.routeSQL(sql, s.backends.ShardNames())
	if requiresRoutedWrite(sql) && !routed {
		s.sendError(frontend, "0A000", "write must include a single id or tenant_id shard key")
		return
	}
	if routed {
		result, err = s.backends.QueryOne(context.Background(), sql, target)
	} else {
		result, err = s.backends.QueryAll(context.Background(), sql)
	}
	if err != nil {
		s.sendError(frontend, "XX000", err.Error())
		return
	}
	if normalized.schema {
		if err := s.backends.RefreshSchema(context.Background()); err != nil {
			s.sendError(frontend, "55000", fmt.Sprintf("refresh schema registry after DDL: %v", err))
			return
		}
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
