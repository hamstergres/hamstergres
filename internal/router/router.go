// SPDX-License-Identifier: AGPL-3.0-only

package router

import (
	"fmt"
	"hash/fnv"
	"math/big"
	"strconv"
	"strings"

	"github.com/jruszo/hamstergres/internal/schema"
	pg_query "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const VirtualShards = 65536

// Plan is the parser-derived routing information shared by simple and
// extended-query execution. Routed is true only when the AST proves a complete,
// constant shard-key tuple for one physical relation.
type Plan struct {
	Table         string
	Write         bool
	Sharded       bool
	Target        string
	Routed        bool
	SingleBurrow  bool
	Deterministic bool
	ScatterError  string
}

// Prepared is the immutable, parser-derived portion of a routing plan. Extended
// protocol clients parse a statement once and bind it many times, so retaining
// this structure avoids repeating PostgreSQL parsing and protobuf allocation on
// every Bind while still resolving each execution against the current schema
// and vshard registry.
type Prepared struct {
	plan              Plan
	relation          *pg_query.RangeVar
	predicate         *pg_query.Node
	insert            *pg_query.InsertStmt
	update            *pg_query.UpdateStmt
	physicalRelations int
	maximumParameter  int
	read              bool
	relations         []string
	scatterError      string
}

// Prepare parses exactly one PostgreSQL statement and caches its immutable
// routing structure. Unsupported or ambiguous syntax remains a valid prepared
// statement with an unrouted plan; parser errors are returned to the caller.
func Prepare(sql string) (*Prepared, error) {
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return nil, err
	}
	prepared := &Prepared{plan: Plan{Write: containsWrite(tree)}, maximumParameter: maxParameterInTree(tree)}
	if len(tree.Stmts) != 1 || tree.Stmts[0].Stmt == nil {
		return prepared, nil
	}

	statement := tree.Stmts[0].Stmt
	switch value := statement.GetNode().(type) {
	case *pg_query.Node_InsertStmt:
		prepared.plan.Write = true
		prepared.insert = value.InsertStmt
		prepared.relation = prepared.insert.Relation
	case *pg_query.Node_UpdateStmt:
		prepared.plan.Write = true
		prepared.update = value.UpdateStmt
		prepared.relation = prepared.update.Relation
		prepared.predicate = prepared.update.WhereClause
		if prepared.update.WithClause != nil || len(prepared.update.FromClause) != 0 {
			prepared.predicate = nil
		}
	case *pg_query.Node_DeleteStmt:
		prepared.plan.Write = true
		prepared.relation = value.DeleteStmt.Relation
		prepared.predicate = value.DeleteStmt.WhereClause
		if value.DeleteStmt.WithClause != nil || len(value.DeleteStmt.UsingClause) != 0 {
			prepared.predicate = nil
		}
	case *pg_query.Node_CopyStmt:
		// COPY data-phase routing is completed by the Proxy. Recording the
		// relation and direction here lets it apply the same unsharded-table
		// policy as ordinary queries without guessing from SQL text.
		prepared.plan.Write = value.CopyStmt.IsFrom
		prepared.relation = value.CopyStmt.Relation
	case *pg_query.Node_MergeStmt:
		// MERGE can contain multiple conditional write actions. Resolve its
		// target relation for policy purposes, but keep sharded MERGE unrouted
		// until every action can be proven to share one shard-key tuple.
		prepared.plan.Write = true
		prepared.relation = value.MergeStmt.Relation
	case *pg_query.Node_SelectStmt:
		selectStatement := value.SelectStmt
		prepared.read = selectStatement.IntoClause == nil
		if selectStatement.WithClause == nil && selectStatement.Op == pg_query.SetOperation_SETOP_NONE && len(selectStatement.FromClause) == 1 {
			prepared.relation = selectStatement.FromClause[0].GetRangeVar()
			prepared.predicate = selectStatement.WhereClause
		}
	case *pg_query.Node_VariableShowStmt:
		prepared.read = true
	default:
	}

	prepared.relations = physicalRelationNames(statement)
	prepared.physicalRelations = len(prepared.relations)
	if selectStatement := statement.GetSelectStmt(); selectStatement != nil {
		prepared.scatterError = unsafeScatterReason(selectStatement, prepared.physicalRelations)
	}
	if prepared.relation == nil {
		return prepared, nil
	}
	prepared.plan.Table = relationName(prepared.relation)
	return prepared, nil
}

// MaxParameter returns the highest real ParamRef found while preparing the
// statement. Dollar sequences inside comments and string literals are ignored.
func (p *Prepared) MaxParameter() int {
	if p == nil {
		return 0
	}
	return p.maximumParameter
}

// Table returns the parser-resolved primary physical relation. It lets callers
// consult schema metadata without reparsing the statement.
func (p *Prepared) Table() string {
	if p == nil {
		return ""
	}
	return p.plan.Table
}

// Analyze resolves one execution of a prepared statement against bound values
// and the current registry. The cached AST is read-only; schema and vshard
// ownership changes therefore remain visible without reparsing SQL.
func (p *Prepared) Analyze(parameters [][]byte, registry schema.Registry, burrows []string) (plan Plan) {
	if p == nil {
		return Plan{}
	}
	plan = p.plan
	containsShardedRelation := false
	if p.read {
		for _, table := range p.relations {
			if registry.IsSharded(table) || registry.IsSharded(registry.CanonicalTable(table)) {
				containsShardedRelation = true
				break
			}
		}
		if !containsShardedRelation {
			plan.SingleBurrow = true
			plan.Deterministic = true
			for _, table := range p.relations {
				if registry.HasTable(table) {
					plan.Deterministic = false
					break
				}
			}
			return plan
		}
		defer func() {
			if !plan.Routed && p.scatterError != "" {
				plan.ScatterError = p.scatterError
			}
		}()
	}
	relation, insert, update := p.relation, p.insert, p.update
	if relation == nil {
		return plan
	}
	columns, sharded := registry.ShardKey(plan.Table)
	types, _ := registry.ShardKeyTypes(plan.Table)
	plan.Sharded = sharded
	if !sharded || len(columns) == 0 || len(burrows) == 0 {
		return plan
	}
	if p.physicalRelations != 1 {
		return plan
	}
	if insert != nil && insert.OnConflictClause != nil {
		return plan
	}
	if update != nil && updatesShardKey(update, columns) {
		return plan
	}

	var values []string
	var ok bool
	if insert != nil {
		values, ok = insertKeyValues(insert, parameters, columns, types)
	} else {
		values, ok = predicateKeyValues(p.predicate, parameters, columns, types, relation)
	}
	if !ok {
		return plan
	}

	plan.Target, plan.Routed = TargetForTableShardKey(plan.Table, values, types, registry, burrows)
	return plan
}

// TargetForShardKey canonicalizes a complete shard-key tuple exactly as SQL
// routing does and resolves its vshard owner. COPY uses this entry point after
// decoding text, CSV, or binary fields from one input row.
func TargetForShardKey(values, types []string, registry schema.Registry, burrows []string) (string, bool) {
	return TargetForTableShardKey("", values, types, registry, burrows)
}

// TargetForTableShardKey resolves the table-specific distribution selected by
// the versioned Nest topology catalog.
func TargetForTableShardKey(table string, values, types []string, registry schema.Registry, burrows []string) (string, bool) {
	if len(values) == 0 || len(types) != 0 && len(values) != len(types) || len(burrows) == 0 {
		return "", false
	}
	canonical := make([]string, len(values))
	for index, value := range values {
		dataType := ""
		if len(types) == len(values) {
			dataType = types[index]
		}
		var ok bool
		canonical[index], ok = canonicalScalar(value, dataType)
		if !ok {
			return "", false
		}
	}
	key := strings.Join(canonical, "\x00")
	vshard := int(HashKey(key) % VirtualShards)
	if registry.VShardCountFor(table) == VirtualShards {
		return registry.VShardOwnerFor(table, vshard)
	}
	if registry.HasTopologyPlacement() {
		return "", false
	}
	return BurrowForKey(key, burrows), true
}

// Analyze parses exactly one PostgreSQL statement and derives its routing plan.
// Unsupported or ambiguous syntax produces an unrouted plan, never a
// permissive single-Burrow guess.
func Analyze(sql string, parameters [][]byte, registry schema.Registry, burrows []string) (Plan, error) {
	prepared, err := Prepare(sql)
	if err != nil {
		return Plan{}, err
	}
	return prepared.Analyze(parameters, registry, burrows), nil
}

// TableForSQL returns the AST-resolved primary physical relation for supported
// DML. Complex reads deliberately return false rather than guessing.
func TableForSQL(sql string) (string, bool) {
	plan, err := Analyze(sql, nil, schema.Registry{}, nil)
	return plan.Table, err == nil && plan.Table != ""
}

// GeneratedInsert is a rewritten single-row INSERT whose generated primary
// key is now explicit, allowing the Proxy to route before contacting a Burrow.
type GeneratedInsert struct {
	SQL    string
	Table  string
	Column string
}

// RewriteGeneratedInsert injects valueExpression when an eligible generated
// primary key is omitted or specified as DEFAULT. Both inspection and rewriting
// operate on PostgreSQL's AST, so expressions containing commas, comments, or
// casts cannot corrupt column/value alignment.
func RewriteGeneratedInsert(sql string, registry schema.Registry, valueExpression string) (GeneratedInsert, bool) {
	tree, err := pg_query.Parse(sql)
	if err != nil || len(tree.Stmts) != 1 || tree.Stmts[0].Stmt == nil {
		return GeneratedInsert{}, false
	}
	insert := tree.Stmts[0].Stmt.GetInsertStmt()
	if insert == nil || insert.Relation == nil || insert.WithClause != nil {
		return GeneratedInsert{}, false
	}
	table := relationName(insert.Relation)
	generated, ok := registry.GeneratedPrimaryKey(table)
	if !ok {
		return GeneratedInsert{}, false
	}
	valuesStatement := insert.SelectStmt.GetSelectStmt()
	if valuesStatement == nil || len(valuesStatement.ValuesLists) != 1 {
		return GeneratedInsert{}, false
	}
	row := valuesStatement.ValuesLists[0].GetList()
	if row == nil || len(row.Items) != len(insert.Cols) {
		return GeneratedInsert{}, false
	}
	replacement, err := parseExpression(valueExpression)
	if err != nil {
		return GeneratedInsert{}, false
	}

	for index, node := range insert.Cols {
		column := node.GetResTarget()
		if column == nil || column.Name != generated.Column {
			continue
		}
		if row.Items[index].GetSetToDefault() == nil {
			return GeneratedInsert{}, false
		}
		row.Items[index] = replacement
		rewritten, err := pg_query.Deparse(tree)
		if err != nil {
			return GeneratedInsert{}, false
		}
		return GeneratedInsert{SQL: rewritten, Table: table, Column: generated.Column}, true
	}

	insert.Cols = append(insert.Cols, pg_query.MakeResTargetNodeWithName(generated.Column, -1))
	row.Items = append(row.Items, replacement)
	rewritten, err := pg_query.Deparse(tree)
	if err != nil {
		return GeneratedInsert{}, false
	}
	return GeneratedInsert{SQL: rewritten, Table: table, Column: generated.Column}, true
}

// MaxParameter returns the highest real ParamRef in the PostgreSQL AST. Dollar
// sequences inside comments and string literals are therefore ignored.
func MaxParameter(sql string) (int, error) {
	prepared, err := Prepare(sql)
	if err != nil {
		return 0, err
	}
	return prepared.MaxParameter(), nil
}

func maxParameterInTree(tree *pg_query.ParseResult) int {
	maximum := 0
	walkMessage(tree.ProtoReflect(), func(message protoreflect.Message) {
		if parameter, ok := message.Interface().(*pg_query.ParamRef); ok && int(parameter.Number) > maximum {
			maximum = int(parameter.Number)
		}
	})
	return maximum
}

// TargetForSchema is retained as the small routing API used by existing tests
// and callers. New code should use Analyze when it also needs write/table state.
func TargetForSchema(sql string, parameters [][]byte, registry schema.Registry, burrows []string) (string, bool) {
	plan, err := Analyze(sql, parameters, registry, burrows)
	return plan.Target, err == nil && plan.Routed
}

// BurrowForKey hashes a primary key into the fixed 64k vshard space and maps
// the vshard to the configured Burrow order using one-indexed modulo placement.
func BurrowForKey(key string, burrows []string) string {
	if len(burrows) == 0 {
		return ""
	}
	vshard := int(HashKey(key) % VirtualShards)
	remainder := vshard % len(burrows)
	if remainder == 0 {
		return burrows[len(burrows)-1]
	}
	return burrows[remainder-1]
}

func HashKey(key string) uint64 {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(key))
	return hash.Sum64()
}

func relationName(relation *pg_query.RangeVar) string {
	if relation == nil {
		return ""
	}
	if relation.Schemaname != "" {
		return relation.Schemaname + "." + relation.Relname
	}
	return relation.Relname
}

func insertKeyValues(statement *pg_query.InsertStmt, parameters [][]byte, columns, types []string) ([]string, bool) {
	if statement == nil || statement.SelectStmt == nil || statement.WithClause != nil {
		return nil, false
	}
	valuesStatement := statement.SelectStmt.GetSelectStmt()
	if valuesStatement == nil || len(valuesStatement.ValuesLists) != 1 {
		return nil, false
	}
	row := valuesStatement.ValuesLists[0].GetList()
	if row == nil || len(row.Items) != len(statement.Cols) {
		return nil, false
	}
	byColumn := make(map[string]*pg_query.Node, len(statement.Cols))
	for index, node := range statement.Cols {
		column := node.GetResTarget()
		if column == nil || column.Name == "" || len(column.Indirection) != 0 {
			return nil, false
		}
		name := column.Name
		if _, exists := byColumn[name]; exists {
			return nil, false
		}
		byColumn[name] = row.Items[index]
	}
	return orderedValues(byColumn, parameters, columns, types)
}

func updatesShardKey(statement *pg_query.UpdateStmt, columns []string) bool {
	keys := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		keys[column] = struct{}{}
	}
	for _, node := range statement.TargetList {
		target := node.GetResTarget()
		if target == nil || len(target.Indirection) != 0 {
			continue
		}
		if _, ok := keys[target.Name]; ok {
			return true
		}
	}
	return false
}

func predicateKeyValues(predicate *pg_query.Node, parameters [][]byte, columns, types []string, relation *pg_query.RangeVar) ([]string, bool) {
	if predicate == nil {
		return nil, false
	}
	byColumn := make(map[string]*pg_query.Node)
	if !collectEqualities(predicate, relation, byColumn) {
		return nil, false
	}
	return orderedValues(byColumn, parameters, columns, types)
}

func collectEqualities(node *pg_query.Node, relation *pg_query.RangeVar, values map[string]*pg_query.Node) bool {
	if node == nil {
		return true
	}
	if boolean := node.GetBoolExpr(); boolean != nil {
		if boolean.Boolop != pg_query.BoolExprType_AND_EXPR {
			return false
		}
		for _, item := range boolean.Args {
			if !collectEqualities(item, relation, values) {
				return false
			}
		}
		return true
	}
	expression := node.GetAExpr()
	if expression == nil || expression.Kind != pg_query.A_Expr_Kind_AEXPR_OP || operatorName(expression.Name) != "=" {
		return true
	}

	left, right := expression.Lexpr, expression.Rexpr
	if leftRow, rightRow := rowItems(left), rowItems(right); len(leftRow) != 0 || len(rightRow) != 0 {
		if len(leftRow) == 0 || len(leftRow) != len(rightRow) {
			return false
		}
		for index := range leftRow {
			if !collectEquality(leftRow[index], rightRow[index], relation, values) {
				return false
			}
		}
		return true
	}
	return collectEquality(left, right, relation, values)
}

func collectEquality(left, right *pg_query.Node, relation *pg_query.RangeVar, values map[string]*pg_query.Node) bool {
	column, ok := columnName(left, relation)
	value := right
	if !ok {
		column, ok = columnName(right, relation)
		value = left
	}
	if !ok {
		return true
	}
	if _, exists := values[column]; exists {
		return false
	}
	values[column] = value
	return true
}

func orderedValues(values map[string]*pg_query.Node, parameters [][]byte, columns, types []string) ([]string, bool) {
	result := make([]string, 0, len(columns))
	for index, column := range columns {
		node, ok := values[column]
		if !ok {
			return nil, false
		}
		dataType := ""
		if len(types) == len(columns) {
			dataType = types[index]
		}
		value, ok := scalarValue(node, parameters, dataType)
		if !ok {
			return nil, false
		}
		result = append(result, value)
	}
	return result, true
}

func columnName(node *pg_query.Node, relation *pg_query.RangeVar) (string, bool) {
	column := node.GetColumnRef()
	if column == nil || len(column.Fields) == 0 || len(column.Fields) > 3 {
		return "", false
	}
	parts := make([]string, 0, len(column.Fields))
	for _, field := range column.Fields {
		name := field.GetString_()
		if name == nil {
			return "", false
		}
		parts = append(parts, name.Sval)
	}
	if len(parts) > 1 && !validQualifier(parts[:len(parts)-1], relation) {
		return "", false
	}
	return parts[len(parts)-1], true
}

func validQualifier(parts []string, relation *pg_query.RangeVar) bool {
	if relation == nil {
		return false
	}
	if alias := relation.GetAlias(); alias != nil && len(parts) == 1 {
		return parts[0] == alias.Aliasname
	}
	if len(parts) == 1 {
		return parts[0] == relation.Relname
	}
	return len(parts) == 2 && parts[0] == relation.Schemaname && parts[1] == relation.Relname
}

func scalarValue(node *pg_query.Node, parameters [][]byte, targetType string) (string, bool) {
	value, explicitType, ok := rawScalarValue(node, parameters)
	if !ok {
		return "", false
	}
	if explicitType != "" {
		targetType = explicitType
	}
	return canonicalScalar(value, targetType)
}

func rawScalarValue(node *pg_query.Node, parameters [][]byte) (string, string, bool) {
	if node == nil {
		return "", "", false
	}
	if cast := node.GetTypeCast(); cast != nil {
		dataType, ok := typeName(cast.TypeName)
		if !ok {
			return "", "", false
		}
		value, _, ok := rawScalarValue(cast.Arg, parameters)
		if !ok {
			return "", "", false
		}
		value, ok = canonicalScalar(value, dataType)
		return value, dataType, ok
	}
	if collate := node.GetCollateClause(); collate != nil {
		return rawScalarValue(collate.Arg, parameters)
	}
	if parameter := node.GetParamRef(); parameter != nil {
		index := int(parameter.Number) - 1
		if index < 0 || index >= len(parameters) || parameters[index] == nil {
			return "", "", false
		}
		return string(parameters[index]), "", true
	}
	if constant := node.GetAConst(); constant != nil && !constant.Isnull {
		switch {
		case constant.GetSval() != nil:
			return constant.GetSval().Sval, "", true
		case constant.GetIval() != nil:
			return strconv.FormatInt(int64(constant.GetIval().Ival), 10), "", true
		case constant.GetFval() != nil:
			return constant.GetFval().Fval, "", true
		case constant.GetBoolval() != nil:
			return strconv.FormatBool(constant.GetBoolval().Boolval), "", true
		}
	}
	if expression := node.GetAExpr(); expression != nil && expression.Kind == pg_query.A_Expr_Kind_AEXPR_OP && expression.Lexpr == nil {
		operator := operatorName(expression.Name)
		if operator == "+" || operator == "-" {
			value, dataType, ok := rawScalarValue(expression.Rexpr, parameters)
			return operator + value, dataType, ok
		}
	}
	return "", "", false
}

func typeName(name *pg_query.TypeName) (string, bool) {
	if name == nil || len(name.Names) == 0 || len(name.ArrayBounds) != 0 {
		return "", false
	}
	parts := make([]string, 0, len(name.Names))
	for _, node := range name.Names {
		part := node.GetString_()
		if part == nil {
			return "", false
		}
		parts = append(parts, part.Sval)
	}
	return strings.Join(parts, "."), true
}

func canonicalScalar(value, dataType string) (string, bool) {
	if dataType == "" {
		return value, true
	}
	typeKey := strings.ToLower(strings.TrimSpace(dataType))
	if strings.HasPrefix(typeKey, "pg_catalog.") {
		typeKey = strings.TrimPrefix(typeKey, "pg_catalog.")
	}
	if index := strings.IndexByte(typeKey, '('); index >= 0 {
		typeKey = strings.TrimSpace(typeKey[:index])
	}
	switch typeKey {
	case "int2", "smallint", "smallserial", "serial2", "int4", "int", "integer", "serial", "serial4", "int8", "bigint", "bigserial", "serial8", "oid":
		integer, ok := new(big.Int).SetString(strings.TrimSpace(value), 10)
		if !ok {
			return "", false
		}
		return integer.String(), true
	case "numeric", "decimal":
		return canonicalDecimal(value)
	case "float4", "real":
		parsed, err := strconv.ParseFloat(strings.TrimSpace(value), 32)
		if err != nil {
			return "", false
		}
		return strconv.FormatFloat(parsed, 'g', -1, 32), true
	case "float8", "double precision":
		parsed, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
		if err != nil {
			return "", false
		}
		return strconv.FormatFloat(parsed, 'g', -1, 64), true
	case "bool", "boolean":
		switch strings.ToLower(strings.TrimSpace(value)) {
		case "true", "t", "yes", "y", "on", "1":
			return "true", true
		case "false", "f", "no", "n", "off", "0":
			return "false", true
		default:
			return "", false
		}
	case "uuid":
		compact := strings.ToLower(strings.Trim(strings.TrimSpace(value), "{}"))
		compact = strings.ReplaceAll(compact, "-", "")
		if len(compact) != 32 || !allHex(compact) {
			return "", false
		}
		return compact[0:8] + "-" + compact[8:12] + "-" + compact[12:16] + "-" + compact[16:20] + "-" + compact[20:32], true
	case "text", "varchar", "character varying", "name":
		return value, true
	case "bpchar", "char", "character":
		return strings.TrimRight(value, " "), true
	default:
		// Unknown PostgreSQL types may have non-obvious equality semantics. An
		// unrouted plan is safer than hashing a textual spelling incorrectly.
		return "", false
	}
}

func canonicalDecimal(value string) (string, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", false
	}
	sign := ""
	if value[0] == '+' || value[0] == '-' {
		if value[0] == '-' {
			sign = "-"
		}
		value = value[1:]
	}
	exponent := 0
	if index := strings.IndexAny(value, "eE"); index >= 0 {
		parsed, err := strconv.Atoi(value[index+1:])
		if err != nil {
			return "", false
		}
		exponent = parsed
		value = value[:index]
	}
	whole, fraction := value, ""
	if index := strings.IndexByte(value, '.'); index >= 0 {
		if strings.IndexByte(value[index+1:], '.') >= 0 {
			return "", false
		}
		whole, fraction = value[:index], value[index+1:]
	}
	if whole == "" && fraction == "" || !allDigits(whole) || !allDigits(fraction) {
		return "", false
	}
	digits := strings.TrimLeft(whole+fraction, "0")
	if digits == "" {
		return "0", true
	}
	scale := len(fraction) - exponent
	for scale > 0 && strings.HasSuffix(digits, "0") {
		digits = strings.TrimSuffix(digits, "0")
		scale--
	}
	if scale <= 0 {
		return sign + digits + strings.Repeat("0", -scale), true
	}
	if scale >= len(digits) {
		return sign + "0." + strings.Repeat("0", scale-len(digits)) + digits, true
	}
	return sign + digits[:len(digits)-scale] + "." + digits[len(digits)-scale:], true
}

func allDigits(value string) bool {
	for _, character := range value {
		if character < '0' || character > '9' {
			return false
		}
	}
	return true
}

func allHex(value string) bool {
	for _, character := range value {
		if !(character >= '0' && character <= '9') && !(character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}

func rowItems(node *pg_query.Node) []*pg_query.Node {
	if node == nil {
		return nil
	}
	if row := node.GetRowExpr(); row != nil {
		return row.Args
	}
	if list := node.GetList(); list != nil {
		return list.Items
	}
	return nil
}

func operatorName(nodes []*pg_query.Node) string {
	if len(nodes) != 1 || nodes[0].GetString_() == nil {
		return ""
	}
	return nodes[0].GetString_().Sval
}

func parseExpression(expression string) (*pg_query.Node, error) {
	tree, err := pg_query.Parse("SELECT " + expression)
	if err != nil || len(tree.Stmts) != 1 {
		return nil, fmt.Errorf("parse generated value expression: %w", err)
	}
	selectStatement := tree.Stmts[0].Stmt.GetSelectStmt()
	if selectStatement == nil || len(selectStatement.TargetList) != 1 || selectStatement.TargetList[0].GetResTarget() == nil {
		return nil, fmt.Errorf("parse generated value expression")
	}
	return selectStatement.TargetList[0].GetResTarget().Val, nil
}

func containsWrite(tree *pg_query.ParseResult) bool {
	found := false
	walkMessage(tree.ProtoReflect(), func(message protoreflect.Message) {
		switch message.Interface().(type) {
		case *pg_query.InsertStmt, *pg_query.UpdateStmt, *pg_query.DeleteStmt, *pg_query.MergeStmt:
			found = true
		}
	})
	return found
}

func physicalRelationNames(statement *pg_query.Node) []string {
	var names []string
	walkMessage(statement.ProtoReflect(), func(message protoreflect.Message) {
		if relation, ok := message.Interface().(*pg_query.RangeVar); ok {
			names = append(names, relationName(relation))
		}
	})
	return names
}

func unsafeScatterReason(statement *pg_query.SelectStmt, physicalRelations int) string {
	if statement == nil {
		return ""
	}
	if statement.Op != pg_query.SetOperation_SETOP_NONE {
		return "set operations over sharded tables require a PostgreSQL-equivalent global result merge"
	}
	if statement.WithClause != nil || countSelectStatements(statement) > 1 {
		return "CTEs and subqueries over sharded tables require a PostgreSQL-equivalent global result merge"
	}
	if physicalRelations > 1 {
		return "joins over sharded tables require a PostgreSQL-equivalent global result merge"
	}
	if len(statement.DistinctClause) > 0 || statement.GroupDistinct {
		return "DISTINCT over sharded tables requires a PostgreSQL-equivalent global result merge"
	}
	if hasAggregateOrWindow(statement) || len(statement.GroupClause) > 0 || statement.HavingClause != nil || len(statement.WindowClause) > 0 {
		return "aggregates, grouping, and window functions over sharded tables require a PostgreSQL-equivalent global result merge"
	}
	if len(statement.SortClause) > 0 {
		return "ORDER BY over sharded tables requires a PostgreSQL-equivalent global result merge"
	}
	if statement.LimitCount != nil || statement.LimitOffset != nil || statement.LimitOption != pg_query.LimitOption_LIMIT_OPTION_DEFAULT {
		return "LIMIT and OFFSET over sharded tables require a PostgreSQL-equivalent global result merge"
	}
	if len(statement.LockingClause) > 0 {
		return "row locking over sharded tables requires a PostgreSQL-equivalent global result merge"
	}
	return ""
}

func countSelectStatements(statement *pg_query.SelectStmt) int {
	count := 0
	walkMessage(statement.ProtoReflect(), func(message protoreflect.Message) {
		if _, ok := message.Interface().(*pg_query.SelectStmt); ok {
			count++
		}
	})
	return count
}

func hasAggregateOrWindow(statement *pg_query.SelectStmt) bool {
	found := false
	walkMessage(statement.ProtoReflect(), func(message protoreflect.Message) {
		call, ok := message.Interface().(*pg_query.FuncCall)
		if !ok || found {
			return
		}
		if call.Over != nil || call.AggStar || call.AggDistinct || call.AggFilter != nil || len(call.AggOrder) > 0 || call.AggWithinGroup {
			found = true
			return
		}
		name := functionName(call.Funcname)
		if isBuiltInAggregateName(name) {
			found = true
		}
	})
	return found
}

func isBuiltInAggregateName(name string) bool {
	switch strings.ToLower(name) {
	case "any_value", "array_agg", "avg", "bit_and", "bit_or", "bit_xor",
		"bool_and", "bool_or", "count", "every", "json_agg", "json_agg_strict",
		"json_arrayagg", "json_objectagg", "jsonb_agg", "jsonb_agg_strict",
		"json_object_agg", "json_object_agg_strict", "json_object_agg_unique",
		"json_object_agg_unique_strict", "jsonb_object_agg", "jsonb_object_agg_strict",
		"jsonb_object_agg_unique", "jsonb_object_agg_unique_strict", "max", "min",
		"range_agg", "range_intersect_agg", "string_agg", "sum", "xmlagg",
		"corr", "covar_pop", "covar_samp", "regr_avgx", "regr_avgy", "regr_count",
		"regr_intercept", "regr_r2", "regr_slope", "regr_sxx", "regr_sxy", "regr_syy",
		"stddev", "stddev_pop", "stddev_samp", "variance", "var_pop", "var_samp":
		return true
	default:
		return false
	}
}

func functionName(nodes []*pg_query.Node) string {
	parts := make([]string, 0, len(nodes))
	for _, node := range nodes {
		value := node.GetString_()
		if value == nil {
			return ""
		}
		parts = append(parts, value.Sval)
	}
	if len(parts) == 0 {
		return ""
	}
	return parts[len(parts)-1]
}

func walkMessage(message protoreflect.Message, visit func(protoreflect.Message)) {
	visit(message)
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if field.IsList() && field.Kind() == protoreflect.MessageKind {
			list := value.List()
			for index := 0; index < list.Len(); index++ {
				walkMessage(list.Get(index).Message(), visit)
			}
		} else if field.Kind() == protoreflect.MessageKind {
			walkMessage(value.Message(), visit)
		}
		return true
	})
}
