// SPDX-License-Identifier: AGPL-3.0-only

// Package integration tests the compiled gateway against the Docker Burrow environment.
package integration

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	collecttracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"github.com/jruszo/hamstergres/internal/nest"
	"github.com/jruszo/hamstergres/internal/router"
	"github.com/jruszo/hamstergres/internal/schema"
	"github.com/jruszo/hamstergres/internal/statistics"
	"github.com/jruszo/hamstergres/internal/status"
)

func TestGatewayEndToEnd(t *testing.T) {
	repoRoot := repositoryRoot(t)
	ensureDockerBurrows(t, repoRoot)

	frontendAddress := availableAddress(t)
	statusAddress := availableAddress(t)
	binary := buildGateway(t, repoRoot)
	configPath := writeGatewayConfig(t, frontendAddress, statusAddress)
	logs := startGateway(t, binary, configPath)
	statusURL := "http://" + statusAddress
	waitForHealthyGateway(t, statusURL, logs)

	queryGateway(t, frontendAddress, "SELECT 1 AS value", func(rows pgx.Rows) {
		values := make([]int32, 0, 2)
		for rows.Next() {
			var value int32
			if err := rows.Scan(&value); err != nil {
				t.Fatal(err)
			}
			values = append(values, value)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if len(values) != 1 || values[0] != 1 {
			t.Fatalf("topology-independent values = %#v, want one logical row", values)
		}
	})
	queryGateway(t, frontendAddress, "SELECT * FROM accounts WHERE tenant_id = 1 AND account_id = 1", func(rows pgx.Rows) {
		for rows.Next() {
			// The local data set may be empty or pre-existing; successful protocol
			// execution and the recorded normalized shape are what this checks.
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
	})
	queryGatewayError(t, frontendAddress, "SELECT * FROM hamstergres_missing_table", "XX000")

	snapshot := gatewaySnapshot(t, statusURL+"/api/v1/status")
	if snapshot.Sharding.Source != "hamstergres-nest" || snapshot.Sharding.VirtualShards != router.VirtualShards {
		t.Fatalf("sharding inventory metadata = %#v", snapshot.Sharding)
	}
	if snapshot.Sharding.SchemaRevision != 1 || snapshot.Sharding.TopologyRevision != 1 {
		t.Fatalf("versioned sharding inventory = %#v", snapshot.Sharding)
	}
	assertNestTopologyPlacement(t, "/hamstergres/tests/"+gatewayTestKey(frontendAddress)+"/topology", "public.accounts")
	accountsFound := false
	for _, table := range snapshot.Sharding.Tables {
		if table.Table == "public.accounts" && table.Sharded && strings.Join(table.ShardKeys, ",") == "tenant_id" {
			accountsFound = true
		}
	}
	if !accountsFound {
		t.Fatalf("accounts missing from sharding inventory: %#v", snapshot.Sharding.Tables)
	}
	if snapshot.Queries.Queries != 3 || snapshot.Queries.FailedQueries != 1 {
		t.Fatalf("query counters = %#v, want two successful and one failed query", snapshot.Queries)
	}
	if snapshot.QueryMetrics.Total.ScatteredQueries != 0 || snapshot.QueryMetrics.Total.SingleShardQueries != 3 {
		t.Fatalf("routing counters = %#v, want three single-Burrow queries", snapshot.QueryMetrics.Total)
	}
	assertTotalShardExecutions(t, snapshot.QueryMetrics.ShardExecutions, 3)
	assertSummary(t, snapshot.QueryMetrics.QuerySummaries, "SELECT ? AS value")
	assertSummary(t, snapshot.QueryMetrics.QuerySummaries, "SELECT * FROM accounts WHERE tenant_id = ? AND account_id = ?")
	metrics := getBody(t, statusURL+"/metrics")
	parser := expfmt.NewTextParser(model.UTF8Validation)
	if _, err := parser.TextToMetricFamilies(strings.NewReader(metrics)); err != nil {
		t.Fatalf("metrics endpoint is not valid Prometheus/OpenMetrics text: %v\n%s", err, metrics)
	}
	for _, want := range []string{
		`hamstergres_proxy_queries_total{outcome="success"} 2`,
		`hamstergres_proxy_queries_total{outcome="failure"} 1`,
		`hamstergres_proxy_query_failures_total{category="sql_error"} 1`,
		`hamstergres_proxy_query_routes_total{route="single_burrow"} 3`,
		`hamstergres_proxy_query_routes_total{route="scatter"} 0`,
		`hamstergres_proxy_burrow_executions_total{burrow="burrow-01"}`,
		`hamstergres_proxy_schema_revision 1`,
		`hamstergres_proxy_topology_revision 1`,
		`hamstergres_proxy_query_duration_seconds_count 3`,
		`hamstergres_proxy_table_sharded{table="public.accounts"} 1`,
		"# EOF",
	} {
		if !strings.Contains(metrics, want) {
			t.Fatalf("metrics endpoint did not contain %q:\n%s", want, metrics)
		}
	}

	page := getBody(t, statusURL+"/")
	if !strings.Contains(page, "SELECT * FROM accounts WHERE tenant_id = ? AND account_id = ?") {
		t.Fatalf("status page did not render the parameterized query shape:\n%s", page)
	}
	if !strings.Contains(page, "Sharding inventory") || !strings.Contains(page, "public.accounts") {
		t.Fatalf("status page lacks Nest inventory:\n%s", page)
	}
	if !strings.Contains(page, statistics.Fingerprint("SELECT ? AS value")) {
		t.Fatalf("status page did not render a query fingerprint:\n%s", page)
	}

	command := exec.Command(binary, "status", "--status-url", statusURL+"/api/v1/status")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("status CLI failed: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), "Routing: 0 scattered / 3 single-shard") || !strings.Contains(string(output), statistics.Fingerprint("SELECT * FROM accounts WHERE tenant_id = ? AND account_id = ?")) {
		t.Fatalf("status CLI output did not contain routing and fingerprint data:\n%s", output)
	}
	assertConcurrentQueriesAndMetricScrapes(t, frontendAddress, statusURL+"/metrics")
}

func TestTopologyTransparentReadResultsEndToEnd(t *testing.T) {
	repoRoot := repositoryRoot(t)
	ensureDockerBurrows(t, repoRoot)
	frontendAddress, statusAddress := availableAddress(t), availableAddress(t)
	logs := startGateway(t, buildGateway(t, repoRoot), writeGatewayConfig(t, frontendAddress, statusAddress))
	statusURL := "http://" + statusAddress
	waitForHealthyGateway(t, statusURL, logs)

	queryGateway(t, frontendAddress, "SELECT 1 AS value", func(rows pgx.Rows) {
		count := 0
		for rows.Next() {
			var value int32
			if err := rows.Scan(&value); err != nil {
				t.Fatal(err)
			}
			if value != 1 {
				t.Fatalf("relation-free value = %d, want 1", value)
			}
			count++
		}
		if count != 1 || rows.Err() != nil {
			t.Fatalf("relation-free rows = %d, err = %v; want one logical row", count, rows.Err())
		}
	})

	const catalogSQL = `SELECT c.relname
		FROM pg_catalog.pg_class c
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'public' AND c.relname = 'accounts'`
	queryGateway(t, frontendAddress, catalogSQL, func(rows pgx.Rows) {
		var names []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Fatal(err)
			}
			names = append(names, name)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if len(names) != 1 || names[0] != "accounts" {
			t.Fatalf("catalog rows = %#v, want one logical accounts row", names)
		}
	})

	config, err := pgx.ParseConfig("postgres://any-user@" + frontendAddress + "/any-database?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	connection, err := pgx.ConnectConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close(context.Background())

	var value int32
	if err := connection.QueryRow(context.Background(), "SELECT $1::int4", int32(7)).Scan(&value); err != nil || value != 7 {
		t.Fatalf("extended relation-free result = %d, err = %v", value, err)
	}
	var relation string
	if err := connection.QueryRow(context.Background(), `SELECT c.relname
		FROM pg_catalog.pg_class c
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'public' AND c.relname = $1`, "accounts").Scan(&relation); err != nil || relation != "accounts" {
		t.Fatalf("extended catalog result = %q, err = %v", relation, err)
	}
	var keyedCount int64
	if err := connection.QueryRow(context.Background(), "SELECT count(*) FROM accounts WHERE tenant_id = $1", int64(1)).Scan(&keyedCount); err != nil {
		t.Fatalf("keyed aggregate should execute on one Burrow: %v", err)
	}

	beforeFailures := gatewaySnapshot(t, statusURL+"/api/v1/status")
	connectionsBefore := operationTotal(beforeFailures, "backend_connection")
	queryGatewayError(t, frontendAddress, "SELECT count(*) FROM accounts", "0A000")
	_, err = connection.Exec(context.Background(), "SELECT tenant_id FROM accounts ORDER BY tenant_id")
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "0A000" {
		t.Fatalf("extended global-result error = %v, want SQLSTATE 0A000", err)
	}

	snapshot := gatewaySnapshot(t, statusURL+"/api/v1/status")
	if snapshot.QueryMetrics.Total.ScatteredQueries != 0 || snapshot.QueryMetrics.Total.SingleShardQueries != 5 {
		t.Fatalf("topology-transparent routing counters = %#v, want five single-Burrow executions", snapshot.QueryMetrics.Total)
	}
	if connectionsAfter := operationTotal(snapshot, "backend_connection"); connectionsAfter != connectionsBefore {
		t.Fatalf("unsupported global results opened Tunnels: backend connections changed from %d to %d", connectionsBefore, connectionsAfter)
	}
	assertTotalShardExecutions(t, snapshot.QueryMetrics.ShardExecutions, 5)
}

func TestTopologyControlsConfiguredBurrowMembership(t *testing.T) {
	repoRoot := repositoryRoot(t)
	ensureDockerBurrows(t, repoRoot)
	binary := buildGateway(t, repoRoot)
	testKey := "topology-membership-" + strconv.FormatInt(time.Now().UnixNano(), 10)

	seedFrontend := availableAddress(t)
	seedStatus := availableAddress(t)
	seedConfig := writeGatewayConfigWithKey(t, seedFrontend, seedStatus, testKey)
	seedLogs := startGateway(t, binary, seedConfig)
	waitForHealthyGateway(t, "http://"+seedStatus, seedLogs)

	extraFrontend := availableAddress(t)
	extraStatus := availableAddress(t)
	extraConfig := writeGatewayConfigWithKey(t, extraFrontend, extraStatus, testKey)
	addConfiguredBurrow(t, extraConfig, "burrow-03", "postgres://hamster:hamster@localhost:5541/hamstergres?sslmode=disable")
	extraLogs := startGateway(t, binary, extraConfig)
	waitForHealthyGateway(t, "http://"+extraStatus, extraLogs)
	queryGateway(t, extraFrontend, "SELECT 1", func(rows pgx.Rows) {
		count := 0
		for rows.Next() {
			count++
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("configured Burrow absent from topology produced %d rows, want one logical row", count)
		}
	})
	snapshot := gatewaySnapshot(t, "http://"+extraStatus+"/api/v1/status")
	if len(snapshot.Burrows) != 2 {
		t.Fatalf("configured Burrow absent from topology became routable: %#v", snapshot.Burrows)
	}

	missingFrontend := availableAddress(t)
	missingStatus := availableAddress(t)
	missingConfig := writeGatewayConfigWithKey(t, missingFrontend, missingStatus, testKey)
	removeConfiguredBurrow(t, missingConfig, "burrow-02")
	command := exec.Command(binary, "--config", missingConfig)
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatal("Proxy started after a topology owner was removed from YAML")
	}
	if !strings.Contains(string(output), "topology Burrow burrow-02 is not configured") {
		t.Fatalf("missing topology owner error was not actionable: %v\n%s", err, output)
	}
}

func TestLegacyV3TopologyUpgradePreservesOwners(t *testing.T) {
	repoRoot := repositoryRoot(t)
	ensureDockerBurrows(t, repoRoot)
	binary := buildGateway(t, repoRoot)
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	sourceKey := "topology-v4-source-" + suffix
	sourceFrontend := availableAddress(t)
	sourceStatus := availableAddress(t)
	sourceConfig := writeGatewayConfigWithKey(t, sourceFrontend, sourceStatus, sourceKey)
	sourceLogs := startGateway(t, binary, sourceConfig)
	waitForHealthyGateway(t, "http://"+sourceStatus, sourceLogs)

	sourceRegistryKey := "/hamstergres/tests/" + sourceKey + "/schema-registry"
	encoded := readNestValue(t, sourceRegistryKey)
	registry, err := schema.FromJSON(encoded)
	if err != nil {
		t.Fatal(err)
	}
	legacy := registry.Snapshot()
	legacy.FormatVersion = 0
	legacy.Revision = 0
	legacy.VShards = make([]string, router.VirtualShards)
	for index := range legacy.VShards {
		legacy.VShards[index] = "burrow-01"
	}
	legacyData, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}

	targetKey := "topology-v3-target-" + suffix
	targetRegistryKey := "/hamstergres/tests/" + targetKey + "/schema-registry"
	putNestValue(t, targetRegistryKey, legacyData)
	targetFrontend := availableAddress(t)
	targetStatus := availableAddress(t)
	targetConfig := writeGatewayConfigWithKey(t, targetFrontend, targetStatus, targetKey)
	targetLogs := startGateway(t, binary, targetConfig)
	waitForHealthyGateway(t, "http://"+targetStatus, targetLogs)

	topologyKey := "/hamstergres/tests/" + targetKey + "/topology"
	stored, found, err := nest.NewTopologyStore("http://127.0.0.1:2379", topologyKey).Get(t.Context())
	if err != nil || !found {
		t.Fatalf("read upgraded topology: found=%v err=%v", found, err)
	}
	placements, err := stored.Catalog.TablePlacements()
	if err != nil {
		t.Fatal(err)
	}
	if len(placements) == 0 {
		t.Fatal("legacy topology upgrade produced no table distributions")
	}
	for table, owners := range placements {
		if len(owners) != router.VirtualShards || owners[0] != "burrow-01" || owners[len(owners)-1] != "burrow-01" {
			t.Fatalf("legacy placement for %s was not preserved", table)
		}
	}
	migrated, err := schema.FromJSON(readNestValue(t, targetRegistryKey))
	if err != nil {
		t.Fatal(err)
	}
	if migrated.VShardCount() != 0 || migrated.Revision() != 1 {
		t.Fatalf("migrated schema still owns topology: revision=%d vshards=%d", migrated.Revision(), migrated.VShardCount())
	}
}

func TestASTRoutingPlacesRowsOnOnlyTheOwningBurrow(t *testing.T) {
	repoRoot := repositoryRoot(t)
	ensureDockerBurrows(t, repoRoot)
	frontendAddress, statusAddress := availableAddress(t), availableAddress(t)
	logs := startGateway(t, buildGateway(t, repoRoot), writeGatewayConfig(t, frontendAddress, statusAddress))
	statusURL := "http://" + statusAddress
	waitForHealthyGateway(t, statusURL, logs)

	queryGateway(t, frontendAddress, `DROP TABLE IF EXISTS ast_routing_e2e`, func(rows pgx.Rows) {})
	queryGateway(t, frontendAddress, `DROP TABLE IF EXISTS "QuotedRoutes"`, func(rows pgx.Rows) {})
	queryGateway(t, frontendAddress, `CREATE TABLE ast_routing_e2e (tenant_id bigint PRIMARY KEY, payload text NOT NULL)`, func(rows pgx.Rows) {})
	queryGateway(t, frontendAddress, `COMMENT ON COLUMN ast_routing_e2e.tenant_id IS 'hamstergres.shard_key'`, func(rows pgx.Rows) {})
	queryGateway(t, frontendAddress, `CREATE TABLE "QuotedRoutes" ("Tenant_ID" bigint PRIMARY KEY, payload text NOT NULL)`, func(rows pgx.Rows) {})
	queryGateway(t, frontendAddress, `COMMENT ON COLUMN "QuotedRoutes"."Tenant_ID" IS 'hamstergres.shard_key'`, func(rows pgx.Rows) {})
	t.Cleanup(func() {
		queryGateway(t, frontendAddress, `DROP TABLE IF EXISTS ast_routing_e2e`, func(rows pgx.Rows) {})
		queryGateway(t, frontendAddress, `DROP TABLE IF EXISTS "QuotedRoutes"`, func(rows pgx.Rows) {})
	})

	config, err := pgx.ParseConfig("postgres://any-user@" + frontendAddress + "/any-database?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	connection, err := pgx.ConnectConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close(context.Background())

	keys := keysForDifferentBurrows()
	connectionsBeforeQuoted := operationTotal(gatewaySnapshot(t, statusURL+"/api/v1/status"), "backend_connection")
	quotedKey := fmt.Sprintf("%08d", keys[0])
	quotedResult := connection.PgConn().ExecParams(
		context.Background(),
		`INSERT INTO "QuotedRoutes" ("Tenant_ID", payload) VALUES ($1, 'quoted')`,
		[][]byte{[]byte(quotedKey)},
		[]uint32{20},
		[]int16{0},
		[]int16{0},
	).Read()
	if quotedResult.Err != nil {
		t.Fatalf("quoted canonical-key insert: %v", quotedResult.Err)
	}
	connectionsAfterQuoted := operationTotal(gatewaySnapshot(t, statusURL+"/api/v1/status"), "backend_connection")
	if delta := connectionsAfterQuoted - connectionsBeforeQuoted; delta != 1 {
		t.Fatalf("routed extended execution acquired %d Burrow connections, want 1", delta)
	}
	for index, port := range []string{"5541", "5542"} {
		direct, err := pgx.Connect(context.Background(), "postgres://hamster:hamster@localhost:"+port+"/hamstergres?sslmode=disable")
		if err != nil {
			t.Fatal(err)
		}
		var count int
		err = direct.QueryRow(context.Background(), `SELECT count(*) FROM "QuotedRoutes" WHERE "Tenant_ID" = $1`, keys[0]).Scan(&count)
		direct.Close(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		want := 0
		if index == 0 {
			want = 1
		}
		if count != want {
			t.Fatalf("quoted canonical-key placement on Burrow %d = %d, want %d", index+1, count, want)
		}
	}
	for index, key := range keys {
		if _, err := connection.Exec(context.Background(), `INSERT INTO ast_routing_e2e (payload, tenant_id) VALUES ($1, ($2)::bigint)`, fmt.Sprintf("value,%d", index), key); err != nil {
			t.Fatalf("AST-routed insert for key %d: %v", key, err)
		}
	}

	for index, port := range []string{"5541", "5542"} {
		direct, err := pgx.Connect(context.Background(), "postgres://hamster:hamster@localhost:"+port+"/hamstergres?sslmode=disable")
		if err != nil {
			t.Fatal(err)
		}
		var own, other int
		err = direct.QueryRow(context.Background(), `SELECT count(*) FILTER (WHERE tenant_id = $1), count(*) FILTER (WHERE tenant_id = $2) FROM ast_routing_e2e`, keys[index], keys[1-index]).Scan(&own, &other)
		direct.Close(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if own != 1 || other != 0 {
			t.Fatalf("Burrow %d placement = own:%d other:%d, want own:1 other:0", index+1, own, other)
		}
	}

	beforeUnsafe := gatewaySnapshot(t, statusURL+"/api/v1/status")
	cacheBefore := operationTotal(beforeUnsafe, "prepared_statement_cache")
	connectionsBefore := operationTotal(beforeUnsafe, "backend_connection")
	if _, err := connection.Exec(context.Background(), `UPDATE ast_routing_e2e AS a SET payload = 'unsafe' WHERE a.tenant_id = $1 OR a.tenant_id = $2`, keys[0], keys[1]); err == nil {
		t.Fatal("ambiguous OR write reached a Burrow")
	} else {
		var postgresError *pgconn.PgError
		if !errors.As(err, &postgresError) || postgresError.Code != "0A000" {
			t.Fatalf("ambiguous write error = %v, want 0A000", err)
		}
	}
	afterUnsafe := gatewaySnapshot(t, statusURL+"/api/v1/status")
	cacheAfter := operationTotal(afterUnsafe, "prepared_statement_cache")
	if cacheAfter != cacheBefore {
		t.Fatalf("ambiguous write changed backend Parse cache operations from %d to %d", cacheBefore, cacheAfter)
	}
	connectionsAfter := operationTotal(afterUnsafe, "backend_connection")
	if connectionsAfter != connectionsBefore {
		t.Fatalf("ambiguous write opened Burrow connections: backend_connection changed from %d to %d", connectionsBefore, connectionsAfter)
	}
	if _, err := connection.Exec(context.Background(), `UPDATE ast_routing_e2e AS a SET payload = 'targeted' WHERE (($1)::bigint = a.tenant_id)`, keys[0]); err != nil {
		t.Fatalf("AST-routed aliased update: %v", err)
	}

	for index, port := range []string{"5541", "5542"} {
		direct, err := pgx.Connect(context.Background(), "postgres://hamster:hamster@localhost:"+port+"/hamstergres?sslmode=disable")
		if err != nil {
			t.Fatal(err)
		}
		var payload string
		err = direct.QueryRow(context.Background(), `SELECT payload FROM ast_routing_e2e WHERE tenant_id = $1`, keys[index]).Scan(&payload)
		direct.Close(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		want := fmt.Sprintf("value,%d", index)
		if index == 0 {
			want = "targeted"
		}
		if payload != want {
			t.Fatalf("Burrow %d payload = %q, want %q", index+1, payload, want)
		}
	}
}

func operationTotal(snapshot status.Snapshot, operation string) int64 {
	var total int64
	for _, item := range snapshot.QueryMetrics.Operations {
		if item.Operation == operation {
			total += item.Count
		}
	}
	return total
}

func TestReplicatedUnshardedTablesEndToEnd(t *testing.T) {
	repoRoot := repositoryRoot(t)
	ensureDockerBurrows(t, repoRoot)
	frontendAddress, statusAddress := availableAddress(t), availableAddress(t)
	configPath := writeGatewayConfig(t, frontendAddress, statusAddress)
	contents, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	contents = bytes.Replace(contents, []byte("sharding:\n"), []byte("sharding:\n  unsharded_tables:\n    mode: replicated\n"), 1)
	if err := os.WriteFile(configPath, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	logs := startGateway(t, buildGateway(t, repoRoot), configPath)
	waitForHealthyGateway(t, "http://"+statusAddress, logs)
	queryGateway(t, frontendAddress, "DROP TABLE IF EXISTS replicated_e2e; CREATE TABLE replicated_e2e (value bigint)", func(rows pgx.Rows) {})
	queryGateway(t, frontendAddress, "INSERT INTO replicated_e2e (value) VALUES (42)", func(rows pgx.Rows) {})
	copyConnection, err := pgconn.Connect(context.Background(), "postgres://any-user@"+frontendAddress+"/any-database?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := copyConnection.CopyFrom(context.Background(), strings.NewReader("43\n44\n"), "COPY replicated_e2e (value) FROM STDIN"); err != nil {
		copyConnection.Close(context.Background())
		t.Fatalf("replicated COPY FROM STDIN: %v\ngateway logs:\n%s", err, logs.String())
	}
	copyConnection.Close(context.Background())
	for _, port := range []string{"5541", "5542"} {
		connection, err := pgx.Connect(context.Background(), "postgres://hamster:hamster@localhost:"+port+"/hamstergres?sslmode=disable")
		if err != nil {
			t.Fatal(err)
		}
		var count int
		err = connection.QueryRow(context.Background(), "SELECT count(*) FROM replicated_e2e WHERE value IN (42, 43, 44)").Scan(&count)
		connection.Close(context.Background())
		if err != nil || count != 3 {
			t.Fatalf("Burrow %s count = %d, err = %v", port, count, err)
		}
	}
	queryGateway(t, frontendAddress, "SELECT value FROM replicated_e2e", func(rows pgx.Rows) {
		count := 0
		for rows.Next() {
			count++
		}
		if count != 3 {
			t.Fatalf("replicated read returned %d rows, want one Burrow's three rows", count)
		}
	})
	snapshot := gatewaySnapshot(t, "http://"+statusAddress+"/api/v1/status")
	found := false
	for _, table := range snapshot.Sharding.Tables {
		if table.Table == "public.replicated_e2e" && !table.Sharded {
			found = true
		}
	}
	if !found {
		t.Fatalf("unsharded table missing from Nest inventory: %#v", snapshot.Sharding.Tables)
	}
}

func TestPgbenchInitializationThroughProxy(t *testing.T) {
	repoRoot := repositoryRoot(t)
	ensureDockerBurrows(t, repoRoot)
	pgbench := ensurePgbench(t)

	frontendAddress, statusAddress := availableAddress(t), availableAddress(t)
	testKey := fmt.Sprintf("pgbench-init-%d", time.Now().UnixNano())
	logs := startGateway(t, buildGateway(t, repoRoot), writeGatewayConfigWithKey(t, frontendAddress, statusAddress, testKey))
	statusURL := "http://" + statusAddress
	waitForHealthyGateway(t, statusURL, logs)

	const cleanupSQL = "DROP TABLE IF EXISTS pgbench_accounts, pgbench_branches, pgbench_history, pgbench_tellers"
	if err := execGateway(frontendAddress, cleanupSQL); err != nil {
		t.Fatalf("clean pgbench tables before initialization: %v", err)
	}
	t.Cleanup(func() {
		if err := execGateway(frontendAddress, cleanupSQL); err != nil {
			t.Logf("clean pgbench tables: %v", err)
		}
	})

	if output, err := runPgbench(pgbench, frontendAddress, "-i", "-s", "1"); err != nil {
		t.Fatalf("pgbench initialization through Hamstergres Proxy: %v\n%s\ngateway logs:\n%s", err, output, logs.String())
	}

	tables := []struct {
		name string
		key  string
		rows int
	}{
		{name: "pgbench_accounts", key: "aid", rows: 100000},
		{name: "pgbench_branches", key: "bid", rows: 1},
		{name: "pgbench_tellers", key: "tid", rows: 10},
	}
	for index, port := range []string{"5541", "5542"} {
		direct, err := pgx.Connect(context.Background(), "postgres://hamster:hamster@localhost:"+port+"/hamstergres?sslmode=disable")
		if err != nil {
			t.Fatal(err)
		}
		for _, table := range tables {
			var rows, distinctKeys int
			query := fmt.Sprintf("SELECT count(*), count(DISTINCT %s) FROM %s", table.key, table.name)
			if err := direct.QueryRow(context.Background(), query).Scan(&rows, &distinctKeys); err != nil {
				direct.Close(context.Background())
				t.Fatal(err)
			}
			want := 0
			if index == 0 {
				want = table.rows
			}
			if rows != want || distinctKeys != want {
				direct.Close(context.Background())
				t.Fatalf("Burrow %s %s rows/distinct keys = %d/%d, want %d/%d", port, table.name, rows, distinctKeys, want, want)
			}
			var primaryKey bool
			if err := direct.QueryRow(context.Background(), `SELECT EXISTS (
				SELECT 1 FROM pg_index i
				JOIN pg_class c ON c.oid = i.indrelid
				WHERE c.oid = $1::regclass AND i.indisprimary
			)`, table.name).Scan(&primaryKey); err != nil || !primaryKey {
				direct.Close(context.Background())
				t.Fatalf("Burrow %s %s primary key missing, err = %v", port, table.name, err)
			}
		}
		direct.Close(context.Background())
	}

	snapshot := gatewaySnapshot(t, statusURL+"/api/v1/status")
	assertNestInventoryMatches(t, "/hamstergres/tests/"+testKey+"/schema-registry", snapshot.Sharding.Tables)
	for _, mode := range []string{"simple", "extended", "prepared"} {
		output, err := runPgbench(pgbench, frontendAddress, "-S", "-M", mode, "-c", "1", "-j", "1", "-t", "10")
		if err != nil {
			t.Fatalf("read-only pgbench %s protocol: %v\n%s\ngateway logs:\n%s", mode, err, output, logs.String())
		}
		if !strings.Contains(output, "number of failed transactions: 0") {
			t.Fatalf("read-only pgbench %s did not report zero failures:\n%s", mode, output)
		}
	}

	if err := execGateway(frontendAddress, cleanupSQL); err != nil {
		t.Fatalf("clean pgbench tables after initialization: %v", err)
	}
	snapshot = gatewaySnapshot(t, statusURL+"/api/v1/status")
	assertNestInventoryMatches(t, "/hamstergres/tests/"+testKey+"/schema-registry", snapshot.Sharding.Tables)
	for _, table := range snapshot.Sharding.Tables {
		if strings.HasPrefix(table.Table, "public.pgbench_") {
			t.Fatalf("pgbench table remained in live inventory after cleanup: %#v", table)
		}
	}

	if output, err := runPgbenchWorkload(repoRoot, frontendAddress, "sharded", "prepare", "--scale=1"); err != nil {
		t.Fatalf("prepare sharded pgbench dataset: %v\n%s\ngateway logs:\n%s", err, output, logs.String())
	}
	accountRows := make([]int, 0, 2)
	for index, port := range []string{"5541", "5542"} {
		direct, err := pgx.Connect(context.Background(), "postgres://hamster:hamster@localhost:"+port+"/hamstergres?sslmode=disable")
		if err != nil {
			t.Fatal(err)
		}
		var accounts, branches, tellers int
		if err := direct.QueryRow(context.Background(), `SELECT
			(SELECT count(*) FROM pgbench_accounts),
			(SELECT count(*) FROM pgbench_branches),
			(SELECT count(*) FROM pgbench_tellers)`).Scan(&accounts, &branches, &tellers); err != nil {
			direct.Close(context.Background())
			t.Fatal(err)
		}
		accountRows = append(accountRows, accounts)
		wantBranches, wantTellers := 0, 0
		if index == 0 {
			wantBranches, wantTellers = 1, 10
		}
		if branches != wantBranches || tellers != wantTellers {
			direct.Close(context.Background())
			t.Fatalf("Burrow %s sharded pgbench dimensions = %d/%d, want %d/%d", port, branches, tellers, wantBranches, wantTellers)
		}
		direct.Close(context.Background())
	}
	if accountRows[0] == 0 || accountRows[1] == 0 || accountRows[0]+accountRows[1] != 100000 {
		t.Fatalf("sharded pgbench account rows = %v, want 100000 distributed across both Burrows", accountRows)
	}
	snapshot = gatewaySnapshot(t, statusURL+"/api/v1/status")
	foundShardedAccounts := false
	for _, table := range snapshot.Sharding.Tables {
		if table.Table == "public.pgbench_accounts" && table.Sharded && len(table.ShardKeys) == 1 && table.ShardKeys[0] == "aid" {
			foundShardedAccounts = true
		}
		if table.Table != "public.pgbench_accounts" && strings.HasPrefix(table.Table, "public.pgbench_") && table.Sharded {
			t.Fatalf("pgbench dimension table should remain unsharded: %#v", table)
		}
	}
	if !foundShardedAccounts {
		t.Fatalf("sharded pgbench accounts missing from inventory: %#v", snapshot.Sharding.Tables)
	}
	for _, mode := range []string{"simple", "extended", "prepared"} {
		output, err := runPgbench(pgbench, frontendAddress, "-S", "-M", mode, "-c", "1", "-j", "1", "-t", "10")
		if err != nil {
			t.Fatalf("sharded read-only pgbench %s protocol: %v\n%s\ngateway logs:\n%s", mode, err, output, logs.String())
		}
		if !strings.Contains(output, "number of failed transactions: 0") {
			t.Fatalf("sharded read-only pgbench %s did not report zero failures:\n%s", mode, output)
		}
	}
	if output, err := runPgbenchWorkload(repoRoot, frontendAddress, "sharded", "cleanup"); err != nil {
		t.Fatalf("cleanup sharded pgbench dataset: %v\n%s", err, output)
	}
}

func TestTracingAndObservabilityFailureEndToEnd(t *testing.T) {
	repoRoot := repositoryRoot(t)
	ensureDockerBurrows(t, repoRoot)
	var mu sync.Mutex
	var spans []*tracepb.Span
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var request collecttracepb.ExportTraceServiceRequest
		if err := proto.Unmarshal(body, &request); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		for _, resourceSpans := range request.ResourceSpans {
			for _, scopeSpans := range resourceSpans.ScopeSpans {
				spans = append(spans, scopeSpans.Spans...)
			}
		}
		mu.Unlock()
		response, _ := proto.Marshal(&collecttracepb.ExportTraceServiceResponse{})
		w.Header().Set("Content-Type", "application/x-protobuf")
		_, _ = w.Write(response)
	}))
	defer collector.Close()

	frontendAddress, statusAddress := availableAddress(t), availableAddress(t)
	binary := buildGateway(t, repoRoot)
	configPath := writeGatewayConfig(t, frontendAddress, statusAddress)
	contents, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	contents = append(contents, []byte("\nobservability:\n  log_file: \"/missing/hamstergres/proxy.log\"\n")...)
	if err := os.WriteFile(configPath, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	logs := startGatewayWithEnv(t, binary, configPath, []string{
		"OTEL_TRACES_EXPORTER=otlp",
		"OTEL_SERVICE_NAME=hamstergres-proxy-integration",
		"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=" + collector.URL + "/v1/traces",
	})
	statusURL := "http://" + statusAddress
	waitForHealthyGateway(t, statusURL, logs)
	assertStructuredLogEvent(t, logs.String(), "logging_configuration_failed", "observability")
	queryGateway(t, frontendAddress, "SELECT 1", func(rows pgx.Rows) {
		for rows.Next() {
		}
	})
	queryGatewayError(t, frontendAddress, "SELECT * FROM hamstergres_missing_trace_table", "XX000")

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		count := len(spans)
		mu.Unlock()
		if count >= 4 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(spans) < 4 {
		t.Fatalf("exported spans = %d, want at least 4; gateway logs:\n%s", len(spans), logs.String())
	}
	assertExportedTraceShape(t, spans)
}

func TestStatusListenerFailureDoesNotBlockQueries(t *testing.T) {
	repoRoot := repositoryRoot(t)
	ensureDockerBurrows(t, repoRoot)
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	frontendAddress := availableAddress(t)
	binary := buildGateway(t, repoRoot)
	configPath := writeGatewayConfig(t, frontendAddress, occupied.Addr().String())
	logs := startGateway(t, binary, configPath)
	waitForGatewayQuery(t, frontendAddress, logs)
	assertStructuredLogEvent(t, logs.String(), "status_server_failed", "network")
}

func waitForGatewayQuery(t *testing.T, address string, logs *synchronizedBuffer) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		config, err := pgx.ParseConfig("postgres://any-user@" + address + "/any-database?sslmode=disable")
		if err == nil {
			config.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
			connection, connectErr := pgx.ConnectConfig(context.Background(), config)
			if connectErr == nil {
				_, queryErr := connection.Exec(context.Background(), "SELECT 1")
				connection.Close(context.Background())
				if queryErr == nil {
					return
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("Proxy did not serve queries after status listener failure:\n%s", logs.String())
}

func assertStructuredLogEvent(t *testing.T, logs, event, category string) {
	t.Helper()
	for _, line := range strings.Split(logs, "\n") {
		var fields map[string]any
		if json.Unmarshal([]byte(line), &fields) != nil || fields["event"] != event {
			continue
		}
		if fields["component"] != "hamstergres-proxy" || fields["error_category"] != category || fields["level"] == nil {
			t.Fatalf("structured event %s lacks required fields: %#v", event, fields)
		}
		return
	}
	t.Fatalf("structured event %s not found:\n%s", event, logs)
}

func assertExportedTraceShape(t *testing.T, spans []*tracepb.Span) {
	t.Helper()
	parents := make(map[string]*tracepb.Span)
	for _, span := range spans {
		if span.Name == "proxy.query" {
			parents[string(span.SpanId)] = span
		}
		for _, item := range span.Attributes {
			if item.Key == "db.statement" || item.Key == "db.query.text" {
				t.Fatalf("sensitive SQL attribute exported: %s", item.Key)
			}
		}
	}
	if len(parents) < 2 {
		t.Fatalf("query spans = %d, all spans = %#v", len(parents), spans)
	}
	tunnelsByParent := make(map[string]map[string]bool)
	failedQuery := false
	for _, span := range spans {
		if span.Name == "proxy.query" && span.Status.GetCode() == tracepb.Status_STATUS_CODE_ERROR {
			failedQuery = true
		}
		if span.Name != "tunnel.execute" {
			continue
		}
		parent := string(span.ParentSpanId)
		if parents[parent] == nil {
			t.Fatal("Tunnel span is not parented by a frontend query")
		}
		if tunnelsByParent[parent] == nil {
			tunnelsByParent[parent] = make(map[string]bool)
		}
		for _, item := range span.Attributes {
			if item.Key == "hamstergres.burrow" {
				tunnelsByParent[parent][item.Value.GetStringValue()] = true
			}
		}
	}
	if !failedQuery {
		t.Fatal("failed frontend query did not export error status")
	}
	scattered, single := 0, 0
	for parent := range parents {
		switch len(tunnelsByParent[parent]) {
		case 2:
			scattered++
		case 1:
			single++
		default:
			t.Fatalf("query %x selected no Tunnel", parent)
		}
	}
	if scattered != 0 || single != 2 {
		t.Fatalf("Tunnel routes = %d scattered and %d single-Burrow, want two single-Burrow queries", scattered, single)
	}
}

func TestExtendedQueryEndToEnd(t *testing.T) {
	repoRoot := repositoryRoot(t)
	ensureDockerBurrows(t, repoRoot)

	frontendAddress := availableAddress(t)
	statusAddress := availableAddress(t)
	binary := buildGateway(t, repoRoot)
	configPath := writeGatewayConfig(t, frontendAddress, statusAddress)
	logs := startGateway(t, binary, configPath)
	statusURL := "http://" + statusAddress
	waitForHealthyGateway(t, statusURL, logs)

	config, err := pgx.ParseConfig("postgres://any-user@" + frontendAddress + "/any-database?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	connection, err := pgx.ConnectConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close(context.Background())

	queryContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rows, err := connection.Query(queryContext, "SELECT $1::int4 AS value", int32(7))
	if err != nil {
		t.Fatalf("extended query: %v\ngateway logs:\n%s", err, logs.String())
	}
	defer rows.Close()
	values := make([]int32, 0, 2)
	for rows.Next() {
		var value int32
		if err := rows.Scan(&value); err != nil {
			t.Fatal(err)
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(values) != 1 || values[0] != 7 {
		t.Fatalf("extended-query values = %#v, want one logical row", values)
	}

	snapshot := gatewaySnapshot(t, statusURL+"/api/v1/status")
	if snapshot.QueryMetrics.Total.Queries != 1 || snapshot.QueryMetrics.Total.FailedQueries != 0 {
		t.Fatalf("extended-query counters = %#v, want one successful query", snapshot.QueryMetrics.Total)
	}
	assertTotalShardExecutions(t, snapshot.QueryMetrics.ShardExecutions, 1)
	assertSummary(t, snapshot.QueryMetrics.QuerySummaries, "SELECT $?::int4 AS value")
}

func TestExtendedPreparedStatementLifecycleEndToEnd(t *testing.T) {
	repoRoot := repositoryRoot(t)
	ensureDockerBurrows(t, repoRoot)

	frontendAddress := availableAddress(t)
	statusAddress := availableAddress(t)
	binary := buildGateway(t, repoRoot)
	configPath := writeGatewayConfig(t, frontendAddress, statusAddress)
	logs := startGateway(t, binary, configPath)
	statusURL := "http://" + statusAddress
	waitForHealthyGateway(t, statusURL, logs)

	connection, err := pgconn.Connect(context.Background(), "postgres://any-user@"+frontendAddress+"/any-database?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close(context.Background())

	statement, err := connection.Prepare(context.Background(), "extended_lifecycle", "SELECT $1::int4 AS value", []uint32{23})
	if err != nil {
		t.Fatalf("Parse/Describe/Sync: %v\ngateway logs:\n%s", err, logs.String())
	}
	result := connection.ExecPrepared(context.Background(), statement.Name, [][]byte{[]byte("9")}, []int16{0}, []int16{0}).Read()
	if result.Err != nil {
		t.Fatalf("Bind/Execute/Sync: %v\ngateway logs:\n%s", result.Err, logs.String())
	}
	if len(result.Rows) != 1 || string(result.Rows[0][0]) != "9" {
		t.Fatalf("prepared rows = %#v, want one logical row", result.Rows)
	}
	if err := connection.Deallocate(context.Background(), statement.Name); err != nil {
		t.Fatalf("Close/Sync: %v\ngateway logs:\n%s", err, logs.String())
	}
	alias, err := connection.Prepare(context.Background(), "extended_lifecycle_alias", "SELECT $1::int4 AS value", []uint32{23})
	if err != nil {
		t.Fatalf("cached Parse/Describe/Sync: %v\ngateway logs:\n%s", err, logs.String())
	}
	if err := connection.Deallocate(context.Background(), alias.Name); err != nil {
		t.Fatalf("cached Close/Sync: %v\ngateway logs:\n%s", err, logs.String())
	}
	missing := connection.ExecPrepared(context.Background(), statement.Name, nil, nil, nil).Read()
	var postgresError *pgconn.PgError
	if !errors.As(missing.Err, &postgresError) || postgresError.Code != "26000" {
		t.Fatalf("execute closed statement error = %v, want SQLSTATE 26000", missing.Err)
	}

	snapshot := gatewaySnapshot(t, statusURL+"/api/v1/status")
	if snapshot.QueryMetrics.Total.Queries != 1 || snapshot.QueryMetrics.Total.FailedQueries != 0 {
		t.Fatalf("extended lifecycle counters = %#v, want one successful tracked query", snapshot.QueryMetrics.Total)
	}
	cache := map[string]int64{}
	for _, operation := range snapshot.QueryMetrics.Operations {
		if operation.Operation == "prepared_statement_cache" {
			cache[operation.Outcome] = operation.Count
		}
	}
	if cache["hit"] < 1 || cache["miss"] < 1 {
		t.Fatalf("prepared statement cache operations = %#v, want at least one hit and miss", cache)
	}
	assertTotalShardExecutions(t, snapshot.QueryMetrics.ShardExecutions, 1)
}

func TestCopyProtocolEndToEnd(t *testing.T) {
	repoRoot := repositoryRoot(t)
	ensureDockerBurrows(t, repoRoot)

	frontendAddress := availableAddress(t)
	statusAddress := availableAddress(t)
	binary := buildGateway(t, repoRoot)
	configPath := writeGatewayConfig(t, frontendAddress, statusAddress)
	logs := startGateway(t, binary, configPath)
	waitForHealthyGateway(t, "http://"+statusAddress, logs)

	connection, err := pgconn.Connect(context.Background(), "postgres://any-user@"+frontendAddress+"/any-database?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close(context.Background())

	if _, err := connection.Exec(context.Background(), "DROP TABLE IF EXISTS copy_e2e; CREATE TABLE copy_e2e (id bigint PRIMARY KEY, value text)").ReadAll(); err != nil {
		t.Fatalf("prepare COPY table: %v", err)
	}
	if _, err := connection.CopyFrom(context.Background(), strings.NewReader("1\tone\n2\ttwo\n"), "COPY copy_e2e (id, value) FROM STDIN"); err != nil {
		t.Fatalf("COPY FROM STDIN: %v\ngateway logs:\n%s", err, logs.String())
	}
	var copied bytes.Buffer
	if _, err := connection.CopyTo(context.Background(), &copied, "COPY copy_e2e (id, value) TO STDOUT"); err != nil {
		t.Fatalf("COPY TO STDOUT: %v\ngateway logs:\n%s", err, logs.String())
	}
	if got := strings.Count(copied.String(), "\n"); got != 2 {
		t.Fatalf("COPY TO rows = %d, want two primary-Burrow rows: %q", got, copied.String())
	}
	for index, port := range []string{"5541", "5542"} {
		direct, err := pgx.Connect(context.Background(), "postgres://hamster:hamster@localhost:"+port+"/hamstergres?sslmode=disable")
		if err != nil {
			t.Fatal(err)
		}
		var count int
		err = direct.QueryRow(context.Background(), "SELECT count(*) FROM copy_e2e").Scan(&count)
		direct.Close(context.Background())
		want := 0
		if index == 0 {
			want = 2
		}
		if err != nil || count != want {
			t.Fatalf("Burrow %s COPY row count = %d, want %d, err = %v", port, count, want, err)
		}
	}
}

func TestShardAwareCopyProtocolEndToEnd(t *testing.T) {
	repoRoot := repositoryRoot(t)
	ensureDockerBurrows(t, repoRoot)
	frontendAddress, statusAddress := availableAddress(t), availableAddress(t)
	logs := startGateway(t, buildGateway(t, repoRoot), writeGatewayConfig(t, frontendAddress, statusAddress))
	waitForHealthyGateway(t, "http://"+statusAddress, logs)

	connection, err := pgconn.Connect(context.Background(), "postgres://any-user@"+frontendAddress+"/any-database?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close(context.Background())
	if _, err := connection.Exec(context.Background(), `
		DROP TABLE IF EXISTS shard_copy_e2e;
		CREATE TABLE shard_copy_e2e (
			tenant_id bigint NOT NULL,
			region text NOT NULL,
			payload text NOT NULL,
			PRIMARY KEY (tenant_id, region)
		);
		COMMENT ON COLUMN shard_copy_e2e.tenant_id IS 'hamstergres.shard_key';
		COMMENT ON COLUMN shard_copy_e2e.region IS 'hamstergres.shard_key'`).ReadAll(); err != nil {
		t.Fatalf("prepare shard-aware COPY table: %v\ngateway logs:\n%s", err, logs.String())
	}
	t.Cleanup(func() {
		queryGateway(t, frontendAddress, "DROP TABLE IF EXISTS shard_copy_e2e", func(rows pgx.Rows) {})
	})

	textKeys := compoundKeysForDifferentBurrows("eu-west")
	textInput := fmt.Sprintf("%d\teu-west\ttext-one\n%d\teu-west\ttext-two\n", textKeys[0], textKeys[1])
	tag, err := connection.CopyFrom(context.Background(), strings.NewReader(textInput), "COPY shard_copy_e2e (tenant_id, region, payload) FROM STDIN")
	if err != nil || tag.RowsAffected() != 2 {
		t.Fatalf("sharded text COPY = %s, %v\ngateway logs:\n%s", tag, err, logs.String())
	}
	assertCopiedRowPlacement(t, textKeys[0], "eu-west", "text-one")
	assertCopiedRowPlacement(t, textKeys[1], "eu-west", "text-two")

	csvKeys := compoundKeysForDifferentBurrows("eu,west")
	csvInput := fmt.Sprintf("region,payload,tenant_id\n\"eu,west\",csv-one,%d\n\"eu,west\",\"csv,two\",%d\n", csvKeys[0], csvKeys[1])
	tag, err = connection.CopyFrom(context.Background(), strings.NewReader(csvInput), `COPY shard_copy_e2e (region, payload, tenant_id) FROM STDIN WITH (FORMAT csv, HEADER true)`)
	if err != nil || tag.RowsAffected() != 2 {
		t.Fatalf("sharded CSV COPY = %s, %v\ngateway logs:\n%s", tag, err, logs.String())
	}
	assertCopiedRowPlacement(t, csvKeys[0], "eu,west", "csv-one")
	assertCopiedRowPlacement(t, csvKeys[1], "eu,west", "csv,two")
	var copiedCSV bytes.Buffer
	tag, err = connection.CopyTo(context.Background(), &copiedCSV, `COPY shard_copy_e2e (tenant_id, region, payload) TO STDOUT WITH (FORMAT csv, HEADER true)`)
	if err != nil || tag.RowsAffected() != 4 || strings.Count(copiedCSV.String(), "tenant_id,region,payload\n") != 1 {
		t.Fatalf("merged CSV COPY TO = %s, header count %d, err %v: %q", tag, strings.Count(copiedCSV.String(), "tenant_id,region,payload\n"), err, copiedCSV.String())
	}

	binaryKeys := compoundKeysForDifferentBurrows("binary")
	binaryInput := binaryCOPYInput([]binaryCOPYRow{
		{tenantID: binaryKeys[0], region: "binary", payload: "binary-one"},
		{tenantID: binaryKeys[1], region: "binary", payload: "binary-two"},
	})
	tag, err = connection.CopyFrom(context.Background(), bytes.NewReader(binaryInput), `COPY shard_copy_e2e (tenant_id, region, payload) FROM STDIN WITH (FORMAT binary)`)
	if err != nil || tag.RowsAffected() != 2 {
		t.Fatalf("sharded binary COPY = %s, %v\ngateway logs:\n%s", tag, err, logs.String())
	}
	assertCopiedRowPlacement(t, binaryKeys[0], "binary", "binary-one")
	assertCopiedRowPlacement(t, binaryKeys[1], "binary", "binary-two")

	var copiedBinary bytes.Buffer
	tag, err = connection.CopyTo(context.Background(), &copiedBinary, `COPY shard_copy_e2e (tenant_id, region, payload) TO STDOUT WITH (FORMAT binary)`)
	if err != nil || tag.RowsAffected() != 6 || countBinaryCOPYRows(t, copiedBinary.Bytes()) != 6 {
		t.Fatalf("merged binary COPY TO = %s, rows %d, err %v\ngateway logs:\n%s", tag, countBinaryCOPYRows(t, copiedBinary.Bytes()), err, logs.String())
	}

	failureKey := compoundKeysForDifferentBurrows("failure")[0]
	badInput := fmt.Sprintf("%d\tfailure\tshould-rollback\n\\N\tfailure\tinvalid\n", failureKey)
	if _, err := connection.CopyFrom(context.Background(), strings.NewReader(badInput), "COPY shard_copy_e2e (tenant_id, region, payload) FROM STDIN"); err == nil {
		t.Fatal("COPY with a NULL shard key succeeded")
	}
	resynchronized := connection.ExecParams(context.Background(), "SELECT $1::int", [][]byte{[]byte("1")}, []uint32{23}, []int16{0}, []int16{0}).Read()
	if resynchronized.Err != nil || len(resynchronized.Rows) != 1 {
		t.Fatalf("extended protocol did not resynchronize after COPY failure: rows %d, err %v", len(resynchronized.Rows), resynchronized.Err)
	}
	assertCopiedRowAbsent(t, failureKey, "failure")

	constraintKeys := compoundKeysForDifferentBurrows("constraint")
	preexisting := fmt.Sprintf("INSERT INTO shard_copy_e2e (tenant_id, region, payload) VALUES (%d, 'constraint', 'preexisting')", constraintKeys[0])
	if _, err := connection.Exec(context.Background(), preexisting).ReadAll(); err != nil {
		t.Fatalf("seed COPY constraint failure: %v", err)
	}
	if _, err := connection.Exec(context.Background(), "BEGIN").ReadAll(); err != nil {
		t.Fatal(err)
	}
	constraintInput := fmt.Sprintf("%d\tconstraint\tduplicate\n%d\tconstraint\tshould-rollback\n", constraintKeys[0], constraintKeys[1])
	if _, err := connection.CopyFrom(context.Background(), strings.NewReader(constraintInput), "COPY shard_copy_e2e (tenant_id, region, payload) FROM STDIN"); err == nil {
		t.Fatal("COPY with a Burrow constraint failure succeeded")
	}
	if _, err := connection.Exec(context.Background(), "ROLLBACK").ReadAll(); err != nil {
		t.Fatalf("rollback after Burrow COPY failure: %v", err)
	}
	assertCopiedRowAbsent(t, constraintKeys[1], "constraint")
	assertCopiedRowPlacement(t, constraintKeys[0], "constraint", "preexisting")

	transactionKeys := compoundKeysForDifferentBurrows("transaction")
	if _, err := connection.Exec(context.Background(), "BEGIN").ReadAll(); err != nil {
		t.Fatal(err)
	}
	transactionInput := fmt.Sprintf("%d\ttransaction\ttx-one\n%d\ttransaction\ttx-two\n", transactionKeys[0], transactionKeys[1])
	if tag, err := connection.CopyFrom(context.Background(), strings.NewReader(transactionInput), "COPY shard_copy_e2e (tenant_id, region, payload) FROM STDIN"); err != nil || tag.RowsAffected() != 2 {
		t.Fatalf("transactional COPY = %s, %v", tag, err)
	}
	if _, err := connection.Exec(context.Background(), "COMMIT").ReadAll(); err != nil {
		t.Fatalf("commit transactional COPY: %v", err)
	}
	assertCopiedRowPlacement(t, transactionKeys[0], "transaction", "tx-one")
	assertCopiedRowPlacement(t, transactionKeys[1], "transaction", "tx-two")
}

func compoundKeysForDifferentBurrows(region string) [2]int64 {
	var keys [2]int64
	found := map[string]bool{}
	for key := int64(1); len(found) < 2; key++ {
		target := router.BurrowForKey(strconv.FormatInt(key, 10)+"\x00"+region, []string{"burrow-01", "burrow-02"})
		if found[target] {
			continue
		}
		found[target] = true
		if target == "burrow-01" {
			keys[0] = key
		} else {
			keys[1] = key
		}
	}
	return keys
}

func assertCopiedRowPlacement(t *testing.T, tenantID int64, region, payload string) {
	t.Helper()
	wantBurrow := router.BurrowForKey(strconv.FormatInt(tenantID, 10)+"\x00"+region, []string{"burrow-01", "burrow-02"})
	for index, port := range []string{"5541", "5542"} {
		connection, err := pgx.Connect(context.Background(), "postgres://hamster:hamster@localhost:"+port+"/hamstergres?sslmode=disable")
		if err != nil {
			t.Fatal(err)
		}
		var count int
		err = connection.QueryRow(context.Background(), "SELECT count(*) FROM shard_copy_e2e WHERE tenant_id = $1 AND region = $2 AND payload = $3", tenantID, region, payload).Scan(&count)
		connection.Close(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		want := 0
		if wantBurrow == fmt.Sprintf("burrow-%02d", index+1) {
			want = 1
		}
		if count != want {
			t.Fatalf("row (%d, %q) count on burrow-%02d = %d, want %d", tenantID, region, index+1, count, want)
		}
	}
}

func assertCopiedRowAbsent(t *testing.T, tenantID int64, region string) {
	t.Helper()
	for _, port := range []string{"5541", "5542"} {
		connection, err := pgx.Connect(context.Background(), "postgres://hamster:hamster@localhost:"+port+"/hamstergres?sslmode=disable")
		if err != nil {
			t.Fatal(err)
		}
		var count int
		err = connection.QueryRow(context.Background(), "SELECT count(*) FROM shard_copy_e2e WHERE tenant_id = $1 AND region = $2", tenantID, region).Scan(&count)
		connection.Close(context.Background())
		if err != nil || count != 0 {
			t.Fatalf("failed COPY row count on Burrow %s = %d, err %v", port, count, err)
		}
	}
}

type binaryCOPYRow struct {
	tenantID int64
	region   string
	payload  string
}

func binaryCOPYInput(rows []binaryCOPYRow) []byte {
	var output bytes.Buffer
	output.Write([]byte{'P', 'G', 'C', 'O', 'P', 'Y', '\n', 0xff, '\r', '\n', 0})
	_ = binary.Write(&output, binary.BigEndian, uint32(0))
	_ = binary.Write(&output, binary.BigEndian, uint32(0))
	for _, row := range rows {
		_ = binary.Write(&output, binary.BigEndian, int16(3))
		_ = binary.Write(&output, binary.BigEndian, int32(8))
		_ = binary.Write(&output, binary.BigEndian, row.tenantID)
		for _, value := range []string{row.region, row.payload} {
			_ = binary.Write(&output, binary.BigEndian, int32(len(value)))
			output.WriteString(value)
		}
	}
	_ = binary.Write(&output, binary.BigEndian, int16(-1))
	return output.Bytes()
}

func countBinaryCOPYRows(t *testing.T, data []byte) int {
	t.Helper()
	if len(data) < 21 || !bytes.Equal(data[:11], []byte{'P', 'G', 'C', 'O', 'P', 'Y', '\n', 0xff, '\r', '\n', 0}) {
		t.Fatalf("invalid merged binary COPY header")
	}
	position := 19 + int(binary.BigEndian.Uint32(data[15:19]))
	rows := 0
	for position+2 <= len(data) {
		columns := int(int16(binary.BigEndian.Uint16(data[position : position+2])))
		position += 2
		if columns == -1 {
			if position != len(data) {
				t.Fatalf("binary COPY has %d bytes after trailer", len(data)-position)
			}
			return rows
		}
		if columns < 0 {
			t.Fatalf("invalid binary COPY column count %d", columns)
		}
		for column := 0; column < columns; column++ {
			if position+4 > len(data) {
				t.Fatal("truncated binary COPY field length")
			}
			length := int(int32(binary.BigEndian.Uint32(data[position : position+4])))
			position += 4
			if length >= 0 {
				position += length
			}
			if position > len(data) {
				t.Fatal("truncated binary COPY field")
			}
		}
		rows++
	}
	t.Fatal("binary COPY trailer is missing")
	return 0
}

func TestCrossBurrowTransactionUsesTwoPhaseCommit(t *testing.T) {
	repoRoot := repositoryRoot(t)
	ensureDockerBurrows(t, repoRoot)

	frontendAddress := availableAddress(t)
	statusAddress := availableAddress(t)
	binary := buildGateway(t, repoRoot)
	configPath := writeGatewayConfig(t, frontendAddress, statusAddress)
	logs := startGateway(t, binary, configPath)
	waitForHealthyGateway(t, "http://"+statusAddress, logs)

	config, err := pgx.ParseConfig("postgres://any-user@" + frontendAddress + "/any-database?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	config.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	connection, err := pgx.ConnectConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close(context.Background())

	if _, err := connection.Exec(context.Background(), "DROP TABLE IF EXISTS two_pc_e2e"); err != nil {
		t.Fatalf("drop 2PC table: %v", err)
	}
	if _, err := connection.Exec(context.Background(), "CREATE TABLE two_pc_e2e (id bigint PRIMARY KEY, value text); COMMENT ON COLUMN two_pc_e2e.id IS 'hamstergres.shard_key'"); err != nil {
		t.Fatalf("prepare 2PC table: %v", err)
	}
	keys := keysForDifferentBurrows()
	transaction, err := connection.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for index, key := range keys {
		if _, err := transaction.Exec(context.Background(), "INSERT INTO two_pc_e2e (id, value) VALUES ($1, $2)", key, fmt.Sprintf("value-%d", index)); err != nil {
			_ = transaction.Rollback(context.Background())
			t.Fatalf("cross-Burrow insert: %v\ngateway logs:\n%s", err, logs.String())
		}
	}
	if err := transaction.Commit(context.Background()); err != nil {
		t.Fatalf("two-phase commit: %v\ngateway logs:\n%s", err, logs.String())
	}
	rows, err := connection.Query(context.Background(), "SELECT id FROM two_pc_e2e")
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for rows.Next() {
		count++
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("committed row count = %d, want 2", count)
	}
}

func TestCrossBurrowTransactionCanDisableTwoPhaseCommit(t *testing.T) {
	repoRoot := repositoryRoot(t)
	ensureDockerBurrows(t, repoRoot)
	frontendAddress, statusAddress := availableAddress(t), availableAddress(t)
	binary := buildGateway(t, repoRoot)
	configPath := writeGatewayConfig(t, frontendAddress, statusAddress)
	contents, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	contents = append(contents, []byte("transactions:\n  two_phase_commit: false\n")...)
	if err := os.WriteFile(configPath, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	logs := startGateway(t, binary, configPath)
	statusURL := "http://" + statusAddress
	waitForHealthyGateway(t, statusURL, logs)
	assertStructuredLogEvent(t, logs.String(), "two_phase_commit_disabled", "configuration")

	config, err := pgx.ParseConfig("postgres://any-user@" + frontendAddress + "/any-database?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	config.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	connection, err := pgx.ConnectConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close(context.Background())
	if _, err := connection.Exec(context.Background(), "DROP TABLE IF EXISTS no_two_pc_e2e"); err != nil {
		t.Fatal(err)
	}
	if _, err := connection.Exec(context.Background(), "CREATE TABLE no_two_pc_e2e (id bigint PRIMARY KEY, value text); COMMENT ON COLUMN no_two_pc_e2e.id IS 'hamstergres.shard_key'"); err != nil {
		t.Fatal(err)
	}
	transaction, err := connection.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for index, key := range keysForDifferentBurrows() {
		if _, err := transaction.Exec(context.Background(), "INSERT INTO no_two_pc_e2e (id, value) VALUES ($1, $2)", key, fmt.Sprintf("value-%d", index)); err != nil {
			_ = transaction.Rollback(context.Background())
			t.Fatal(err)
		}
	}
	if err := transaction.Commit(context.Background()); err != nil {
		t.Fatalf("best-effort cross-Burrow commit: %v\ngateway logs:\n%s", err, logs.String())
	}
	for _, operation := range gatewaySnapshot(t, statusURL+"/api/v1/status").QueryMetrics.Operations {
		if operation.Operation == "two_phase_commit" {
			t.Fatalf("disabled two-phase commit recorded operation %#v", operation)
		}
	}
}

func TestFrontendDisconnectCancelsBlockedBurrowTransaction(t *testing.T) {
	repoRoot := repositoryRoot(t)
	ensureDockerBurrows(t, repoRoot)

	frontendAddress := availableAddress(t)
	statusAddress := availableAddress(t)
	binary := buildGateway(t, repoRoot)
	configPath := writeGatewayConfig(t, frontendAddress, statusAddress)
	logs := startGateway(t, binary, configPath)
	statusURL := "http://" + statusAddress
	waitForHealthyGateway(t, statusURL, logs)

	queryGateway(t, frontendAddress, "DROP TABLE IF EXISTS disconnect_e2e", func(rows pgx.Rows) {})
	queryGateway(t, frontendAddress, "CREATE TABLE disconnect_e2e (id bigint PRIMARY KEY, value bigint)", func(rows pgx.Rows) {})
	queryGateway(t, frontendAddress, "COMMENT ON COLUMN disconnect_e2e.id IS 'hamstergres.shard_key'", func(rows pgx.Rows) {})
	queryGateway(t, frontendAddress, "INSERT INTO disconnect_e2e (id, value) VALUES (1, 0)", func(rows pgx.Rows) {})
	target := router.BurrowForKey("1", []string{"burrow-01", "burrow-02"})
	port := "5541"
	if target == "burrow-02" {
		port = "5542"
	}
	blocker, err := pgx.Connect(context.Background(), "postgres://hamster:hamster@localhost:"+port+"/hamstergres?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close(context.Background())
	blockerTx, err := blocker.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer blockerTx.Rollback(context.Background())
	if _, err := blockerTx.Exec(context.Background(), "UPDATE disconnect_e2e SET value = value + 1 WHERE id = 1"); err != nil {
		t.Fatal(err)
	}

	client, err := pgx.Connect(context.Background(), "postgres://any-user@"+frontendAddress+"/any-database?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Exec(context.Background(), "BEGIN"); err != nil {
		t.Fatal(err)
	}
	blocked := make(chan error, 1)
	go func() {
		_, err := client.Exec(context.Background(), "UPDATE disconnect_e2e SET value = value + 1 WHERE id = $1", int64(1))
		blocked <- err
	}()
	time.Sleep(200 * time.Millisecond)
	select {
	case err := <-blocked:
		t.Fatalf("transactional update was not blocked before disconnect: %v", err)
	default:
	}
	if err := client.PgConn().Conn().Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-blocked:
	case <-time.After(5 * time.Second):
		t.Fatalf("blocked frontend query did not stop after disconnect\ngateway logs:\n%s", logs.String())
	}
	if err := blockerTx.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		snapshot := gatewaySnapshot(t, statusURL+"/api/v1/status")
		if snapshot.Frontend.ActiveConnections == 0 && activeApplicationTransactions(t) == 0 {
			queryGateway(t, frontendAddress, "SELECT 1", func(rows pgx.Rows) {})
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("frontend or Burrow transaction remained active after disconnect\ngateway logs:\n%s", logs.String())
}

func activeApplicationTransactions(t *testing.T) int {
	t.Helper()
	total := 0
	for _, port := range []string{"5541", "5542"} {
		connection, err := pgx.Connect(context.Background(), "postgres://hamster:hamster@localhost:"+port+"/hamstergres?sslmode=disable")
		if err != nil {
			t.Fatal(err)
		}
		var count int
		err = connection.QueryRow(context.Background(), "SELECT count(*) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND usename = 'hamster' AND xact_start IS NOT NULL").Scan(&count)
		connection.Close(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		total += count
	}
	return total
}

func keysForDifferentBurrows() [2]int64 {
	burrows := []string{"burrow-01", "burrow-02"}
	var keys [2]int64
	found := make(map[string]bool)
	for key := int64(1); len(found) < len(burrows); key++ {
		name := router.BurrowForKey(strconv.FormatInt(key, 10), burrows)
		if found[name] {
			continue
		}
		found[name] = true
		if name == burrows[0] {
			keys[0] = key
		} else {
			keys[1] = key
		}
	}
	return keys
}

func TestGeneratedIDsAcrossConcurrentProxies(t *testing.T) {
	repoRoot := repositoryRoot(t)
	ensureDockerBurrows(t, repoRoot)
	prepareGeneratedIDTable(t)

	key := fmt.Sprintf("generated-id-%d", time.Now().UnixNano())
	frontendAddresses := []string{availableAddress(t), availableAddress(t)}
	for _, frontendAddress := range frontendAddresses {
		statusAddress := availableAddress(t)
		binary := buildGateway(t, repoRoot)
		configPath := writeGatewayConfigWithKey(t, frontendAddress, statusAddress, key)
		logs := startGateway(t, binary, configPath)
		waitForHealthyGateway(t, "http://"+statusAddress, logs)
	}

	const inserts = 32
	ids := make(chan int64, inserts)
	errors := make(chan error, inserts)
	var wait sync.WaitGroup
	for index := 0; index < inserts; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			id, err := insertGeneratedID(frontendAddresses[index%len(frontendAddresses)], index%2 == 0, index)
			if err != nil {
				errors <- err
				return
			}
			ids <- id
		}(index)
	}
	wait.Wait()
	close(ids)
	close(errors)
	for err := range errors {
		t.Errorf("generated insert: %v", err)
	}

	seen := make(map[int64]bool, inserts)
	for id := range ids {
		if seen[id] {
			t.Fatalf("generated ID %d was allocated more than once", id)
		}
		seen[id] = true
	}
	if len(seen) != inserts {
		t.Fatalf("generated IDs = %#v, want %d unique values", seen, inserts)
	}
	for id := int64(1); id <= inserts; id++ {
		if !seen[id] {
			t.Fatalf("generated IDs = %#v, missing %d", seen, id)
		}
	}
}

func prepareGeneratedIDTable(t *testing.T) {
	t.Helper()
	dsns := []string{
		"postgres://hamster:hamster@localhost:5541/hamstergres?sslmode=disable",
		"postgres://hamster:hamster@localhost:5542/hamstergres?sslmode=disable",
	}
	for _, dsn := range dsns {
		connection, err := pgx.Connect(context.Background(), dsn)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := connection.Exec(context.Background(), "DROP TABLE IF EXISTS generated_id_e2e"); err != nil {
			t.Fatal(err)
		}
		if _, err := connection.Exec(context.Background(), "CREATE TABLE generated_id_e2e (id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, payload INTEGER NOT NULL)"); err != nil {
			t.Fatal(err)
		}
		if _, err := connection.Exec(context.Background(), "COMMENT ON COLUMN generated_id_e2e.id IS 'hamstergres.shard_key'"); err != nil {
			t.Fatal(err)
		}
		connection.Close(context.Background())
	}
	t.Cleanup(func() {
		for _, dsn := range dsns {
			connection, err := pgx.Connect(context.Background(), dsn)
			if err != nil {
				t.Log(err)
				continue
			}
			if _, err := connection.Exec(context.Background(), "DROP TABLE IF EXISTS generated_id_e2e"); err != nil {
				t.Log(err)
			}
			connection.Close(context.Background())
		}
	})
}

func insertGeneratedID(address string, simple bool, payload int) (int64, error) {
	config, err := pgx.ParseConfig("postgres://any-user@" + address + "/any-database?sslmode=disable")
	if err != nil {
		return 0, err
	}
	if simple {
		config.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	}
	connection, err := pgx.ConnectConfig(context.Background(), config)
	if err != nil {
		return 0, err
	}
	defer connection.Close(context.Background())
	var id int64
	err = connection.QueryRow(context.Background(), "INSERT INTO generated_id_e2e (payload) VALUES ($1) RETURNING id", payload).Scan(&id)
	return id, err
}

const sysbenchVersion = "1.0.20"

func ensurePgbench(t *testing.T) string {
	t.Helper()
	path, err := exec.LookPath("pgbench")
	if err != nil {
		t.Skip("pgbench is required for the initialization compatibility test")
	}
	return path
}

func runPgbench(path, address string, options ...string) (string, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return "", err
	}
	arguments := append([]string(nil), options...)
	arguments = append(arguments, "-h", host, "-p", port, "-U", "hamster", "hamstergres")
	command := exec.Command(path, arguments...)
	command.Env = append(os.Environ(), "PGPASSWORD=hamster")
	output, err := command.CombinedOutput()
	return string(output), err
}

func runPgbenchWorkload(repositoryRoot, address, mode, action string, options ...string) (string, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return "", err
	}
	arguments := append([]string{mode, action}, options...)
	command := exec.Command(filepath.Join(repositoryRoot, "scripts", "pgbench-workload.sh"), arguments...)
	command.Env = append(os.Environ(),
		"HAMSTERGRES_BENCHMARK_HOST="+host,
		"HAMSTERGRES_BENCHMARK_PORT="+port,
		"HAMSTERGRES_BENCHMARK_USER=hamster",
		"HAMSTERGRES_BENCHMARK_PASSWORD=hamster",
		"HAMSTERGRES_BENCHMARK_DATABASE=hamstergres",
	)
	output, err := command.CombinedOutput()
	return string(output), err
}

func execGateway(address, sql string) error {
	config, err := pgx.ParseConfig("postgres://any-user@" + address + "/any-database?sslmode=disable")
	if err != nil {
		return err
	}
	config.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	connection, err := pgx.ConnectConfig(context.Background(), config)
	if err != nil {
		return err
	}
	defer connection.Close(context.Background())
	_, err = connection.Exec(context.Background(), sql)
	return err
}

func assertNestInventoryMatches(t *testing.T, key string, live []schema.TableInventory) {
	t.Helper()
	body, err := json.Marshal(map[string]string{"key": base64.StdEncoding.EncodeToString([]byte(key))})
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.Post("http://127.0.0.1:2379/v3/kv/range", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		contents, _ := io.ReadAll(response.Body)
		t.Fatalf("read Nest schema registry = %s: %s", response.Status, contents)
	}
	var result struct {
		KVs []struct {
			Value string `json:"value"`
		} `json:"kvs"`
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if len(result.KVs) != 1 {
		t.Fatalf("Nest schema registry %q values = %d, want 1", key, len(result.KVs))
	}
	encoded, err := base64.StdEncoding.DecodeString(result.KVs[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := schema.FromJSON(encoded)
	if err != nil {
		t.Fatal(err)
	}
	nestInventory, err := json.Marshal(registry.Inventory())
	if err != nil {
		t.Fatal(err)
	}
	liveInventory, err := json.Marshal(live)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(nestInventory, liveInventory) {
		t.Fatalf("Nest inventory = %s, live inventory = %s", nestInventory, liveInventory)
	}
}

func assertNestTopologyPlacement(t *testing.T, key, table string) {
	t.Helper()
	stored, found, err := nest.NewTopologyStore("http://127.0.0.1:2379", key).Get(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatalf("Nest topology %q is missing", key)
	}
	placements, err := stored.Catalog.TablePlacements()
	if err != nil {
		t.Fatal(err)
	}
	owners := placements[table]
	if len(owners) != router.VirtualShards {
		t.Fatalf("topology table %s has %d owners, want %d", table, len(owners), router.VirtualShards)
	}
	if owners[0] != "burrow-02" || owners[1] != "burrow-01" {
		t.Fatalf("topology did not preserve v3 modulo placement: %#v", owners[:2])
	}
}

func readNestValue(t *testing.T, key string) []byte {
	t.Helper()
	body, err := json.Marshal(map[string]string{"key": base64.StdEncoding.EncodeToString([]byte(key))})
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.Post("http://127.0.0.1:2379/v3/kv/range", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var result struct {
		KVs []struct {
			Value string `json:"value"`
		} `json:"kvs"`
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || len(result.KVs) != 1 {
		t.Fatalf("read Nest key %q: status=%s values=%d", key, response.Status, len(result.KVs))
	}
	value, err := base64.StdEncoding.DecodeString(result.KVs[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func putNestValue(t *testing.T, key string, value []byte) {
	t.Helper()
	body, err := json.Marshal(map[string]string{
		"key":   base64.StdEncoding.EncodeToString([]byte(key)),
		"value": base64.StdEncoding.EncodeToString(value),
	})
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.Post("http://127.0.0.1:2379/v3/kv/put", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		contents, _ := io.ReadAll(response.Body)
		t.Fatalf("write Nest key %q: %s: %s", key, response.Status, contents)
	}
}

// TestSysbenchReadWriteEndToEnd is an opt-in compatibility test. It exercises
// sysbench's PostgreSQL extended-query workload against the Docker Burrows;
// the default Go test suite stays read-only and fast for everyday development.
func TestSysbenchReadWriteEndToEnd(t *testing.T) {
	if os.Getenv("HAMSTERGRES_SYSBENCH_E2E") != "1" {
		t.Skip("set HAMSTERGRES_SYSBENCH_E2E=1 to run the local sysbench compatibility test")
	}

	repoRoot := repositoryRoot(t)
	ensureDockerBurrows(t, repoRoot)
	sysbench := ensureSysbench(t)

	frontendAddress := availableAddress(t)
	statusAddress := availableAddress(t)
	binary := buildGateway(t, repoRoot)
	configPath := writeGatewayConfig(t, frontendAddress, statusAddress)
	logs := startGateway(t, binary, configPath)
	statusURL := "http://" + statusAddress
	waitForHealthyGateway(t, statusURL, logs)

	for _, mode := range []string{"unsharded", "sharded"} {
		t.Run(mode, func(t *testing.T) {
			cleanupNeeded := true
			cleanup := func() error {
				output, err := runSysbench(sysbench, repoRoot, frontendAddress, mode, "cleanup")
				if err != nil {
					return fmt.Errorf("sysbench %s cleanup: %w\n%s", mode, err, output)
				}
				cleanupNeeded = false
				return nil
			}
			t.Cleanup(func() {
				if cleanupNeeded {
					if err := cleanup(); err != nil {
						t.Log(err)
					}
				}
			})

			if output, err := runSysbench(sysbench, repoRoot, frontendAddress, mode, "prepare"); err != nil {
				t.Fatalf("sysbench %s prepare: %v\n%s", mode, err, output)
			}
			assertSysbenchDistribution(t, mode)
			if output, err := runSysbench(sysbench, repoRoot, frontendAddress, mode, "run"); err != nil {
				t.Fatalf("sysbench %s oltp_read_write workload: %v\n%s", mode, err, output)
			}
			if err := cleanup(); err != nil {
				t.Fatal(err)
			}
		})
	}

	snapshot := gatewaySnapshot(t, statusURL+"/api/v1/status")
	if snapshot.QueryMetrics.Total.Queries == 0 || snapshot.QueryMetrics.Total.FailedQueries != 0 {
		t.Fatalf("sysbench query counters = %#v, want successful queries", snapshot.QueryMetrics.Total)
	}
	if snapshot.QueryMetrics.Total.SingleShardQueries == 0 {
		t.Fatalf("sysbench routing counters = %#v, want keyed reads and writes routed to one Burrow", snapshot.QueryMetrics.Total)
	}
	if snapshot.QueryMetrics.Total.ScatteredQueries == 0 {
		t.Fatalf("sysbench routing counters = %#v, want schema commands scattered to both Burrows", snapshot.QueryMetrics.Total)
	}
	assertStatement(t, snapshot.QueryMetrics.QuerySummaries, "SELECT")
	assertStatement(t, snapshot.QueryMetrics.QuerySummaries, "UPDATE")
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	workingDirectory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Dir(workingDirectory)
}

func ensureDockerBurrows(t *testing.T, repositoryRoot string) {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("Docker is required for the end-to-end gateway test")
	}
	command := exec.Command("docker", "compose", "up", "-d", "--wait", "hamstergres-nest", "burrow-01", "burrow-02")
	command.Dir = repositoryRoot
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("start Docker Burrows: %v\n%s", err, output)
	}
}

func availableAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	return listener.Addr().String()
}

func buildGateway(t *testing.T, repositoryRoot string) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "hamstergres-proxy")
	command := exec.Command("go", "build", "-o", binary, "./cmd/hamstergres-proxy")
	command.Dir = repositoryRoot
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build gateway: %v\n%s", err, output)
	}
	return binary
}

func writeGatewayConfig(t *testing.T, frontendAddress, statusAddress string) string {
	t.Helper()
	testKey := gatewayTestKey(frontendAddress)
	return writeGatewayConfigWithKey(t, frontendAddress, statusAddress, testKey)
}

func gatewayTestKey(frontendAddress string) string {
	return strings.NewReplacer(":", "-", ".", "-").Replace(frontendAddress)
}

func addConfiguredBurrow(t *testing.T, configPath, name, dsn string) {
	t.Helper()
	contents, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	marker := "  physical_shards:\n"
	replacement := marker + fmt.Sprintf("    %s:\n      dsn: %q\n", name, dsn)
	updated := strings.Replace(string(contents), marker, replacement, 1)
	if updated == string(contents) {
		t.Fatal("physical_shards marker is missing")
	}
	if err := os.WriteFile(configPath, []byte(updated), 0o600); err != nil {
		t.Fatal(err)
	}
}

func removeConfiguredBurrow(t *testing.T, configPath, name string) {
	t.Helper()
	contents, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(string(contents), "\n")
	filtered := make([]string, 0, len(lines))
	skip := false
	for _, line := range lines {
		if line == "    "+name+":" {
			skip = true
			continue
		}
		if skip && strings.HasPrefix(line, "      ") {
			continue
		}
		skip = false
		filtered = append(filtered, line)
	}
	if err := os.WriteFile(configPath, []byte(strings.Join(filtered, "\n")), 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeGatewayConfigWithKey(t *testing.T, frontendAddress, statusAddress, testKey string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "gateway.yaml")
	contents := fmt.Sprintf(`listen:
  address: %q
status:
  address: %q
nest:
  endpoint: "http://127.0.0.1:2379"
  registry_key: "/hamstergres/tests/%s/schema-registry"
  topology_key: "/hamstergres/tests/%s/topology"
  sequence_key: "/hamstergres/tests/%s/global-id"
sharding:
  physical_shards:
    burrow-01:
      dsn: "postgres://hamster:hamster@localhost:5541/hamstergres?sslmode=disable"
    burrow-02:
      dsn: "postgres://hamster:hamster@localhost:5542/hamstergres?sslmode=disable"
`, frontendAddress, statusAddress, testKey, testKey, testKey)
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := nest.NewMaintenanceClient("http://127.0.0.1:2379").DeleteTestNamespace(ctx, testKey); err != nil {
			t.Errorf("clean Nest test namespace %q: %v", testKey, err)
		}
	})
	return path
}

type synchronizedBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *synchronizedBuffer) Write(contents []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(contents)
}
func (b *synchronizedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

func startGateway(t *testing.T, binary, configPath string) *synchronizedBuffer {
	return startGatewayWithEnv(t, binary, configPath, nil)
}

func startGatewayWithEnv(t *testing.T, binary, configPath string, environment []string) *synchronizedBuffer {
	t.Helper()
	logs := &synchronizedBuffer{}
	command := exec.Command(binary, "-config", configPath)
	command.Env = append(os.Environ(), environment...)
	command.Stdout = logs
	command.Stderr = logs
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if command.ProcessState == nil || !command.ProcessState.Exited() {
			_ = command.Process.Signal(os.Interrupt)
			_, _ = command.Process.Wait()
		}
	})
	return logs
}

func waitForHealthyGateway(t *testing.T, statusURL string, logs *synchronizedBuffer) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		response, err := http.Get(statusURL + "/healthz")
		if err == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("gateway did not become healthy:\n%s", logs.String())
}

func queryGateway(t *testing.T, address, sql string, assert func(pgx.Rows)) {
	t.Helper()
	config, err := pgx.ParseConfig("postgres://any-user@" + address + "/any-database?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	config.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	connection, err := pgx.ConnectConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close(context.Background())
	rows, err := connection.Query(context.Background(), sql)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	assert(rows)
}

func queryGatewayError(t *testing.T, address, sql, code string) {
	t.Helper()
	config, err := pgx.ParseConfig("postgres://any-user@" + address + "/any-database?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	config.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	connection, err := pgx.ConnectConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close(context.Background())
	_, err = connection.Exec(context.Background(), sql)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != code {
		t.Fatalf("query error = %v, want PostgreSQL code %s", err, code)
	}
}

func assertConcurrentQueriesAndMetricScrapes(t *testing.T, address, metricsURL string) {
	t.Helper()
	var wg sync.WaitGroup
	errorsFound := make(chan error, 40)
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				response, err := http.Get(metricsURL)
				if err != nil {
					errorsFound <- err
					continue
				}
				_, readErr := io.Copy(io.Discard, response.Body)
				response.Body.Close()
				if readErr != nil || response.StatusCode != http.StatusOK {
					errorsFound <- fmt.Errorf("metrics scrape: status=%s read=%v", response.Status, readErr)
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			config, err := pgx.ParseConfig("postgres://any-user@" + address + "/any-database?sslmode=disable")
			if err != nil {
				errorsFound <- err
				return
			}
			config.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
			connection, err := pgx.ConnectConfig(context.Background(), config)
			if err != nil {
				errorsFound <- err
				continue
			}
			_, err = connection.Exec(context.Background(), "SELECT 1")
			connection.Close(context.Background())
			if err != nil {
				errorsFound <- err
			}
		}
	}()
	wg.Wait()
	close(errorsFound)
	for err := range errorsFound {
		t.Error(err)
	}
}

func gatewaySnapshot(t *testing.T, endpoint string) status.Snapshot {
	t.Helper()
	response, err := http.Get(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status response = %s", response.Status)
	}
	var snapshot status.Snapshot
	if err := json.NewDecoder(response.Body).Decode(&snapshot); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func getBody(t *testing.T, endpoint string) string {
	t.Helper()
	response, err := http.Get(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("GET %s = %s", endpoint, response.Status)
	}
	contents, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(contents)
}

func assertTotalShardExecutions(t *testing.T, executions []statistics.ShardCount, want int64) {
	t.Helper()
	var got int64
	for _, execution := range executions {
		got += execution.Queries
	}
	if got != want {
		t.Fatalf("total Burrow executions = %d, want %d: %#v", got, want, executions)
	}
}

func assertSummary(t *testing.T, summaries []statistics.QuerySummary, shape string) {
	t.Helper()
	for _, summary := range summaries {
		if summary.QueryShape == shape {
			if summary.Fingerprint != statistics.Fingerprint(shape) {
				t.Fatalf("fingerprint for %q = %q", shape, summary.Fingerprint)
			}
			return
		}
	}
	t.Fatalf("query shape %q was not summarized: %#v", shape, summaries)
}

func assertStatement(t *testing.T, summaries []statistics.QuerySummary, statement string) {
	t.Helper()
	for _, summary := range summaries {
		if summary.Statement == statement && summary.Statistics.Queries > 0 {
			return
		}
	}
	t.Fatalf("%s was not recorded in query summaries: %#v", statement, summaries)
}

func ensureSysbench(t *testing.T) string {
	t.Helper()
	sysbench, err := exec.LookPath("sysbench")
	if err != nil {
		t.Fatalf("sysbench %s is required; install it with `brew install sysbench`", sysbenchVersion)
	}
	output, err := exec.Command(sysbench, "--version").CombinedOutput()
	if err != nil {
		t.Fatalf("read sysbench version: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), "sysbench "+sysbenchVersion) {
		t.Fatalf("sysbench version = %q, want sysbench %s", strings.TrimSpace(string(output)), sysbenchVersion)
	}
	return sysbench
}

func runSysbench(sysbench, repositoryRoot, frontendAddress, mode, action string) (string, error) {
	_, port, err := net.SplitHostPort(frontendAddress)
	if err != nil {
		return "", err
	}
	context, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	sharding := "off"
	if mode == "sharded" {
		sharding = "on"
	}
	command := exec.CommandContext(context, sysbench,
		"--db-driver=pgsql",
		// Hamstergres requires explicit client-generated keys. The custom
		// workload also makes sharded preparation use one keyed INSERT per row.
		"--auto_inc=off",
		"--pgsql-host=127.0.0.1",
		"--pgsql-port="+port,
		"--pgsql-user=hamster",
		"--pgsql-password=hamster",
		"--pgsql-db=hamstergres",
		"--tables=2",
		"--table-size=1000",
		"--threads=4",
		"--time=3",
		"--events=0",
		"--report-interval=0",
		"--rand-seed=1",
		"--hamstergres-sharding="+sharding,
		filepath.Join(repositoryRoot, "scripts", "sysbench-oltp-read-write.lua"),
		action,
	)
	output, err := command.CombinedOutput()
	return string(output), err
}

func assertSysbenchDistribution(t *testing.T, mode string) {
	t.Helper()
	for table := 1; table <= 2; table++ {
		counts := make([]int, 0, 2)
		comments := make([]string, 0, 2)
		for _, port := range []string{"5541", "5542"} {
			connection, err := pgx.Connect(context.Background(), "postgres://hamster:hamster@localhost:"+port+"/hamstergres?sslmode=disable")
			if err != nil {
				t.Fatal(err)
			}
			var count int
			var comment *string
			name := fmt.Sprintf("sbtest%d", table)
			if err := connection.QueryRow(context.Background(), fmt.Sprintf(
				"SELECT count(*), col_description('%s'::regclass, 1) FROM %s", name, name)).Scan(&count, &comment); err != nil {
				connection.Close(context.Background())
				t.Fatal(err)
			}
			connection.Close(context.Background())
			counts = append(counts, count)
			if comment == nil {
				comments = append(comments, "")
			} else {
				comments = append(comments, *comment)
			}
		}
		if mode == "sharded" {
			if counts[0] == 0 || counts[1] == 0 || counts[0]+counts[1] != 1000 {
				t.Fatalf("%s sharded row counts = %v, want 1000 distributed across both Burrows", fmt.Sprintf("sbtest%d", table), counts)
			}
			if comments[0] != "hamstergres.shard_key" || comments[1] != "hamstergres.shard_key" {
				t.Fatalf("sbtest%d shard-key comments = %v", table, comments)
			}
		} else if counts[0] != 1000 || counts[1] != 0 || comments[0] != "" || comments[1] != "" {
			t.Fatalf("sbtest%d unsharded distribution/comments = %v/%v", table, counts, comments)
		}
	}
}
