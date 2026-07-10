// Package integration tests the compiled gateway against the Docker Burrow environment.
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

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
		if len(values) != 2 || values[0] != 1 || values[1] != 1 {
			t.Fatalf("merged values = %#v, want [1 1] from both Burrows", values)
		}
	})
	queryGateway(t, frontendAddress, "SELECT * FROM accounts WHERE tenant_id = 1", func(rows pgx.Rows) {
		for rows.Next() {
			// The local data set may be empty or pre-existing; successful protocol
			// execution and the recorded normalized shape are what this checks.
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
	})

	snapshot := gatewaySnapshot(t, statusURL+"/api/v1/status")
	if snapshot.Queries.Queries != 2 || snapshot.Queries.FailedQueries != 0 {
		t.Fatalf("query counters = %#v, want two successful queries", snapshot.Queries)
	}
	if snapshot.QueryMetrics.Total.ScatteredQueries != 2 || snapshot.QueryMetrics.Total.SingleShardQueries != 0 {
		t.Fatalf("routing counters = %#v, want two scattered queries", snapshot.QueryMetrics.Total)
	}
	assertShardExecutions(t, snapshot.QueryMetrics.Total, snapshot.QueryMetrics.ShardExecutions, 2)
	assertSummary(t, snapshot.QueryMetrics.QuerySummaries, "SELECT ? AS value")
	assertSummary(t, snapshot.QueryMetrics.QuerySummaries, "SELECT * FROM accounts WHERE tenant_id = ?")

	page := getBody(t, statusURL+"/")
	if !strings.Contains(page, "SELECT * FROM accounts WHERE tenant_id = ?") {
		t.Fatalf("status page did not render the parameterized query shape:\n%s", page)
	}
	if !strings.Contains(page, statistics.Fingerprint("SELECT ? AS value")) {
		t.Fatalf("status page did not render a query fingerprint:\n%s", page)
	}

	command := exec.Command(binary, "status", "--status-url", statusURL+"/api/v1/status")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("status CLI failed: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), "Routing: 2 scattered / 0 single-shard") || !strings.Contains(string(output), statistics.Fingerprint("SELECT * FROM accounts WHERE tenant_id = ?")) {
		t.Fatalf("status CLI output did not contain routing and fingerprint data:\n%s", output)
	}
}

const sysbenchVersion = "1.0.20"

// TestSysbenchReadWriteEndToEnd is an opt-in compatibility test. Sysbench uses
// the PostgreSQL extended-query protocol, which the current Proxy deliberately
// does not implement. Keep the test failing when explicitly requested so the
// remaining sysbench compatibility work has one repeatable acceptance check,
// while the default Go test suite remains usable during development.
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

	cleanupNeeded := true
	cleanup := func() error {
		output, err := runSysbench(sysbench, frontendAddress, "cleanup")
		if err != nil {
			return fmt.Errorf("sysbench cleanup: %w\n%s", err, output)
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

	if output, err := runSysbench(sysbench, frontendAddress, "prepare"); err != nil {
		t.Fatalf("sysbench prepare: %v\n%s", err, output)
	}
	if output, err := runSysbench(sysbench, frontendAddress, "run"); err != nil {
		t.Fatalf("sysbench oltp_read_write workload: %v\n%s", err, output)
	}
	if err := cleanup(); err != nil {
		t.Fatal(err)
	}

	snapshot := gatewaySnapshot(t, statusURL+"/api/v1/status")
	if snapshot.QueryMetrics.Total.Queries == 0 || snapshot.QueryMetrics.Total.FailedQueries != 0 {
		t.Fatalf("sysbench query counters = %#v, want successful queries", snapshot.QueryMetrics.Total)
	}
	if snapshot.QueryMetrics.Total.ScatteredQueries != snapshot.QueryMetrics.Total.Queries || snapshot.QueryMetrics.Total.SingleShardQueries != 0 {
		t.Fatalf("sysbench routing counters = %#v, want every query scattered", snapshot.QueryMetrics.Total)
	}
	assertShardExecutions(t, snapshot.QueryMetrics.Total, snapshot.QueryMetrics.ShardExecutions, snapshot.QueryMetrics.Total.Queries)
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
	command := exec.Command("docker", "compose", "up", "-d", "--wait")
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
	path := filepath.Join(t.TempDir(), "gateway.yaml")
	contents := fmt.Sprintf(`listen:
  address: %q
status:
  address: %q
sharding:
  physical_shards:
    burrow-01:
      dsn: "postgres://hamster:hamster@localhost:5541/hamstergres?sslmode=disable"
    burrow-02:
      dsn: "postgres://hamster:hamster@localhost:5542/hamstergres?sslmode=disable"
`, frontendAddress, statusAddress)
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func startGateway(t *testing.T, binary, configPath string) *bytes.Buffer {
	t.Helper()
	logs := &bytes.Buffer{}
	command := exec.Command(binary, "-config", configPath)
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

func waitForHealthyGateway(t *testing.T, statusURL string, logs *bytes.Buffer) {
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

func assertShardExecutions(t *testing.T, totals statistics.Statistics, executions []statistics.ShardCount, want int64) {
	t.Helper()
	if totals.Queries != want || len(executions) != 2 {
		t.Fatalf("shard execution totals = %#v, executions = %#v", totals, executions)
	}
	for _, execution := range executions {
		if execution.Queries != want {
			t.Fatalf("shard %s executions = %d, want %d", execution.Name, execution.Queries, want)
		}
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

func runSysbench(sysbench, frontendAddress, action string) (string, error) {
	_, port, err := net.SplitHostPort(frontendAddress)
	if err != nil {
		return "", err
	}
	context, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	command := exec.CommandContext(context, sysbench,
		"--db-driver=pgsql",
		"--pgsql-host=127.0.0.1",
		"--pgsql-port="+port,
		"--pgsql-user=hamster",
		"--pgsql-password=hamster",
		"--pgsql-db=hamstergres",
		"--tables=2",
		"--table-size=10",
		"--threads=2",
		"--time=3",
		"--events=0",
		"--report-interval=0",
		"--rand-seed=1",
		"oltp_read_write",
		action,
	)
	output, err := command.CombinedOutput()
	return string(output), err
}
