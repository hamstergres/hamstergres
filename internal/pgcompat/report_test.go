// SPDX-License-Identifier: AGPL-3.0-only

package pgcompat

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestParseTAPAndValidateCompleteness(t *testing.T) {
	log := `# using postmaster on Unix socket
    ok 1      + boolean                                101 ms
not ok 2      + char                                   202 ms
    ok 3      - select                                 303 ms
1..3
`
	results, err := ParseTAP(strings.NewReader(log), "17.10", time.Unix(123, 0))
	if err != nil {
		t.Fatal(err)
	}
	scheduled, err := ParseSchedule(strings.NewReader("test: boolean char\ntest: select\n"))
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateCompleteness(&results, scheduled); err != nil {
		t.Fatal(err)
	}
	if results.PassedTests != 2 || results.FailedTests != 1 || results.ExpectedTests != 3 {
		t.Fatalf("unexpected counts: %#v", results)
	}
	if results.Tests[1].Name != "char" || results.Tests[1].Status != StatusFail || results.Tests[1].DurationMS != 202 {
		t.Fatalf("unexpected failed test: %#v", results.Tests[1])
	}
}

func TestValidateCompletenessRejectsTruncatedRun(t *testing.T) {
	results := Results{Tests: []TestResult{{Name: "boolean", Status: StatusPass}}}
	err := ValidateCompleteness(&results, []string{"boolean", "char"})
	if err == nil || !strings.Contains(err.Error(), "completed 1 of 2") || !strings.Contains(err.Error(), "missing: char") {
		t.Fatalf("error = %v, want missing test", err)
	}
	if results.ExpectedTests != 2 || results.PassedTests != 1 || results.FailedTests != 1 ||
		len(results.MissingTests) != 1 || results.MissingTests[0] != "char" ||
		len(results.Tests) != 2 || results.Tests[1].Name != "char" || results.Tests[1].Status != StatusMissing {
		t.Fatalf("partial results = %#v", results)
	}
}

func TestParseTAPToleratesInterleavedDiagnosticPrefix(t *testing.T) {
	log := `# not ok 165   + foreign_key                                 5 ms
(test process exited with exit code 2)
`
	results, err := ParseTAP(strings.NewReader(log), "17.10", time.Unix(123, 0))
	if err != nil {
		t.Fatal(err)
	}
	if len(results.Tests) != 1 || results.Tests[0].Name != "foreign_key" || results.Tests[0].Status != StatusFail {
		t.Fatalf("interleaved result = %#v", results)
	}
}

func TestFindRegressionsOnlyTracksPreviouslyPassingTests(t *testing.T) {
	baseline := Results{Tests: []TestResult{
		{Name: "boolean", Status: StatusPass},
		{Name: "char", Status: StatusFail},
		{Name: "select", Status: StatusPass},
	}}
	current := Results{Tests: []TestResult{
		{Name: "boolean", Status: StatusFail},
		{Name: "char", Status: StatusPass},
	}}
	regressions := FindRegressions(baseline, current)
	if len(regressions) != 2 || regressions[0].Name != "boolean" || regressions[1].Name != "select" || regressions[1].Current != "missing" {
		t.Fatalf("regressions = %#v", regressions)
	}
}

func TestFindRegressionsTracksNewProxyCrashes(t *testing.T) {
	regressions := FindRegressions(Results{}, Results{ProxyCrashes: 1})
	if len(regressions) != 1 || regressions[0].Name != "Hamstergres Proxy process" {
		t.Fatalf("regressions = %#v", regressions)
	}
}

func TestCountProxyCrashes(t *testing.T) {
	crashes, err := CountProxyCrashes(strings.NewReader("log line\npanic: first\nnot panic: ignored\npanic: second\n"))
	if err != nil || crashes != 2 {
		t.Fatalf("crashes = %d, err = %v", crashes, err)
	}
}

func TestWriteAndReadResultsRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "results.json")
	want := Results{
		FormatVersion:     ResultsFormatVersion,
		PostgreSQLVersion: "17.10",
		GeneratedAt:       time.Date(2026, time.July, 15, 10, 30, 0, 0, time.UTC),
		ExpectedTests:     2,
		PassedTests:       1,
		FailedTests:       1,
		MissingTests:      []string{"missing_test"},
		ProxyCrashes:      1,
		Tests: []TestResult{
			{Name: "boolean", Status: StatusPass, DurationMS: 10},
			{Name: "char", Status: StatusFail, DurationMS: 20},
		},
	}
	if err := WriteResults(path, want); err != nil {
		t.Fatal(err)
	}
	got, err := ReadResults(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.FormatVersion != want.FormatVersion || got.PostgreSQLVersion != want.PostgreSQLVersion ||
		!got.GeneratedAt.Equal(want.GeneratedAt) || got.ExpectedTests != want.ExpectedTests ||
		got.PassedTests != want.PassedTests || got.FailedTests != want.FailedTests ||
		strings.Join(got.MissingTests, ",") != strings.Join(want.MissingTests, ",") ||
		got.ProxyCrashes != want.ProxyCrashes || len(got.Tests) != len(want.Tests) ||
		got.Tests[0] != want.Tests[0] || got.Tests[1] != want.Tests[1] {
		t.Fatalf("round trip = %#v, want %#v", got, want)
	}
}

func TestWriteBadgeEndpoint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "badges", "overall.json")
	results := Results{PostgreSQLVersion: "17.10", ExpectedTests: 225, PassedTests: 12}
	if err := WriteBadgeEndpoint(path, results); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"schemaVersion": 1`, `"label": "PostgreSQL 17.10"`, `"message": "12/225 passing"`, `"color": "red"`} {
		if !strings.Contains(string(contents), want) {
			t.Fatalf("badge endpoint does not contain %q:\n%s", want, contents)
		}
	}

	results.MissingTests = []string{"foreign_key"}
	if err := WriteBadgeEndpoint(path, results); err != nil {
		t.Fatal(err)
	}
	contents, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(contents), `"message": "12/225 passing (incomplete)"`) {
		t.Fatalf("incomplete badge endpoint:\n%s", contents)
	}
}

func TestBadgeColor(t *testing.T) {
	for _, test := range []struct {
		passed int
		total  int
		want   string
	}{{0, 0, "lightgrey"}, {225, 225, "brightgreen"}, {180, 225, "yellow"}, {113, 225, "orange"}, {12, 225, "red"}} {
		if got := badgeColor(test.passed, test.total); got != test.want {
			t.Fatalf("badgeColor(%d, %d) = %q, want %q", test.passed, test.total, got, test.want)
		}
	}
}

func TestReadResultsRejectsIncompatibleFormatVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "results.json")
	if err := os.WriteFile(path, []byte(`{"format_version": 2}`), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := ReadResults(path)
	if err == nil || !strings.Contains(err.Error(), "format version 2, want 1") {
		t.Fatalf("error = %v, want incompatible format version", err)
	}
}

func TestMarkdownSeparatesGapsFromRegressions(t *testing.T) {
	results := Results{
		PostgreSQLVersion: "17.10",
		ExpectedTests:     2,
		PassedTests:       1,
		FailedTests:       1,
		ProxyCrashes:      1,
		Tests: []TestResult{
			{Name: "boolean", Status: StatusPass, DurationMS: 10},
			{Name: "char", Status: StatusFail, DurationMS: 20},
		},
	}
	report := Markdown(results, []Regression{{Name: "char", Previous: StatusPass, Current: StatusFail}}, true)
	for _, want := range []string{"1/2 passing", "1 compatibility gaps", "crashed 1 time", "1 regression(s)", "`char`: pass -> fail", "| `char` | GAP | 20 ms |"} {
		if !strings.Contains(report, want) {
			t.Fatalf("report does not contain %q:\n%s", want, report)
		}
	}
}

func TestMarkdownExplainsMissingBaseline(t *testing.T) {
	report := Markdown(Results{PostgreSQLVersion: "17.10"}, nil, false)
	if !strings.Contains(report, "No compatible baseline was supplied") {
		t.Fatalf("report did not explain missing baseline:\n%s", report)
	}
}

func TestMarkdownReportsPartialInventory(t *testing.T) {
	results := Results{
		PostgreSQLVersion: "17.10",
		ExpectedTests:     2,
		PassedTests:       1,
		FailedTests:       1,
		MissingTests:      []string{"foreign_key"},
		Tests: []TestResult{
			{Name: "boolean", Status: StatusPass, DurationMS: 10},
			{Name: "foreign_key", Status: StatusMissing},
		},
	}
	report := Markdown(results, nil, false)
	for _, want := range []string{"1/2 passing", "Harness incomplete", "`foreign_key`", "| `foreign_key` | MISSING | 0 ms |"} {
		if !strings.Contains(report, want) {
			t.Fatalf("partial report does not contain %q:\n%s", want, report)
		}
	}
}
