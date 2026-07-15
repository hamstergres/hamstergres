// SPDX-License-Identifier: AGPL-3.0-only

package pgcompat

import (
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
	if err == nil || !strings.Contains(err.Error(), "missing: char") {
		t.Fatalf("error = %v, want missing test", err)
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

func TestMarkdownNoRegressionsWithBaseline(t *testing.T) {
	results := Results{
		PostgreSQLVersion: "17.10",
		ExpectedTests:     1,
		PassedTests:       1,
		Tests:             []TestResult{{Name: "boolean", Status: StatusPass, DurationMS: 5}},
	}
	report := Markdown(results, nil, true)
	if !strings.Contains(report, "No previously passing PostgreSQL regression test regressed.") {
		t.Fatalf("report did not report a clean comparison:\n%s", report)
	}
	if strings.Contains(report, "regression(s)") {
		t.Fatalf("report unexpectedly mentions regressions:\n%s", report)
	}
}

func TestParseTAPSkipsNonResultLines(t *testing.T) {
	log := "# using postmaster on Unix socket\n" +
		"    ok 1      + boolean                                101 ms\n" +
		"1..1\n"
	results, err := ParseTAP(strings.NewReader(log), "17.10", time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	if len(results.Tests) != 1 || results.Tests[0].Name != "boolean" {
		t.Fatalf("unexpected tests: %#v", results.Tests)
	}
}

func TestParseTAPRejectsDuplicateTestName(t *testing.T) {
	log := "    ok 1      + boolean                                101 ms\n" +
		"    ok 2      + boolean                                102 ms\n"
	_, err := ParseTAP(strings.NewReader(log), "17.10", time.Unix(0, 0))
	if err == nil || !strings.Contains(err.Error(), `duplicate pg_regress result for "boolean"`) {
		t.Fatalf("err = %v, want duplicate result error", err)
	}
}

func TestParseTAPRejectsEmptyOutput(t *testing.T) {
	_, err := ParseTAP(strings.NewReader("# no matching lines here\n"), "17.10", time.Unix(0, 0))
	if err == nil || !strings.Contains(err.Error(), "no TAP test results") {
		t.Fatalf("err = %v, want empty output error", err)
	}
}

func TestParseTAPSetsFormatVersionAndUTCTimestamp(t *testing.T) {
	generatedAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.FixedZone("test", 3600))
	results, err := ParseTAP(strings.NewReader("    ok 1      + boolean                                101 ms\n"), "17.10", generatedAt)
	if err != nil {
		t.Fatal(err)
	}
	if results.FormatVersion != ResultsFormatVersion {
		t.Fatalf("FormatVersion = %d, want %d", results.FormatVersion, ResultsFormatVersion)
	}
	if !results.GeneratedAt.Equal(generatedAt) || results.GeneratedAt.Location() != time.UTC {
		t.Fatalf("GeneratedAt = %v, want UTC equivalent of %v", results.GeneratedAt, generatedAt)
	}
}

func TestParseScheduleHandlesMultipleTestsAndLines(t *testing.T) {
	tests, err := ParseSchedule(strings.NewReader("test: boolean char\ntest: select\n# ignored\n"))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"boolean", "char", "select"}
	if len(tests) != len(want) {
		t.Fatalf("tests = %#v, want %#v", tests, want)
	}
	for i, name := range want {
		if tests[i] != name {
			t.Fatalf("tests[%d] = %q, want %q", i, tests[i], name)
		}
	}
}

func TestParseScheduleRejectsDuplicateTest(t *testing.T) {
	_, err := ParseSchedule(strings.NewReader("test: boolean\ntest: boolean\n"))
	if err == nil || !strings.Contains(err.Error(), `duplicate scheduled test "boolean"`) {
		t.Fatalf("err = %v, want duplicate scheduled test error", err)
	}
}

func TestParseScheduleRejectsEmptySchedule(t *testing.T) {
	_, err := ParseSchedule(strings.NewReader("# nothing scheduled\n"))
	if err == nil || !strings.Contains(err.Error(), "no tests") {
		t.Fatalf("err = %v, want empty schedule error", err)
	}
}

func TestValidateCompletenessAllowsExtraCompletedTests(t *testing.T) {
	results := Results{Tests: []TestResult{
		{Name: "boolean", Status: StatusPass},
		{Name: "unscheduled_extra", Status: StatusPass},
	}}
	if err := ValidateCompleteness(&results, []string{"boolean"}); err != nil {
		t.Fatalf("unexpected error for extra completed test: %v", err)
	}
	if results.ExpectedTests != 1 {
		t.Fatalf("ExpectedTests = %d, want 1", results.ExpectedTests)
	}
}

func TestFindRegressionsNoRegressionsWhenPreviouslyPassingTestsStillPass(t *testing.T) {
	baseline := Results{Tests: []TestResult{{Name: "boolean", Status: StatusPass}}, ProxyCrashes: 1}
	current := Results{Tests: []TestResult{{Name: "boolean", Status: StatusPass}}, ProxyCrashes: 1}
	regressions := FindRegressions(baseline, current)
	if len(regressions) != 0 {
		t.Fatalf("regressions = %#v, want none", regressions)
	}
}

func TestFindRegressionsIgnoresPreviouslyFailingTests(t *testing.T) {
	baseline := Results{Tests: []TestResult{{Name: "char", Status: StatusFail}}}
	current := Results{Tests: []TestResult{{Name: "char", Status: StatusFail}}}
	regressions := FindRegressions(baseline, current)
	if len(regressions) != 0 {
		t.Fatalf("regressions = %#v, want none for a test that never passed", regressions)
	}
}

func TestCountProxyCrashesNoCrashes(t *testing.T) {
	crashes, err := CountProxyCrashes(strings.NewReader("log line one\nlog line two\n"))
	if err != nil || crashes != 0 {
		t.Fatalf("crashes = %d, err = %v", crashes, err)
	}
}

func TestWriteResultsAndReadResultsRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "results.json")
	original := Results{
		FormatVersion:     ResultsFormatVersion,
		PostgreSQLVersion: "17.10",
		ExpectedTests:     1,
		PassedTests:       1,
		Tests:             []TestResult{{Name: "boolean", Status: StatusPass, DurationMS: 42}},
	}
	if err := WriteResults(path, original); err != nil {
		t.Fatal(err)
	}
	loaded, err := ReadResults(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.PostgreSQLVersion != original.PostgreSQLVersion || len(loaded.Tests) != 1 || loaded.Tests[0].Name != "boolean" {
		t.Fatalf("loaded = %#v, want match for %#v", loaded, original)
	}
}

func TestReadResultsRejectsFormatVersionMismatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "results.json")
	if err := WriteResults(path, Results{FormatVersion: ResultsFormatVersion + 1}); err != nil {
		t.Fatal(err)
	}
	_, err := ReadResults(path)
	if err == nil || !strings.Contains(err.Error(), "format version") {
		t.Fatalf("err = %v, want format version mismatch error", err)
	}
}

func TestReadResultsMissingFile(t *testing.T) {
	_, err := ReadResults(filepath.Join(t.TempDir(), "does-not-exist.json"))
	if err == nil {
		t.Fatal("expected an error for a missing results file")
	}
}
