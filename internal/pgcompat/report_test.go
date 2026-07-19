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

func TestVerifyExpectedDifferencesRequiresExactOutput(t *testing.T) {
	testResultsDirectory := t.TempDir()
	if err := os.WriteFile(filepath.Join(testResultsDirectory, "float4.out"), []byte("hello"), 0o600); err != nil {
		t.Fatal(err)
	}
	expected := ExpectedDifferences{Differences: []ExpectedDifference{{
		Test:         "float4",
		OutputSHA256: "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
		Reason:       "reviewed rejection",
	}}}
	results := Results{Tests: []TestResult{{Name: "float4", Status: StatusFail}}}
	verifications := VerifyExpectedDifferences(results, testResultsDirectory, expected)
	if len(verifications) != 1 || !verifications[0].Matched || verifications[0].Problem != "" {
		t.Fatalf("verifications = %#v", verifications)
	}

	if err := os.WriteFile(filepath.Join(testResultsDirectory, "float4.out"), []byte("changed"), 0o600); err != nil {
		t.Fatal(err)
	}
	verifications = VerifyExpectedDifferences(results, testResultsDirectory, expected)
	if len(verifications) != 1 || verifications[0].Matched || !strings.Contains(verifications[0].Problem, "output changed") {
		t.Fatalf("changed verifications = %#v", verifications)
	}
}

func TestVerifyExpectedDifferencesRejectsStalePassingEntry(t *testing.T) {
	expected := ExpectedDifferences{Differences: []ExpectedDifference{{Test: "float8", OutputSHA256: strings.Repeat("0", 64), Reason: "reviewed rejection"}}}
	results := Results{Tests: []TestResult{{Name: "float8", Status: StatusPass}}}
	verifications := VerifyExpectedDifferences(results, t.TempDir(), expected)
	if len(verifications) != 1 || verifications[0].Matched || !strings.Contains(verifications[0].Problem, "stale") {
		t.Fatalf("verifications = %#v", verifications)
	}
}

func TestVerifyExpectedDifferencesNormalizesExactVolatileMatchCount(t *testing.T) {
	testResultsDirectory := t.TempDir()
	outputPath := filepath.Join(testResultsDirectory, "xid.out")
	if err := os.WriteFile(outputPath, []byte("id=1234\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	expected := ExpectedDifferences{Differences: []ExpectedDifference{{
		Test:         "xid",
		OutputSHA256: "6712db25bf5c6ed7b442f2605fdfb6796597060ff57dfd7441388b0674fc5ac9",
		Reason:       "reviewed rejection",
		Normalizations: []OutputNormalization{{
			Pattern:         `(?m)^id=[0-9]+$`,
			Replacement:     "id=<xid>",
			ExpectedMatches: 1,
		}},
	}}}
	results := Results{Tests: []TestResult{{Name: "xid", Status: StatusFail}}}
	verifications := VerifyExpectedDifferences(results, testResultsDirectory, expected)
	if len(verifications) != 1 || !verifications[0].Matched {
		t.Fatalf("verifications = %#v", verifications)
	}

	if err := os.WriteFile(outputPath, []byte("no volatile value\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	verifications = VerifyExpectedDifferences(results, testResultsDirectory, expected)
	if len(verifications) != 1 || verifications[0].Matched || !strings.Contains(verifications[0].Problem, "matched 0 time") {
		t.Fatalf("missing normalization match = %#v", verifications)
	}
}

func TestVerifyExpectedDifferencesReportsMalformedNormalization(t *testing.T) {
	testResultsDirectory := t.TempDir()
	if err := os.WriteFile(filepath.Join(testResultsDirectory, "xid.out"), []byte("id=1234\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	expected := ExpectedDifferences{Differences: []ExpectedDifference{{
		Test:         "xid",
		OutputSHA256: strings.Repeat("0", 64),
		Reason:       "direct-call validation",
		Normalizations: []OutputNormalization{{
			Pattern:         "[",
			ExpectedMatches: 1,
		}},
	}}}
	results := Results{Tests: []TestResult{{Name: "xid", Status: StatusFail}}}

	verifications := VerifyExpectedDifferences(results, testResultsDirectory, expected)
	if len(verifications) != 1 || verifications[0].Matched || !strings.Contains(verifications[0].Problem, "invalid normalization") {
		t.Fatalf("verifications = %#v", verifications)
	}
}

func TestSeparateExpectedRegressionsRequiresVerifiedSignature(t *testing.T) {
	regressions := []Regression{{Name: "float4"}, {Name: "float8"}}
	verifications := []DifferenceVerification{
		{ExpectedDifference: ExpectedDifference{Test: "float4"}, Matched: true},
		{ExpectedDifference: ExpectedDifference{Test: "float8"}, Problem: "output changed"},
	}
	expected, unexpected := SeparateExpectedRegressions(regressions, verifications)
	if len(expected) != 1 || expected[0].Name != "float4" || len(unexpected) != 1 || unexpected[0].Name != "float8" {
		t.Fatalf("expected = %#v, unexpected = %#v", expected, unexpected)
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

func TestReadExpectedDifferencesRejectsUnknownAndMissingFields(t *testing.T) {
	for _, test := range []struct {
		name     string
		contents string
		want     string
	}{
		{name: "unknown", contents: `{"format_version":1,"postgresql_version":"17.10","difference":[]}`, want: "unknown field"},
		{name: "missing", contents: `{"format_version":1,"postgresql_version":"17.10"}`, want: "omit differences"},
		{name: "null", contents: `{"format_version":1,"postgresql_version":"17.10","differences":null}`, want: "omit differences"},
		{name: "trailing", contents: `{"format_version":1,"postgresql_version":"17.10","differences":[]} {}`, want: "multiple JSON values"},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "expected.json")
			if err := os.WriteFile(path, []byte(test.contents), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := ReadExpectedDifferences(path)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
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
	report := Markdown(results, []Regression{{Name: "char", Previous: StatusPass, Current: StatusFail}}, nil, nil, true)
	for _, want := range []string{"1/2 passing", "1 compatibility gaps", "crashed 1 time", "1 unexpected regression(s)", "`char`: pass -> fail", "| `char` | GAP | 20 ms |"} {
		if !strings.Contains(report, want) {
			t.Fatalf("report does not contain %q:\n%s", want, report)
		}
	}
}

func TestMarkdownExplainsMissingBaseline(t *testing.T) {
	report := Markdown(Results{PostgreSQLVersion: "17.10"}, nil, nil, nil, false)
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
	report := Markdown(results, nil, nil, nil, false)
	for _, want := range []string{"1/2 passing", "Harness incomplete", "`foreign_key`", "| `foreign_key` | MISSING | 0 ms |"} {
		if !strings.Contains(report, want) {
			t.Fatalf("partial report does not contain %q:\n%s", want, report)
		}
	}
}

func TestMarkdownReportsVerifiedIntentionalDifferences(t *testing.T) {
	verification := DifferenceVerification{
		ExpectedDifference: ExpectedDifference{Test: "float4", Reason: "reviewed rejection"},
		Matched:            true,
	}
	report := Markdown(Results{PostgreSQLVersion: "17.10"}, nil, []Regression{{Name: "float4"}}, []DifferenceVerification{verification}, true)
	for _, want := range []string{"No unexpected pass-to-fail", "1 pass-to-fail regression", "1 intentional PostgreSQL difference", "raw PostgreSQL score remains unchanged"} {
		if !strings.Contains(report, want) {
			t.Fatalf("report does not contain %q:\n%s", want, report)
		}
	}
}
