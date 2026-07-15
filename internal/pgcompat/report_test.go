// SPDX-License-Identifier: AGPL-3.0-only

package pgcompat

import (
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
