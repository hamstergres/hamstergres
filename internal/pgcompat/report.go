// SPDX-License-Identifier: AGPL-3.0-only

// Package pgcompat parses PostgreSQL's pg_regress TAP output and reports
// compatibility gaps separately from regressions in previously passing tests.
package pgcompat

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const ResultsFormatVersion = 1

type TestStatus string

const (
	StatusPass    TestStatus = "pass"
	StatusFail    TestStatus = "fail"
	StatusMissing TestStatus = "missing"
)

type TestResult struct {
	Name       string     `json:"name"`
	Status     TestStatus `json:"status"`
	DurationMS int        `json:"duration_ms"`
}

type Results struct {
	FormatVersion     int          `json:"format_version"`
	PostgreSQLVersion string       `json:"postgresql_version"`
	GeneratedAt       time.Time    `json:"generated_at"`
	ExpectedTests     int          `json:"expected_tests"`
	PassedTests       int          `json:"passed_tests"`
	FailedTests       int          `json:"failed_tests"`
	MissingTests      []string     `json:"missing_tests,omitempty"`
	ProxyCrashes      int          `json:"proxy_crashes"`
	Tests             []TestResult `json:"tests"`
}

type Regression struct {
	Name     string     `json:"name"`
	Previous TestStatus `json:"previous_status"`
	Current  TestStatus `json:"current_status"`
}

// BadgeEndpoint is the Shields.io endpoint schema published by the
// compatibility workflow for the live README progress badge.
type BadgeEndpoint struct {
	SchemaVersion int    `json:"schemaVersion"`
	Label         string `json:"label"`
	Message       string `json:"message"`
	Color         string `json:"color"`
}

var tapResultPattern = regexp.MustCompile(`^\s*(?:#\s*)?(not )?ok\s+[0-9]+\s+[+-]\s+(\S+)\s+([0-9]+)\s+ms\s*$`)

func ParseTAP(reader io.Reader, postgresqlVersion string, generatedAt time.Time) (Results, error) {
	scanner := bufio.NewScanner(reader)
	results := Results{
		FormatVersion:     ResultsFormatVersion,
		PostgreSQLVersion: postgresqlVersion,
		GeneratedAt:       generatedAt.UTC(),
	}
	seen := make(map[string]struct{})
	for scanner.Scan() {
		matches := tapResultPattern.FindStringSubmatch(scanner.Text())
		if matches == nil {
			continue
		}
		name := matches[2]
		if _, exists := seen[name]; exists {
			return Results{}, fmt.Errorf("duplicate pg_regress result for %q", name)
		}
		seen[name] = struct{}{}
		duration, err := strconv.Atoi(matches[3])
		if err != nil {
			return Results{}, fmt.Errorf("parse duration for %q: %w", name, err)
		}
		status := StatusPass
		if matches[1] != "" {
			status = StatusFail
		}
		results.Tests = append(results.Tests, TestResult{Name: name, Status: status, DurationMS: duration})
	}
	if err := scanner.Err(); err != nil {
		return Results{}, fmt.Errorf("read pg_regress output: %w", err)
	}
	if len(results.Tests) == 0 {
		return Results{}, errors.New("pg_regress output contained no TAP test results")
	}
	results.recount()
	return results, nil
}

func ParseSchedule(reader io.Reader) ([]string, error) {
	scanner := bufio.NewScanner(reader)
	var tests []string
	seen := make(map[string]struct{})
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "test:") {
			continue
		}
		for _, name := range strings.Fields(strings.TrimSpace(strings.TrimPrefix(line, "test:"))) {
			if _, exists := seen[name]; exists {
				return nil, fmt.Errorf("duplicate scheduled test %q", name)
			}
			seen[name] = struct{}{}
			tests = append(tests, name)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read PostgreSQL schedule: %w", err)
	}
	if len(tests) == 0 {
		return nil, errors.New("PostgreSQL schedule contained no tests")
	}
	return tests, nil
}

func ValidateCompleteness(results *Results, scheduled []string) error {
	results.ExpectedTests = len(scheduled)
	results.MissingTests = nil
	completed := len(results.Tests)
	actual := make(map[string]struct{}, len(results.Tests))
	for _, test := range results.Tests {
		actual[test.Name] = struct{}{}
	}
	var missing []string
	for _, name := range scheduled {
		if _, found := actual[name]; !found {
			missing = append(missing, name)
			results.Tests = append(results.Tests, TestResult{Name: name, Status: StatusMissing})
		}
	}
	results.MissingTests = append(results.MissingTests, missing...)
	results.recount()
	if len(missing) > 0 {
		return fmt.Errorf("pg_regress completed %d of %d scheduled tests; missing: %s", completed, len(scheduled), strings.Join(missing, ", "))
	}
	return nil
}

func FindRegressions(baseline, current Results) []Regression {
	currentByName := make(map[string]TestStatus, len(current.Tests))
	for _, test := range current.Tests {
		currentByName[test.Name] = test.Status
	}
	var regressions []Regression
	for _, previous := range baseline.Tests {
		if previous.Status != StatusPass {
			continue
		}
		status, found := currentByName[previous.Name]
		if !found {
			status = "missing"
		}
		if status != StatusPass {
			regressions = append(regressions, Regression{Name: previous.Name, Previous: previous.Status, Current: status})
		}
	}
	if baseline.ProxyCrashes == 0 && current.ProxyCrashes > 0 {
		regressions = append(regressions, Regression{Name: "Hamstergres Proxy process", Previous: StatusPass, Current: StatusFail})
	}
	sort.Slice(regressions, func(i, j int) bool { return regressions[i].Name < regressions[j].Name })
	return regressions
}

func CountProxyCrashes(reader io.Reader) (int, error) {
	scanner := bufio.NewScanner(reader)
	crashes := 0
	for scanner.Scan() {
		if strings.HasPrefix(scanner.Text(), "panic:") {
			crashes++
		}
	}
	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("read Proxy log: %w", err)
	}
	return crashes, nil
}

func ReadResults(path string) (Results, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return Results{}, err
	}
	var results Results
	if err := json.Unmarshal(contents, &results); err != nil {
		return Results{}, fmt.Errorf("decode results %s: %w", path, err)
	}
	if results.FormatVersion != ResultsFormatVersion {
		return Results{}, fmt.Errorf("results %s use format version %d, want %d", path, results.FormatVersion, ResultsFormatVersion)
	}
	return results, nil
}

func WriteResults(path string, results Results) error {
	contents, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return err
	}
	contents = append(contents, '\n')
	return os.WriteFile(path, contents, 0o644)
}

func WriteBadgeEndpoint(path string, results Results) error {
	total := results.ExpectedTests
	if total == 0 {
		total = len(results.Tests)
	}
	message := fmt.Sprintf("%d/%d passing", results.PassedTests, total)
	if len(results.MissingTests) > 0 {
		message += " (incomplete)"
	}
	endpoint := BadgeEndpoint{
		SchemaVersion: 1,
		Label:         "PostgreSQL " + results.PostgreSQLVersion,
		Message:       message,
		Color:         badgeColor(results.PassedTests, total),
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	contents, err := json.MarshalIndent(endpoint, "", "  ")
	if err != nil {
		return err
	}
	contents = append(contents, '\n')
	return os.WriteFile(path, contents, 0o644)
}

func badgeColor(passed, total int) string {
	if total == 0 {
		return "lightgrey"
	}
	if passed == total {
		return "brightgreen"
	}
	percentage := passed * 100 / total
	switch {
	case percentage >= 80:
		return "yellow"
	case percentage >= 50:
		return "orange"
	default:
		return "red"
	}
}

func Markdown(results Results, regressions []Regression, baselineCompared bool) string {
	var report strings.Builder
	report.WriteString("## PostgreSQL Compatibility Report\n\n")
	fmt.Fprintf(&report, "PostgreSQL `%s`: **%d/%d passing**, %d compatibility gaps.\n\n", results.PostgreSQLVersion, results.PassedTests, results.ExpectedTests, results.FailedTests)
	if results.ProxyCrashes > 0 {
		fmt.Fprintf(&report, "> **Hamstergres Proxy crashed %d time(s).** Results after the first crash can reflect connection loss as well as statement incompatibility. See `proxy.log`.\n\n", results.ProxyCrashes)
	}
	if len(results.MissingTests) > 0 {
		fmt.Fprintf(&report, "> **Harness incomplete:** %d scheduled test(s) produced no result: `%s`. The partial inventory is shown below.\n\n", len(results.MissingTests), strings.Join(results.MissingTests, "`, `"))
	}
	if !baselineCompared {
		report.WriteString("No compatible baseline was supplied. This run records the current inventory but cannot detect pass-to-fail regressions.\n\n")
	} else if len(regressions) == 0 {
		report.WriteString("No previously passing PostgreSQL regression test regressed.\n\n")
	} else {
		fmt.Fprintf(&report, "**%d regression(s) in previously passing tests:**\n\n", len(regressions))
		for _, regression := range regressions {
			fmt.Fprintf(&report, "- `%s`: %s -> %s\n", regression.Name, regression.Previous, regression.Current)
		}
		report.WriteString("\n")
	}
	report.WriteString("| Test | Status | Duration |\n|---|---:|---:|\n")
	for _, test := range results.Tests {
		status := "PASS"
		if test.Status == StatusFail {
			status = "GAP"
		} else if test.Status == StatusMissing {
			status = "MISSING"
		}
		fmt.Fprintf(&report, "| `%s` | %s | %d ms |\n", test.Name, status, test.DurationMS)
	}
	return report.String()
}

func (results *Results) recount() {
	results.PassedTests = 0
	results.FailedTests = 0
	for _, test := range results.Tests {
		switch test.Status {
		case StatusPass:
			results.PassedTests++
		case StatusFail, StatusMissing:
			results.FailedTests++
		}
	}
}
