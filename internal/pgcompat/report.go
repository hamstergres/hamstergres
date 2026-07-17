// SPDX-License-Identifier: AGPL-3.0-only

// Package pgcompat parses PostgreSQL's pg_regress TAP output and reports
// compatibility gaps separately from regressions in previously passing tests.
package pgcompat

import (
	"bufio"
	"crypto/sha256"
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
const ExpectedDifferencesFormatVersion = 1

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

// ExpectedDifferences records reviewed, intentional differences from
// PostgreSQL's expected output without changing the raw compatibility score.
// The complete output digest keeps the exception narrower than a test-name
// allowlist: any additional difference in the same test fails verification.
type ExpectedDifferences struct {
	FormatVersion     int                  `json:"format_version"`
	PostgreSQLVersion string               `json:"postgresql_version"`
	Differences       []ExpectedDifference `json:"differences"`
}

type ExpectedDifference struct {
	Test           string                `json:"test"`
	OutputSHA256   string                `json:"output_sha256"`
	Reason         string                `json:"reason"`
	Normalizations []OutputNormalization `json:"normalizations,omitempty"`
}

type OutputNormalization struct {
	Pattern         string `json:"pattern"`
	Replacement     string `json:"replacement"`
	ExpectedMatches int    `json:"expected_matches"`
}

type DifferenceVerification struct {
	ExpectedDifference
	ActualSHA256 string
	Matched      bool
	Problem      string
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
var testNamePattern = regexp.MustCompile(`^[A-Za-z0-9_]+$`)
var sha256Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

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

func ReadExpectedDifferences(path string) (ExpectedDifferences, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return ExpectedDifferences{}, err
	}
	var expected ExpectedDifferences
	if err := json.Unmarshal(contents, &expected); err != nil {
		return ExpectedDifferences{}, fmt.Errorf("decode expected differences %s: %w", path, err)
	}
	if expected.FormatVersion != ExpectedDifferencesFormatVersion {
		return ExpectedDifferences{}, fmt.Errorf("expected differences %s use format version %d, want %d", path, expected.FormatVersion, ExpectedDifferencesFormatVersion)
	}
	seen := make(map[string]struct{}, len(expected.Differences))
	for _, difference := range expected.Differences {
		if !testNamePattern.MatchString(difference.Test) {
			return ExpectedDifferences{}, fmt.Errorf("expected differences %s contain invalid test name %q", path, difference.Test)
		}
		if _, exists := seen[difference.Test]; exists {
			return ExpectedDifferences{}, fmt.Errorf("expected differences %s contain duplicate test %q", path, difference.Test)
		}
		seen[difference.Test] = struct{}{}
		if !sha256Pattern.MatchString(difference.OutputSHA256) {
			return ExpectedDifferences{}, fmt.Errorf("expected differences %s contain invalid SHA-256 for %q", path, difference.Test)
		}
		if strings.TrimSpace(difference.Reason) == "" {
			return ExpectedDifferences{}, fmt.Errorf("expected differences %s contain no reason for %q", path, difference.Test)
		}
		for _, normalization := range difference.Normalizations {
			if _, err := regexp.Compile(normalization.Pattern); err != nil {
				return ExpectedDifferences{}, fmt.Errorf("expected differences %s contain invalid normalization for %q: %w", path, difference.Test, err)
			}
			if normalization.ExpectedMatches <= 0 {
				return ExpectedDifferences{}, fmt.Errorf("expected differences %s contain non-positive normalization match count for %q", path, difference.Test)
			}
		}
	}
	return expected, nil
}

// VerifyExpectedDifferences checks every policy entry on every run, including
// after the default-branch baseline records the test as a gap. A passing test
// makes its entry stale, while any output change invalidates the exact digest.
func VerifyExpectedDifferences(results Results, testResultsDirectory string, expected ExpectedDifferences) []DifferenceVerification {
	statusByName := make(map[string]TestStatus, len(results.Tests))
	for _, test := range results.Tests {
		statusByName[test.Name] = test.Status
	}
	verifications := make([]DifferenceVerification, 0, len(expected.Differences))
	for _, difference := range expected.Differences {
		verification := DifferenceVerification{ExpectedDifference: difference}
		status, found := statusByName[difference.Test]
		switch {
		case !found:
			verification.Problem = "test was not present in the compatibility results"
		case status == StatusPass:
			verification.Problem = "test now passes; remove the stale expected difference"
		case status == StatusMissing:
			verification.Problem = "test did not complete"
		default:
			contents, err := os.ReadFile(filepath.Join(testResultsDirectory, difference.Test+".out"))
			if err != nil {
				verification.Problem = fmt.Sprintf("read test output: %v", err)
				break
			}
			normalized := string(contents)
			for _, normalization := range difference.Normalizations {
				pattern := regexp.MustCompile(normalization.Pattern)
				matches := pattern.FindAllStringIndex(normalized, -1)
				if len(matches) != normalization.ExpectedMatches {
					verification.Problem = fmt.Sprintf("normalization %q matched %d time(s), want %d", normalization.Pattern, len(matches), normalization.ExpectedMatches)
					break
				}
				normalized = pattern.ReplaceAllString(normalized, normalization.Replacement)
			}
			if verification.Problem != "" {
				break
			}
			verification.ActualSHA256 = fmt.Sprintf("%x", sha256.Sum256([]byte(normalized)))
			if verification.ActualSHA256 != difference.OutputSHA256 {
				verification.Problem = fmt.Sprintf("output changed: got SHA-256 %s", verification.ActualSHA256)
				break
			}
			verification.Matched = true
		}
		verifications = append(verifications, verification)
	}
	sort.Slice(verifications, func(i, j int) bool { return verifications[i].Test < verifications[j].Test })
	return verifications
}

// SeparateExpectedRegressions accepts a pass-to-fail regression only when its
// current complete output exactly matches a verified intentional difference.
func SeparateExpectedRegressions(regressions []Regression, verifications []DifferenceVerification) (expected, unexpected []Regression) {
	verified := make(map[string]struct{}, len(verifications))
	for _, verification := range verifications {
		if verification.Matched {
			verified[verification.Test] = struct{}{}
		}
	}
	for _, regression := range regressions {
		if _, found := verified[regression.Name]; found {
			expected = append(expected, regression)
		} else {
			unexpected = append(unexpected, regression)
		}
	}
	return expected, unexpected
}

func DifferenceVerificationProblems(verifications []DifferenceVerification) []DifferenceVerification {
	var problems []DifferenceVerification
	for _, verification := range verifications {
		if !verification.Matched {
			problems = append(problems, verification)
		}
	}
	return problems
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

func Markdown(results Results, unexpectedRegressions, expectedRegressions []Regression, verifications []DifferenceVerification, baselineCompared bool) string {
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
	} else if len(unexpectedRegressions) == 0 {
		report.WriteString("No unexpected pass-to-fail regressions were found.\n\n")
	} else {
		fmt.Fprintf(&report, "**%d unexpected regression(s) in previously passing tests:**\n\n", len(unexpectedRegressions))
		for _, regression := range unexpectedRegressions {
			fmt.Fprintf(&report, "- `%s`: %s -> %s\n", regression.Name, regression.Previous, regression.Current)
		}
		report.WriteString("\n")
	}
	if len(expectedRegressions) > 0 {
		fmt.Fprintf(&report, "%d pass-to-fail regression(s) matched reviewed intentional-difference signatures.\n\n", len(expectedRegressions))
	}
	if len(verifications) > 0 {
		problems := DifferenceVerificationProblems(verifications)
		if len(problems) == 0 {
			fmt.Fprintf(&report, "**%d intentional PostgreSQL difference(s) verified by exact output signature.** The raw PostgreSQL score remains unchanged.\n\n", len(verifications))
		} else {
			fmt.Fprintf(&report, "**%d intentional-difference signature problem(s):**\n\n", len(problems))
			for _, verification := range problems {
				fmt.Fprintf(&report, "- `%s`: %s\n", verification.Test, verification.Problem)
			}
			report.WriteString("\n")
		}
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
