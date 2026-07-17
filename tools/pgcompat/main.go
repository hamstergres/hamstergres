// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/jruszo/hamstergres/internal/pgcompat"
)

func main() {
	logPath := flag.String("log", "", "pg_regress TAP output")
	schedulePath := flag.String("schedule", "", "PostgreSQL parallel_schedule")
	outputDirectory := flag.String("output", "", "directory for results.json and compatibility-report.md")
	baselinePath := flag.String("baseline", "", "optional prior results.json")
	expectedDifferencesPath := flag.String("expected-differences", "", "optional reviewed intentional-differences policy")
	testResultsDirectory := flag.String("test-results", "", "directory containing pg_regress per-test .out files")
	postgresqlVersion := flag.String("postgresql-version", "", "tested PostgreSQL release")
	proxyLogPath := flag.String("proxy-log", "", "Hamstergres Proxy log used to detect process crashes")
	flag.Parse()

	if *logPath == "" || *schedulePath == "" || *outputDirectory == "" || *postgresqlVersion == "" {
		flag.Usage()
		os.Exit(2)
	}
	logFile, err := os.Open(*logPath)
	fatalIf(err)
	defer logFile.Close()
	results, err := pgcompat.ParseTAP(logFile, *postgresqlVersion, time.Now())
	fatalIf(err)
	scheduleFile, err := os.Open(*schedulePath)
	fatalIf(err)
	defer scheduleFile.Close()
	scheduled, err := pgcompat.ParseSchedule(scheduleFile)
	fatalIf(err)
	completenessErr := pgcompat.ValidateCompleteness(&results, scheduled)
	if *proxyLogPath != "" {
		proxyLog, err := os.Open(*proxyLogPath)
		fatalIf(err)
		results.ProxyCrashes, err = pgcompat.CountProxyCrashes(proxyLog)
		closeErr := proxyLog.Close()
		fatalIf(err)
		fatalIf(closeErr)
	}

	var regressions []pgcompat.Regression
	baselineCompared := false
	if *baselinePath != "" {
		baseline, err := pgcompat.ReadResults(*baselinePath)
		fatalIf(err)
		if baseline.PostgreSQLVersion != results.PostgreSQLVersion {
			fmt.Fprintf(os.Stderr, "ignoring PostgreSQL %s baseline while testing %s\n", baseline.PostgreSQLVersion, results.PostgreSQLVersion)
		} else {
			baselineCompared = true
			regressions = pgcompat.FindRegressions(baseline, results)
		}
	}
	var verifications []pgcompat.DifferenceVerification
	if *expectedDifferencesPath != "" {
		if *testResultsDirectory == "" {
			fatalIf(fmt.Errorf("-test-results is required with -expected-differences"))
		}
		expected, err := pgcompat.ReadExpectedDifferences(*expectedDifferencesPath)
		fatalIf(err)
		if expected.PostgreSQLVersion != results.PostgreSQLVersion {
			fatalIf(fmt.Errorf("expected differences target PostgreSQL %s while testing %s", expected.PostgreSQLVersion, results.PostgreSQLVersion))
		}
		verifications = pgcompat.VerifyExpectedDifferences(results, *testResultsDirectory, expected)
	}
	expectedRegressions, unexpectedRegressions := pgcompat.SeparateExpectedRegressions(regressions, verifications)
	verificationProblems := pgcompat.DifferenceVerificationProblems(verifications)
	fatalIf(os.MkdirAll(*outputDirectory, 0o755))
	fatalIf(pgcompat.WriteResults(filepath.Join(*outputDirectory, "results.json"), results))
	fatalIf(os.WriteFile(filepath.Join(*outputDirectory, "compatibility-report.md"), []byte(pgcompat.Markdown(results, unexpectedRegressions, expectedRegressions, verifications, baselineCompared)), 0o644))
	fatalIf(pgcompat.WriteBadgeEndpoint(filepath.Join(*outputDirectory, "badges", "overall.json"), results))
	fmt.Printf("PostgreSQL %s compatibility: %d/%d passed, %d gaps, %d unexpected regressions, %d verified intentional differences\n", results.PostgreSQLVersion, results.PassedTests, results.ExpectedTests, results.FailedTests, len(unexpectedRegressions), len(verifications)-len(verificationProblems))
	if completenessErr != nil {
		fmt.Fprintln(os.Stderr, completenessErr)
		os.Exit(2)
	}
	if len(verificationProblems) > 0 {
		for _, verification := range verificationProblems {
			fmt.Fprintf(os.Stderr, "expected difference %s: %s\n", verification.Test, verification.Problem)
		}
		os.Exit(1)
	}
	if len(unexpectedRegressions) > 0 {
		os.Exit(1)
	}
}

func fatalIf(err error) {
	if err == nil {
		return
	}
	fmt.Fprintln(os.Stderr, err)
	os.Exit(2)
}
