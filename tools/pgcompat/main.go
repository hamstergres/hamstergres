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
	fatalIf(pgcompat.ValidateCompleteness(&results, scheduled))
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
	fatalIf(os.MkdirAll(*outputDirectory, 0o755))
	fatalIf(pgcompat.WriteResults(filepath.Join(*outputDirectory, "results.json"), results))
	fatalIf(os.WriteFile(filepath.Join(*outputDirectory, "compatibility-report.md"), []byte(pgcompat.Markdown(results, regressions, baselineCompared)), 0o644))
	fmt.Printf("PostgreSQL %s compatibility: %d/%d passed, %d gaps, %d regressions\n", results.PostgreSQLVersion, results.PassedTests, results.ExpectedTests, results.FailedTests, len(regressions))
	if len(regressions) > 0 {
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
