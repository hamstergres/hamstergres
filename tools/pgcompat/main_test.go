// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestIncompleteRunWritesReportBeforeFailing(t *testing.T) {
	tempDir := t.TempDir()
	binary := filepath.Join(tempDir, "pgcompat")
	build := exec.Command("go", "build", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build pgcompat: %v\n%s", err, output)
	}
	logPath := filepath.Join(tempDir, "pg_regress.log")
	schedulePath := filepath.Join(tempDir, "parallel_schedule")
	outputDir := filepath.Join(tempDir, "results")
	if err := os.WriteFile(logPath, []byte("ok 1 + boolean 10 ms\n1..2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(schedulePath, []byte("test: boolean foreign_key\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(binary,
		"-log", logPath,
		"-schedule", schedulePath,
		"-output", outputDir,
		"-postgresql-version", "17.10",
	)
	output, err := command.CombinedOutput()
	var exitError *exec.ExitError
	if !errors.As(err, &exitError) || exitError.ExitCode() != 2 {
		t.Fatalf("pgcompat exit = %v, output:\n%s", err, output)
	}
	for _, name := range []string{"results.json", "compatibility-report.md", filepath.Join("badges", "overall.json")} {
		if _, err := os.Stat(filepath.Join(outputDir, name)); err != nil {
			t.Fatalf("%s was not written before exit: %v\n%s", name, err, output)
		}
	}
	report, err := os.ReadFile(filepath.Join(outputDir, "compatibility-report.md"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(report), "Harness incomplete") || !strings.Contains(string(report), "`foreign_key`") {
		t.Fatalf("partial report:\n%s", report)
	}
}
