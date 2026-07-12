package main

import (
	"path/filepath"
	"testing"
)

func TestConfigureLoggingFailureIsReturnedForFailOpenStartup(t *testing.T) {
	closeLog, err := configureLogging(filepath.Join(t.TempDir(), "missing", "proxy.log"))
	if err == nil {
		if closeLog != nil {
			closeLog()
		}
		t.Fatal("configureLogging succeeded for a missing parent directory")
	}
}
