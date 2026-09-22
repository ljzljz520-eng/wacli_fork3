package main

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

func runLedger(t *testing.T, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	stdout = captureRootStdout(t, func() {
		stderr = captureRootStderr(t, func() {
			err = execute(args)
		})
	})
	return stdout, stderr, err
}

func TestLedgerStatusJSON(t *testing.T) {
	store := filepath.Join(t.TempDir(), "store")
	stdout, _, err := runLedger(t, "--store", store, "ledger", "status", "--json")
	if err != nil {
		t.Fatal(err)
	}
	var env struct {
		Success bool `json:"success"`
		Data    struct {
			HeadSeq int64 `json:"head_seq"`
			Views   []any `json:"views"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(stdout), &env); err != nil {
		t.Fatalf("status JSON = %q: %v", stdout, err)
	}
	if !env.Success || env.Data.HeadSeq != 0 || len(env.Data.Views) != 0 {
		t.Fatalf("unexpected status payload: %s", stdout)
	}
}

func TestLedgerStatusHuman(t *testing.T) {
	store := filepath.Join(t.TempDir(), "store")
	stdout, _, err := runLedger(t, "--store", store, "ledger", "status")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout, "HEAD_SEQ") || !strings.Contains(stdout, "RAW_EVENTS") {
		t.Fatalf("human status missing fields:\n%s", stdout)
	}
}

func TestLedgerVerifyClean(t *testing.T) {
	store := filepath.Join(t.TempDir(), "store")
	stdout, _, err := runLedger(t, "--store", store, "ledger", "verify")
	if err != nil {
		t.Fatalf("verify on fresh store should pass: %v", err)
	}
	if !strings.Contains(stdout, "OK") {
		t.Fatalf("verify output = %q", stdout)
	}
}

func TestLedgerWriteRefusedReadOnly(t *testing.T) {
	t.Setenv("WACLI_READONLY", "1")
	store := filepath.Join(t.TempDir(), "store")
	for _, sub := range []string{"rebuild", "promote", "rollback"} {
		_, stderr, err := runLedger(t, "--store", store, "ledger", sub)
		if err == nil {
			t.Fatalf("ledger %s should fail in read-only mode", sub)
		}
		if !strings.Contains(stderr, "read-only mode") {
			t.Fatalf("ledger %s error = %q", sub, stderr)
		}
	}
}

func TestLedgerHelpListsSubcommands(t *testing.T) {
	store := filepath.Join(t.TempDir(), "store")
	stdout, _, err := runLedger(t, "--store", store, "ledger", "--help")
	if err != nil {
		t.Fatal(err)
	}
	for _, sub := range []string{"status", "rebuild", "verify", "diff", "promote", "rollback"} {
		if !strings.Contains(stdout, sub) {
			t.Fatalf("ledger help missing %q:\n%s", sub, stdout)
		}
	}
}
