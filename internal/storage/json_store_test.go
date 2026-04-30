package storage

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/gliedabrennung/raft/internal/raft"
)

func TestJSONStore_SaveAndLoadState(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test_state.json")
	store := NewJSONStore(filePath)

	term := 5
	votedFor := 1
	log := []raft.LogEntry{
		{Index: 0, Term: 1, Command: "cmd1"},
		{Index: 1, Term: 5, Command: "cmd2"},
	}

	if err := store.SaveState(term, votedFor, log); err != nil {
		t.Fatalf("SaveState failed: %v", err)
	}

	gotTerm, gotVotedFor, gotLog, err := store.LoadState()
	if err != nil {
		t.Fatalf("LoadState failed: %v", err)
	}

	if gotTerm != term {
		t.Errorf("expected term %d, got %d", term, gotTerm)
	}
	if gotVotedFor != votedFor {
		t.Errorf("expected votedFor %d, got %d", votedFor, gotVotedFor)
	}
	if !reflect.DeepEqual(gotLog, log) {
		t.Errorf("expected log %+v, got %+v", log, gotLog)
	}
}

func TestJSONStore_LoadState_NoFile(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "non_existent.json")
	store := NewJSONStore(filePath)

	gotTerm, gotVotedFor, gotLog, err := store.LoadState()
	if err != nil {
		t.Fatalf("LoadState failed: %v", err)
	}

	if gotTerm != 0 {
		t.Errorf("expected term 0, got %d", gotTerm)
	}
	if gotVotedFor != -1 {
		t.Errorf("expected votedFor -1, got %d", gotVotedFor)
	}
	if gotLog != nil {
		t.Errorf("expected nil log, got %+v", gotLog)
	}
}

func TestJSONStore_LoadState_CorruptFile(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "corrupt.json")
	if err := os.WriteFile(filePath, []byte("invalid json"), 0644); err != nil {
		t.Fatal(err)
	}
	store := NewJSONStore(filePath)

	_, _, _, err := store.LoadState()
	if err == nil {
		t.Error("expected error loading corrupt file, got nil")
	}
}
