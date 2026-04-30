package transport

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gliedabrennung/raft/internal/raft"
)

type mockStorage struct{}

func (m *mockStorage) SaveState(term int, votedFor int, log []raft.LogEntry) error { return nil }
func (m *mockStorage) LoadState() (int, int, []raft.LogEntry, error)              { return 0, -1, nil, nil }

func TestHTTPTransport_Status(t *testing.T) {
	node := raft.NewNode(1, nil, &mockStorage{})
	transport := NewHTTPTransport(node)

	req, err := http.NewRequest("GET", "/status", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(transport.handleStatus)
	handler.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}

	if resp["id"].(float64) != 1 {
		t.Errorf("expected id 1, got %v", resp["id"])
	}
}

func TestHTTPTransport_Command(t *testing.T) {
	node := raft.NewNode(1, nil, &mockStorage{})
	// Manually set node state to Leader to allow commands
	// Note: In real scenarios, state transitions are handled by the node's loop.
	// For unit testing the handler, we force the state.
	// However, we can't easily set private state field from another package.
	// We might need to add a test-only helper or use a more integration-like approach.
	// For now, let's test the "not leader" case.

	transport := NewHTTPTransport(node)

	cmdReq := map[string]string{"command": "test-cmd"}
	body, _ := json.Marshal(cmdReq)
	req, err := http.NewRequest("POST", "/command", bytes.NewBuffer(body))
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(transport.handleCommand)
	handler.ServeHTTP(rr, req)

	// Since node is Follower by default, it should return Service Unavailable (503)
	if status := rr.Code; status != http.StatusServiceUnavailable {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusServiceUnavailable)
	}
}
