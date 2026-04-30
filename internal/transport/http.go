package transport

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gliedabrennung/raft/internal/raft"
)

type HTTPTransport struct {
	node *raft.Node
}

func NewHTTPTransport(node *raft.Node) *HTTPTransport {
	return &HTTPTransport{node: node}
}

func (t *HTTPTransport) RegisterHandlers(mux *http.ServeMux) {
	mux.HandleFunc("POST /append-entries", t.handleAppendEntries)
	mux.HandleFunc("POST /request-vote", t.handleRequestVote)
	mux.HandleFunc("GET /status", t.handleStatus)
	mux.HandleFunc("POST /command", t.handleCommand)
}

func (t *HTTPTransport) handleAppendEntries(w http.ResponseWriter, r *http.Request) {
	var args raft.AppendEntriesArgs
	if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	reply := t.node.HandleAppendEntries(args)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(reply)
}

func (t *HTTPTransport) handleRequestVote(w http.ResponseWriter, r *http.Request) {
	var args raft.RequestVoteArgs
	if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	reply := t.node.HandleRequestVote(args)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(reply)
}

func (t *HTTPTransport) handleStatus(w http.ResponseWriter, r *http.Request) {
    status := map[string]any{
        "id":    t.node.ID(),
        "state": fmt.Sprintf("%s", t.node.CurrentState()),
        "term":  t.node.CurrentTerm(),
    }
    
    statusStr, _ := json.Marshal(status)
	w.Header().Set("Content-Type", "application/json")
	w.Write(statusStr)
}

func (t *HTTPTransport) handleCommand(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Command string `json:"command"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	entry, err := t.node.SubmitCommand(req.Command)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(entry)
}
