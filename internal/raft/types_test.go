package raft

import (
	"testing"

	"github.com/gliedabrennung/raft/internal/config"
)

type mockStorage struct{}

func (m *mockStorage) SaveState(term int, votedFor int, log []LogEntry) error { return nil }
func (m *mockStorage) LoadState() (int, int, []LogEntry, error)              { return 0, -1, nil, nil }

func TestHandleRequestVote(t *testing.T) {
	peers := []config.Peer{{ID: 2, Address: "localhost:8002"}}
	node := NewNode(1, peers, &mockStorage{})

	tests := []struct {
		name        string
		currentTerm int
		votedFor    int
		log         []LogEntry
		args        RequestVoteArgs
		wantGranted bool
		wantTerm    int
	}{
		{
			name:        "Reject older term",
			currentTerm: 2,
			votedFor:    -1,
			args:        RequestVoteArgs{Term: 1, CandidateId: 2},
			wantGranted: false,
			wantTerm:    2,
		},
		{
			name:        "Grant vote in new term",
			currentTerm: 1,
			votedFor:    -1,
			args:        RequestVoteArgs{Term: 2, CandidateId: 2, LastLogIndex: -1, LastLogTerm: -1},
			wantGranted: true,
			wantTerm:    2,
		},
		{
			name:        "Reject if already voted",
			currentTerm: 2,
			votedFor:    3,
			args:        RequestVoteArgs{Term: 2, CandidateId: 2, LastLogIndex: -1, LastLogTerm: -1},
			wantGranted: false,
			wantTerm:    2,
		},
		{
			name:        "Reject if log is not up to date",
			currentTerm: 1,
			votedFor:    -1,
			log:         []LogEntry{{Index: 0, Term: 1, Command: "cmd"}},
			args:        RequestVoteArgs{Term: 1, CandidateId: 2, LastLogIndex: -1, LastLogTerm: -1},
			wantGranted: false,
			wantTerm:    1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node.mu.Lock()
			node.currentTerm = tt.currentTerm
			node.votedFor = tt.votedFor
			node.log = tt.log
			node.mu.Unlock()

			reply := node.HandleRequestVote(tt.args)

			if reply.VoteGranted != tt.wantGranted {
				t.Errorf("got granted %v, want %v", reply.VoteGranted, tt.wantGranted)
			}
			if reply.Term != tt.wantTerm {
				t.Errorf("got term %d, want %d", reply.Term, tt.wantTerm)
			}
		})
	}
}

func TestHandleAppendEntries(t *testing.T) {
	peers := []config.Peer{{ID: 2, Address: "localhost:8002"}}
	node := NewNode(1, peers, &mockStorage{})

	tests := []struct {
		name        string
		currentTerm int
		log         []LogEntry
		args        AppendEntriesArgs
		wantSuccess bool
		wantLogLen  int
	}{
		{
			name:        "Reject older term",
			currentTerm: 2,
			args:        AppendEntriesArgs{Term: 1},
			wantSuccess: false,
			wantLogLen:  0,
		},
		{
			name:        "Successful heartbeat",
			currentTerm: 1,
			args:        AppendEntriesArgs{Term: 1, PrevLogIndex: -1, PrevLogTerm: -1},
			wantSuccess: true,
			wantLogLen:  0,
		},
		{
			name:        "Reject if prev log index doesn't exist",
			currentTerm: 1,
			args:        AppendEntriesArgs{Term: 1, PrevLogIndex: 0, PrevLogTerm: 1},
			wantSuccess: false,
			wantLogLen:  0,
		},
		{
			name:        "Successful append",
			currentTerm: 1,
			args:        AppendEntriesArgs{Term: 1, PrevLogIndex: -1, PrevLogTerm: -1, Entries: []LogEntry{{Index: 0, Term: 1, Command: "cmd"}}},
			wantSuccess: true,
			wantLogLen:  1,
		},
		{
			name:        "Inconsistency - conflict",
			currentTerm: 1,
			log:         []LogEntry{{Index: 0, Term: 1, Command: "cmd"}},
			args:        AppendEntriesArgs{Term: 1, PrevLogIndex: -1, PrevLogTerm: -1, Entries: []LogEntry{{Index: 0, Term: 2, Command: "new_cmd"}}},
			wantSuccess: true,
			wantLogLen:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node.mu.Lock()
			node.currentTerm = tt.currentTerm
			node.log = tt.log
			node.mu.Unlock()

			reply := node.HandleAppendEntries(tt.args)

			if reply.Success != tt.wantSuccess {
				t.Errorf("got success %v, want %v", reply.Success, tt.wantSuccess)
			}
			if len(node.log) != tt.wantLogLen {
				t.Errorf("got log len %d, want %d", len(node.log), tt.wantLogLen)
			}
		})
	}
}
