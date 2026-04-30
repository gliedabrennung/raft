package storage

import (
	"encoding/json"
	"os"

	"github.com/gliedabrennung/raft/internal/raft"
)

type JSONStore struct {
	filePath string
}

type persistedState struct {
	Term     int             `json:"term"`
	VotedFor int             `json:"voted_for"`
	Log      []raft.LogEntry `json:"log"`
}

func NewJSONStore(filePath string) *JSONStore {
	return &JSONStore{
		filePath: filePath,
	}
}

func (s *JSONStore) SaveState(term int, votedFor int, log []raft.LogEntry) error {
	state := persistedState{
		Term:     term,
		VotedFor: votedFor,
		Log:      log,
	}

	data, err := json.Marshal(state)
	if err != nil {
		return err
	}

	return os.WriteFile(s.filePath, data, 0644)
}

func (s *JSONStore) LoadState() (int, int, []raft.LogEntry, error) {
	data, err := os.ReadFile(s.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, -1, nil, nil
		}
		return 0, -1, nil, err
	}

	var state persistedState
	if err := json.Unmarshal(data, &state); err != nil {
		return 0, -1, nil, err
	}

	return state.Term, state.VotedFor, state.Log, nil
}
