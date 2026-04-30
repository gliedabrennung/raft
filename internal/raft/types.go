package raft

import (
	"sync"

	"github.com/gliedabrennung/raft/internal/config"
)

type Storage interface {
	SaveState(term int, votedFor int, log []LogEntry) error
	LoadState() (term int, votedFor int, log []LogEntry, err error)
}

type State int

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

type Node struct {
	mu    sync.RWMutex
	id    int
	state State
	peers []config.Peer

	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  map[int]int
	matchIndex map[int]int

	heartbeatCh chan struct{}
	stopCh      chan struct{}
	applyCh     chan LogEntry

	storage Storage
}

func NewNode(id int, peers []config.Peer, store Storage) *Node {
	n := &Node{
		id:          id,
		state:       Follower,
		peers:       peers,
		currentTerm: 0,
		votedFor:    -1,
		log:         make([]LogEntry, 0),
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make(map[int]int),
		matchIndex:  make(map[int]int),
		heartbeatCh: make(chan struct{}, 1),
		stopCh:      make(chan struct{}),
		applyCh:     make(chan LogEntry, 100),
		storage:     store,
	}

	if store != nil {
		term, votedFor, log, err := store.LoadState()
		if err == nil {
			n.currentTerm = term
			n.votedFor = votedFor
			if log != nil {
				n.log = log
			}
		}
	}

	return n
}

func (n *Node) persist() {
	if n.storage != nil {
		_ = n.storage.SaveState(n.currentTerm, n.votedFor, n.log)
	}
}

func (n *Node) ID() int {
	return n.id
}

func (n *Node) CurrentState() State {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state
}

func (n *Node) CurrentTerm() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.currentTerm
}

func (n *Node) NotifyHeartbeat(leaderTerm int) {
	n.mu.Lock()
	if leaderTerm >= n.currentTerm {
		n.currentTerm = leaderTerm
		n.state = Follower
		n.votedFor = -1
		n.persist()
	}
	n.mu.Unlock()

	select {
	case n.heartbeatCh <- struct{}{}:
	default:
	}
}

type LogEntry struct {
	Index   int    `json:"index"`
	Term    int    `json:"term"`
	Command string `json:"command"`
}

type RequestVoteArgs struct {
	Term         int    `json:"term"`
	CandidateId  uint64 `json:"candidate_id"`
	LastLogIndex int    `json:"last_log_index"`
	LastLogTerm  int    `json:"last_log_term"`
}

type RequestVoteReply struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"vote_granted"`
}

type AppendEntriesArgs struct {
	Term         int        `json:"term"`
	LeaderID     int        `json:"leader_id"`
	PrevLogIndex int        `json:"prev_log_index"`
	PrevLogTerm  int        `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit int        `json:"leader_commit"`
}

type AppendEntriesReply struct {
	Term    int  `json:"term"`
	Success bool `json:"success"`
}
