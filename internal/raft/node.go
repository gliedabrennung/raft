package raft

import (
	"log/slog"
	"math/rand"
	"time"
)

const (
	Follower State = iota
	Candidate
	Leader
)

func (n *Node) StateMachineLoop() {
	slog.Info("Starting state machine", "node", n.id)
	
	go n.applyCommitted()

	for {
		select {
		case <-n.stopCh:
			slog.Info("State machine stopped", "node", n.id)
			return
		default:
		}

		switch n.CurrentState() {
		case Follower:
			n.runFollower()
		case Candidate:
			n.runCandidate()
		case Leader:
			n.runLeader()
		}
	}
}

func (n *Node) Stop() {
	close(n.stopCh)
}

func (n *Node) runFollower() {
	timeout := randomElectionTimeout()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-timer.C:
			n.mu.Lock()
			slog.Info("Election timeout, becoming candidate", "node", n.id, "term", n.currentTerm+1)
			n.state = Candidate
			n.mu.Unlock()
			return
		case <-n.heartbeatCh:
			timer.Reset(randomElectionTimeout())
		}
	}
}

func (n *Node) runCandidate() {
	n.mu.Lock()
	n.currentTerm++
	n.votedFor = n.id
	n.persist()
	term := n.currentTerm
	n.mu.Unlock()

	slog.Info("Starting election", "node", n.id, "term", term)
	n.startElection(term)
}

func (n *Node) runLeader() {
	slog.Info("Became leader", "node", n.id, "term", n.CurrentTerm())
	
	n.mu.Lock()
	for _, p := range n.peers {
		n.nextIndex[p.ID] = len(n.log)
		n.matchIndex[p.ID] = -1
	}
	n.mu.Unlock()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	n.replicateLog()

	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			if n.CurrentState() != Leader {
				return
			}
			n.replicateLog()
		}
	}
}

func (n *Node) applyCommitted() {
	for {
		select {
		case <-n.stopCh:
			return
		default:
		}

		n.mu.Lock()
		if n.commitIndex > n.lastApplied {
			n.lastApplied++
			entry := n.log[n.lastApplied]
			n.mu.Unlock()

			slog.Info("Applying log entry", "node", n.id, "index", entry.Index, "command", entry.Command)
			n.applyCh <- entry
		} else {
			n.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func randomElectionTimeout() time.Duration {
	return time.Duration(300+rand.Intn(200)) * time.Millisecond
}
