package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

func (n *Node) replicateLog() {
	n.mu.RLock()
	term := n.currentTerm
	leaderID := n.id
	peers := n.peers
	n.mu.RUnlock()

	for _, p := range peers {
		go n.replicateTo(p.ID, p.Address, term, leaderID)
	}
}

func (n *Node) replicateTo(peerID int, addr string, term int, leaderID int) {
	n.mu.RLock()
	if n.state != Leader {
		n.mu.RUnlock()
		return
	}

	nextIdx := n.nextIndex[peerID]
	var entries []LogEntry
	if nextIdx < len(n.log) {
		entries = n.log[nextIdx:]
	}

	prevLogIndex := nextIdx - 1
	prevLogTerm := -1
	if prevLogIndex >= 0 {
		prevLogTerm = n.log[prevLogIndex].Term
	}

	args := AppendEntriesArgs{
		Term:         term,
		LeaderID:     leaderID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	n.mu.RUnlock()

	reply, err := sendAppendEntries(addr, args)
	if err != nil {
		slog.Debug("AppendEntries failed", "addr", addr, "err", err)
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader || n.currentTerm != term {
		return
	}

	if reply.Term > n.currentTerm {
		n.currentTerm = reply.Term
		n.state = Follower
		n.votedFor = -1
		n.persist()
		return
	}

	if reply.Success {
		n.nextIndex[peerID] = nextIdx + len(entries)
		n.matchIndex[peerID] = n.nextIndex[peerID] - 1
		n.advanceCommitIndex()
	} else {
		n.nextIndex[peerID]--
	}
}

func sendAppendEntries(addr string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	body, err := json.Marshal(args)
	if err != nil {
		return AppendEntriesReply{}, err
	}

	url := fmt.Sprintf("http://%s/append-entries", addr)
	client := &http.Client{Timeout: 100 * time.Millisecond}

	resp, err := client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return AppendEntriesReply{}, err
	}
	defer resp.Body.Close()

	var reply AppendEntriesReply
	if err := json.NewDecoder(resp.Body).Decode(&reply); err != nil {
		return AppendEntriesReply{}, err
	}

	return reply, nil
}

func (n *Node) advanceCommitIndex() {
	for i := len(n.log) - 1; i > n.commitIndex; i-- {
		if n.log[i].Term == n.currentTerm {
			matchCount := 1
			for _, p := range n.peers {
				if n.matchIndex[p.ID] >= i {
					matchCount++
				}
			}
			if matchCount > len(n.peers)/2 {
				n.commitIndex = i
				return
			}
		}
	}
}

func (n *Node) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := AppendEntriesReply{Term: n.currentTerm, Success: false}

	if args.Term < n.currentTerm {
		return reply
	}

	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.state = Follower
		n.votedFor = -1
		n.persist()
	}

	select {
	case n.heartbeatCh <- struct{}{}:
	default:
	}

	if args.PrevLogIndex >= 0 {
		if args.PrevLogIndex >= len(n.log) {
			return reply
		}
		if n.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			return reply
		}
	}

	insertIdx := args.PrevLogIndex + 1
	for i, entry := range args.Entries {
		if insertIdx+i < len(n.log) && n.log[insertIdx+i].Term != entry.Term {
			n.log = n.log[:insertIdx+i]
		}
		if insertIdx+i == len(n.log) {
			n.log = append(n.log, entry)
		}
	}
	if len(args.Entries) > 0 {
		n.persist()
	}

	if args.LeaderCommit > n.commitIndex {
		lastNewIndex := insertIdx + len(args.Entries) - 1
		if args.LeaderCommit < lastNewIndex {
			n.commitIndex = args.LeaderCommit
		} else {
			n.commitIndex = lastNewIndex
		}
	}

	reply.Success = true
	reply.Term = n.currentTerm
	return reply
}

func (n *Node) SubmitCommand(command string) (LogEntry, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader {
		return LogEntry{}, fmt.Errorf("not leader")
	}

	entry := LogEntry{
		Index:   len(n.log),
		Term:    n.currentTerm,
		Command: command,
	}

	n.log = append(n.log, entry)
	n.persist()
	
	for _, p := range n.peers {
		if n.matchIndex[p.ID] == -1 {
			n.nextIndex[p.ID] = len(n.log)
		}
	}

	return entry, nil
}
