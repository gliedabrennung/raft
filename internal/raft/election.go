package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"time"
)

func (n *Node) startElection(term int) {
	voteCh := make(chan bool, len(n.peers))

	n.mu.RLock()
	lastLogIndex := len(n.log) - 1
	lastLogTerm := -1
	if lastLogIndex >= 0 {
		lastLogTerm = n.log[lastLogIndex].Term
	}
	n.mu.RUnlock()

	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  uint64(n.id),
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	for _, p := range n.peers {
		go func(addr string) {
			granted := n.sendRequestVote(addr, args)
			voteCh <- granted
		}(p.Address)
	}

	votesReceived := 1
	majority := (len(n.peers)+1)/2 + 1

	timeout := time.Duration(300+rand.Intn(200)) * time.Millisecond
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-n.stopCh:
			return

		case granted := <-voteCh:
			if granted {
				votesReceived++
				slog.Info("Vote received", "node", n.id, "votes", votesReceived, "needed", majority)
				if votesReceived >= majority {
					n.mu.Lock()
					if n.state == Candidate && n.currentTerm == term {
						n.state = Leader
					}
					n.mu.Unlock()
					return
				}
			}

		case <-timer.C:
			slog.Info("Election timed out", "node", n.id, "term", term)
			n.mu.Lock()
			n.state = Follower
			n.mu.Unlock()
			return

		case <-n.heartbeatCh:
			slog.Info("Heartbeat received during election, stepping down", "node", n.id)
			n.mu.Lock()
			n.state = Follower
			n.mu.Unlock()
			return
		}
	}
}

func (n *Node) sendRequestVote(addr string, args RequestVoteArgs) bool {
	body, err := json.Marshal(args)
	if err != nil {
		slog.Error("Failed to marshal RequestVote", "err", err)
		return false
	}

	url := fmt.Sprintf("http://%s/request-vote", addr)
	client := &http.Client{Timeout: 200 * time.Millisecond}

	resp, err := client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		slog.Debug("RequestVote RPC failed", "addr", addr, "err", err)
		return false
	}
	defer resp.Body.Close()

	var reply RequestVoteReply
	if err := json.NewDecoder(resp.Body).Decode(&reply); err != nil {
		slog.Error("Failed to decode RequestVote reply", "addr", addr, "err", err)
		return false
	}

	n.mu.Lock()
	if reply.Term > n.currentTerm {
		n.currentTerm = reply.Term
		n.state = Follower
		n.votedFor = -1
		n.persist()
	}
	n.mu.Unlock()

	return reply.VoteGranted
}

func (n *Node) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := RequestVoteReply{Term: n.currentTerm, VoteGranted: false}

	if args.Term < n.currentTerm {
		return reply
	}

	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.state = Follower
		n.votedFor = -1
	}

	lastLogIndex := len(n.log) - 1
	lastLogTerm := -1
	if lastLogIndex >= 0 {
		lastLogTerm = n.log[lastLogIndex].Term
	}

	logIsUpToDate := args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	if (n.votedFor == -1 || n.votedFor == int(args.CandidateId)) && logIsUpToDate {
		n.votedFor = int(args.CandidateId)
		reply.VoteGranted = true
		reply.Term = n.currentTerm
	}

	return reply
}
