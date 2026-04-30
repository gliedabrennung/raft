package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"

	"github.com/gliedabrennung/raft/internal/config"
	"github.com/gliedabrennung/raft/internal/raft"
	"github.com/gliedabrennung/raft/internal/storage"
	"github.com/gliedabrennung/raft/internal/transport"
)

func main() {
	nodeID := flag.Int("id", 0, "Unique node ID")
	runAll := flag.Bool("all", false, "Run all nodes defined in the Raftfile simultaneously")
	raftfile := flag.String("raftfile", "./Raftfile", "Path to the Raftfile")
	env := flag.String("env", "local", "Environment to load from Raftfile")
	flag.Parse()

	if !*runAll && *nodeID <= 0 {
		slog.Error("You must specify either --id <positive integer> or --all flag.")
		os.Exit(1)
	}

	cfg, err := config.ParseRaftfile(*raftfile)
	if err != nil {
		slog.Error("Failed to parse Raftfile", "path", *raftfile, "err", err)
		os.Exit(1)
	}

	if *runAll {
		peers, err := cfg.GetEnv(*env)
		if err != nil {
			slog.Error("Environment not found in Raftfile", "env", *env, "err", err)
			os.Exit(1)
		}

		slog.Info("Starting ALL nodes from Raftfile", "env", *env, "count", len(peers))
		
		var wg sync.WaitGroup
		for _, self := range peers {
			otherPeers := cfg.GetOtherPeers(*env, self.ID)
			wg.Add(1)
			go startNode(self, otherPeers, &wg)
		}
		
		slog.Info("All nodes are running in the background. Press Ctrl+C to stop.")
		wg.Wait()
		
	} else {
		self, ok := cfg.GetPeer(*env, *nodeID)
		if !ok {
			slog.Error("Node ID not found in Raftfile for env", "id", *nodeID, "env", *env)
			os.Exit(1)
		}

		otherPeers := cfg.GetOtherPeers(*env, *nodeID)
		
		var wg sync.WaitGroup
		wg.Add(1)
		startNode(self, otherPeers, &wg)
		wg.Wait()
	}
}

func startNode(self config.Peer, otherPeers []config.Peer, wg *sync.WaitGroup) {
	defer wg.Done()

	slog.Info("Node configuration loaded",
		"id", self.ID,
		"address", self.Address,
		"peers", fmt.Sprintf("%v", otherPeers),
	)

	storeFilePath := fmt.Sprintf("node_%d_state.json", self.ID)
	store := storage.NewJSONStore(storeFilePath)

	node := raft.NewNode(self.ID, otherPeers, store)

	mux := http.NewServeMux()
	httpTransport := transport.NewHTTPTransport(node)
	httpTransport.RegisterHandlers(mux)

	go node.StateMachineLoop()

	slog.Info("Starting HTTP server", "id", self.ID, "address", self.Address)
	if err := http.ListenAndServe(self.Address, mux); err != nil {
		slog.Error("HTTP server failed", "id", self.ID, "err", err)
	}
}
