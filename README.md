# Raft Implementation in Go

This project is an implementation of the Raft consensus algorithm in Go. It supports leader election, log replication, state persistence, and flexible configuration via a `Raftfile`.

## Features

- **Leader Election:** Implemented with randomized timeouts to ensure cluster stability.
- **Election Restriction:** Nodes only vote for candidates with a log that is as up-to-date as their own.
- **Log Replication:** The leader replicates entries to all followers and commits them once a majority acknowledgment is received.
- **Persistence:** Node state (`currentTerm`, `votedFor`, `log`) is saved to JSON files on disk, allowing nodes to recover after a crash.
- **Raftfile:** Support for environment sections (e.g., `[local]`, `[production]`) for easy switching between configurations (WIP).
- **Multi-node Runner:** Ability to start the entire cluster with a single command for local development.
- **Docker Support:** Ready for containerization with a provided Dockerfile.
- **Unit Testing:** Comprehensive tests for configuration, Raft core logic, and storage.

## Project Structure

- `cmd/node/`: The entry point for the application.
- `internal/raft/`: Core Raft algorithm logic (states, RPC handlers, replication).
- `internal/config/`: `Raftfile` parsing and configuration management.
- `internal/storage/`: Persistent state storage logic.
- `internal/transport/`: HTTP transport layer for node communication.

## Quick Start

### 1. Build the Project

```bash
go build -o raft-node ./cmd/node
```

### 2. Run the Entire Cluster Locally

The easiest way to test the project is to start all nodes simultaneously:

```bash
go run ./cmd/node --all --env local
```
*This will start all services defined in the `[local]` section of your `Raftfile`.*

### 3. Run a Single Node

If you want to run nodes in separate terminals or on different servers:

```bash
# In terminal 1
go run ./cmd/node --id 1 --env local

# In terminal 2
go run ./cmd/node --id 2 --env local
```

## Using the API

### Check Status
Check the node's current role and term:
```bash
curl http://localhost:8001/status
```

### Submit a Command
Append a message to the cluster's log (send to the leader):
```bash
curl -X POST http://localhost:8001/command -d '{"command": "my message"}'
```

## Testing

Run all project tests with a coverage report:
```bash
go test -v -cover ./...
```

## Docker

Build the image:
```bash
docker build -t raft-node .
```

Run a container:
```bash
docker run -p 8001:8001 raft-node --id 1 --env production
```

## Raftfile Example

Example of a `Raftfile` configuration:
```ini
[local]
1 127.0.0.1:8001
2 127.0.0.1:8002
3 127.0.0.1:8003
4 127.0.0.1:8004
5 127.0.0.1:8005

[production]
1 raft-1.internal:8001
2 raft-2.internal:8001
3 raft-3.internal:8001
```
