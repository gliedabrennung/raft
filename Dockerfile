# Build stage
FROM golang:1.22-alpine AS builder

WORKDIR /app

COPY go.mod ./
# COPY go.sum ./ # Uncomment if you use go.sum

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /app/raft-node ./cmd/node

# Final stage
FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/raft-node /app/raft-node

COPY Raftfile /app/Raftfile

EXPOSE 8001 8002 8003 8004 8005

ENTRYPOINT ["/app/raft-node"]
CMD ["--id", "1", "--env", "production"]
