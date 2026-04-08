#!/bin/sh
set -e
HASH=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BTIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
CGO_ENABLED=0 go build -ldflags "-X main.gitHash=$HASH -X main.buildTime=$BTIME" -o "${1:-mygnoscan}" .
