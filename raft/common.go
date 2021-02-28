package raft

import (
	"math/rand"
	"time"
)

const (
	Follower            = "Follower"
	Candidate           = "Candidate"
	Leader              = "Leader"
	VoteNil             = -1
	HeartbeatInterval   = 100 * time.Millisecond
	RpcCallTimeout      = 800 * time.Millisecond
	MinElectionDuration = 300
	MaxElectionDuration = 450
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// range in [from, to)
func RandomRange(from, to int) int {
	return rand.Intn(to-from) + from
}

//获取随机时间，用于选举
func electionTimeout() time.Duration {
	return time.Duration(RandomRange(MinElectionDuration, MaxElectionDuration)) * time.Millisecond
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a < b {
		return b
	}
	return a
}
