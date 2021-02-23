package raft

import "time"

const VoteNil = -1

type PeerState int

const (
	Follower = 0
	Candidate
	Leader
)

const RpcCallTimeout = time.Second
