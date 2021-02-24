package raft

import (
	"log"
	"math/rand"
	"time"
)

const VoteNil = -1

type PeerState int

const (
	Follower = iota
	Candidate
	Leader
)

const RpcCallTimeout = time.Second
const HeartbeatInterval = 100 * time.Millisecond

const MinElectionDuration = 400
const MaxElectionDuration = 800

// Debugging
const Debug = true

func DInfo(format string, a ...interface{}) {
	log.Printf(format, a...)
	return
}

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func init() {
	rand.Seed(time.Now().UnixNano())
	log.SetFlags(log.Lmicroseconds)
}

// Generate int with range in [from, to)
func RandomRange(from, to int) int {
	return rand.Intn(to-from) + from
}

func genRandomRangeDuration() time.Duration {
	return time.Duration(RandomRange(MinElectionDuration, MaxElectionDuration)) * time.Millisecond
}
