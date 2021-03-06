package shardmaster

import (
	"crypto/rand"
	"math/big"
)

// The number of shards.
const NShards = 10

type Config struct {
	// 配置编号
	Num int
	// shard -> gid
	Shards [NShards]int
	// gid -> servers[]
	Groups map[int][]string
}

const (
	// 该日志对应的操作
	Join  = "Join"
	Leave = "Leave"
	Move  = "Move"
	Query = "Query"
)

const (
	OK = "OK"
)

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Err string

type JoinArgs struct {
	// new GID -> servers mappings
	Servers map[int][]string
	Clerk int64
	Index int
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs  []int
	Clerk int64
	Index int
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int
	Clerk int64
	Index int
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num   int
	Clerk int64
	Index int
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
