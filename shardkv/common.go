package shardkv

import (
	"crypto/rand"
	"math/big"
	"time"
)

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
	Put           = "Put"
	Append        = "Append"
	Get           = "Get"
	CmdOk         = true
	CmdFail       = false
	request       = "Request"
	newConfig     = "NewConfig"
	newShard      = "NewShard"
	newSend       = "newSend"
	newLeader     = "newLeader"
	CfgGetTime    = time.Duration(88) * time.Millisecond
	SendShardWait = time.Duration(30) * time.Millisecond
)

type Err string

type PutAppendArgs struct {
	Key   string
	Value string
	Op    string

	Clerk    int64
	CmdIndex int
	Shard    int
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string

	Clerk    int64
	CmdIndex int
	Shard    int
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}
