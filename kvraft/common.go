package kvraft

import (
	"crypto/rand"
	"math/big"
)

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

type PutAppendArgs struct {
	Key   string
	Value string
	// Put 或者 Append
	Op string

	// 发送消息的 clerk 编号
	ClerkId int64
	// 这个 clerk 发送的消息编号，自增
	CmdIndex int
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// 发送消息的 clerk 编号
	ClerkId int64
	// 这个 clerk 发送的消息编号，自增
	CmdIndex int
}

type GetReply struct {
	// 请求的 server 不是 Leader 时为 true
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