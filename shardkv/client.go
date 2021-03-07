package shardkv

import (
	"distributed-project/labrpc"
	"distributed-project/shardmaster"
	"sync/atomic"
)
import "time"


func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

type Clerk struct {
	sm       *shardmaster.Clerk
	config   shardmaster.Config
	make_end func(string) *labrpc.ClientEnd

	me int64
	cmdIndex int64
}

func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(masters)
	ck.make_end = make_end
	ck.me = nrand()
	ck.cmdIndex = 0
	return ck
}

func (ck *Clerk) Get(key string) string {
	atomic.AddInt64(&ck.cmdIndex, 1)
	args := GetArgs{}
	args.Key = key
	args.Clerk = ck.me
	args.CmdIndex = int(ck.cmdIndex)

	shard := key2shard(key)
	args.Shard = shard

	for {
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && reply.WrongLeader == false && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	atomic.AddInt64(&ck.cmdIndex, 1)
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.Clerk = ck.me
	args.CmdIndex = int(ck.cmdIndex)

	shard := key2shard(key)
	args.Shard = shard

	for {
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.WrongLeader == false && reply.Err == OK {
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
