package shardmaster

import (
	"ledis/labrpc"
	"sync/atomic"
	"time"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	me int64
	cmdIndex int64
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers

	ck.me = nrand()
	ck.cmdIndex = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	atomic.AddInt64(&ck.cmdIndex, 1)
	args.Clerk = ck.me
	args.Index = int(ck.cmdIndex)
	args.Num = num
	for {
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	atomic.AddInt64(&ck.cmdIndex, 1)
	args := &JoinArgs{}
	args.Clerk = ck.me
	args.Index = int(ck.cmdIndex)
	args.Servers = servers

	for {
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	atomic.AddInt64(&ck.cmdIndex, 1)
	args := &LeaveArgs{}
	args.Clerk = ck.me
	args.Index = int(ck.cmdIndex)
	args.GIDs = gids

	for {
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	atomic.AddInt64(&ck.cmdIndex, 1)
	args := &MoveArgs{}
	args.Shard = shard
	args.Clerk = ck.me
	args.GID = gid
	args.Index = int(ck.cmdIndex)

	for {
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
