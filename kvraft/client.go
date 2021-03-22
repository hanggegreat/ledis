package kvraft

import (
	"ledis/labrpc"
	"ledis/raft"
	"sync/atomic"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// 记录最新的 Leader，方便下次通信
	leader int
	// 每个 clerk 的唯一编号，使用 nrand 生成的随机数
	me int64
	// clerk 持有的命令编号，自增
	cmdIndex int64
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.leader = 0
	ck.me = nrand()
	ck.cmdIndex = 0
	raft.InfoKV.Printf("Client:%20v | Create new clerk!\n", ck.me)
	return ck
}

// 从服务器获取键值
func (ck *Clerk) Get(key string) string {
	atomic.AddInt64(&ck.cmdIndex, 1)
	args := GetArgs{
		key,
		ck.me,
		int(ck.cmdIndex),
	}

	leader := ck.leader
	raft.InfoKV.Printf("Client:%20v cmdIndex:%4d| Begin! Get:[%v] from server:%3d\n", ck.me, ck.cmdIndex, key, leader)

	for {
		reply := GetReply{}
		if ok := ck.servers[leader].Call("KVServer.Get", &args, &reply); ok && !reply.WrongLeader {
			ck.leader = leader
			// 收到回复信息
			if reply.Value == ErrNoKey {
				return ""
			}

			raft.InfoKV.Printf("Client:%20v cmdIndex:%4d| Successful! Get:[%v] from server:%3d value:[%v]\n", ck.me, ck.cmdIndex, key, leader, reply.Value)
			return reply.Value
		}

		// 请求的不是 Leader，也有可能是 Leader 挂了没鸟我。。。
		leader = (leader + 1) % len(ck.servers)
	}
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	atomic.AddInt64(&ck.cmdIndex, 1)
	args := PutAppendArgs{
		key,
		value,
		op,
		ck.me,
		int(ck.cmdIndex),
	}

	leader := ck.leader

	for {
		reply := PutAppendReply{}
		if ok := ck.servers[leader].Call("KVServer.PutAppend", &args, &reply); ok && !reply.WrongLeader && reply.Err == OK {
			raft.InfoKV.Printf("Client:%20v cmdIndex:%4d| Successful! %6s key:[%s] value:[%s] to server:%3d\n", ck.me, ck.cmdIndex, op, key, value, leader)
			ck.leader = leader
			return
		}

		leader = (leader + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
