package shardkv

import (
	"distributed-project/raft"
	"distributed-project/shardmaster"
	"time"
)

// Request
type Request struct {
	Operation string
	Key       string
	Value     string
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	wrongLeader, wrongGroup := kv.executeOp(
		Op{
			request,
			args.Shard,
			nil,
			shardmaster.Config{},
			&Request{
				Get,
				args.Key,
				"",
			},
			args.Clerk,
			args.CmdIndex,
		},
	)
	reply.WrongLeader = wrongLeader
	if wrongGroup {
		reply.Err = ErrWrongGroup
		return
	}

	if wrongLeader {
		return
	}

	kv.mu.Lock()
	if val, ok := kv.skvDB[args.Shard][args.Key]; ok {
		reply.Value = val
		reply.Err = OK
	} else {
		reply.Value = ""
		reply.Err = ErrNoKey
	}
	kv.mu.Unlock()
	return
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	var op string
	if args.Op == "Put" {
		op = Put
	} else {
		op = Append
	}
	wrongLeader, wrongGroup := kv.executeOp(
		Op{
			request,
			args.Shard,
			nil,
			shardmaster.Config{},
			&Request{
				op,
				args.Key,
				args.Value,
			},
			args.Clerk,
			args.CmdIndex,
		},
	)
	reply.WrongLeader = wrongLeader
	reply.Err = OK
	if wrongGroup {
		reply.Err = ErrWrongGroup
	}
	return
}

func (kv *ShardKV) executeOp(op Op) (wrongLeader, wrongGroup bool) {
	// 执行命令逻辑
	index := 0
	isLeader := false
	isOp := op.Type == request
	wrongLeader = true
	wrongGroup = true

	if isOp {
		kv.mu.Lock()
		// 判断指令需要的 shard 加载完成没有
		if kv.isLeader {
			if !kv.haveShard(op.Shard) || !kv.shardInHand(op.Shard) {
				raft.ShardInfo.Printf("GID:%2d me%2d cfg:%2d leader:%6v| Do not responsible for this shard %2d\n", kv.gid, kv.me, kv.cfg.Num, kv.isLeader, op.Shard)
				raft.ShardInfo.Printf("responsible:{%v} need{%v}\n", kv.shards, kv.needShards())

				raft.ShardInfo.Printf("cfg:%d ==> %v\n\n", kv.cfg.Num, kv.cfg.Shards)
				kv.mu.Unlock()
				return
			}
			if ind, ok := kv.sClerkLog[op.Shard][op.Clerk]; ok && ind >= op.CmdIndex {
				// 指令已经执行过
				raft.ShardInfo.Printf("GID:%2d me:%2d cfg:%2d leader:%6v| Command{%v} have done before\n", kv.gid, kv.me, kv.cfg.Num, kv.isLeader, op)
				wrongLeader, wrongGroup = false, false
				kv.mu.Unlock()
				return
			}
		}
		kv.mu.Unlock()
	}
	wrongGroup = false

	index, _, isLeader = kv.rf.Start(op)

	kv.mu.Lock()
	kv.convertToLeader(isLeader)
	if !isLeader {
		kv.mu.Unlock()
		return
	}
	wrongLeader = false

	ch := make(chan MsgCh, 1) // 必须为 1,防止阻塞
	kv.msgCh[index] = ch
	kv.mu.Unlock()
	raft.ShardInfo.Printf("GID:%2d me:%2d cfg:%2d leader:%6v| command{%v} begin! index:%2d\n", kv.gid, kv.me, kv.cfg.Num, kv.isLeader, op, index)

	select {
	case <-time.After(raft.RpcCallTimeout):
		wrongLeader = true
		raft.ShardInfo.Printf("GID:%2d me:%2d cfg:%2d leader:%6v| command{%v} index:%2d timeout!\n", kv.gid, kv.me, kv.cfg.Num, kv.isLeader, op, index)
	case res := <-ch:
		if res.isOk == CmdFail {
			//不再负责这个shard
			raft.ShardInfo.Printf("GID:%2d me:%2d cfg:%2d leader:%6v| exclude command{%v} index:%2d!\n", kv.gid, kv.me, kv.cfg.Num, kv.isLeader, op, index)
			wrongGroup = true
		} else if !kv.equal(res.Op, op) {
			wrongLeader = true
			raft.ShardInfo.Printf("GID:%2d me:%2d cfg:%2d leader:%6v| command{%v} index:%2d different between post and apply!\n", kv.gid, kv.me, kv.cfg.Num, kv.isLeader, op, index)
		} else {
			raft.ShardInfo.Printf("GID:%2d me:%2d cfg:%2d leader:%6v| command{%v} index:%2d Done!\n", kv.gid, kv.me, kv.cfg.Num, kv.isLeader, op, index)
		}
	}

	go kv.closeCh(index)
	return
}

// 判断执行成功的命令是否和发起的相同
func (kv *ShardKV) equal(begin, done Op) bool {
	return begin.Type == done.Type && begin.Shard == done.Shard && begin.Clerk == done.Clerk && begin.CmdIndex == done.CmdIndex
}