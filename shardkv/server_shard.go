package shardkv

import (
	"ledis/raft"
	"ledis/shardmaster"
	"time"
)

// 接收到新的 shard
type NewShard struct {
	ShardDB  map[string]string
	ClerkLog map[int64]int
}

func (kv *ShardKV) broadShard() {
	for _, shard := range kv.shardsToSend {
		go kv.sendShard(shard, kv.cfg.Num)
	}
	raft.ShardInfo.Printf("GID:%2d me:%2d cfg:%2d leader:%6v|Begin to transfer {%v}\n", kv.gid, kv.me, kv.cfg.Num, kv.isLeader, kv.shardsToSend)
}

// 旧集群发送 shard 逻辑
func (kv *ShardKV) sendShard(shard int, cfgNum int) {

	for {
		kv.mu.Lock()

		if !kv.isLeader || !kv.shardInHand(shard) || kv.cfg.Num > cfgNum {
			kv.mu.Unlock()
			return
		}

		gid := kv.cfg.Shards[shard]
		kvDB := make(map[string]string)
		ckLog := make(map[int64]int)
		kv.copyKVDB(kv.skvDB[shard], kvDB)
		kv.copyClerkLog(kv.sClerkLog[shard], ckLog)

		args := PushShardArgs{cfgNum, shard, kvDB, ckLog, kv.gid}

		if servers, ok := kv.cfg.Groups[gid]; ok {
			raft.ShardInfo.Printf("GID:%2d me:%2d cfg:%2d leader:%6v| transfer shard %2d to gid:%2d args{%v}\n", kv.gid, kv.me, kv.cfg.Num, kv.isLeader, shard, gid, args)
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				var reply PushShardReply

				kv.mu.Unlock()

				ok := srv.Call("ShardKV.PushShard", &args, &reply)

				kv.mu.Lock()

				if !kv.isLeader {
					kv.mu.Unlock()
					return
				}

				if ok && reply.WrongLeader == false {
					raft.ShardInfo.Printf("GID:%2d me:%2d cfg:%2d leader:%6v| OK! transfer shard %2d to gid:%2d args{%v}\n", kv.gid, kv.me, kv.cfg.Num, kv.isLeader, shard, gid, args)
					kv.mu.Unlock()

					// 同步发送成功日志
					op := Op{
						newSend,
						shard,
						nil,
						shardmaster.Config{},
						nil,
						-1,
						-1,
					}
					wl, wg := kv.executeOp(op)

					if wl {
						return
					}

					if !wg {
						// 同步成功
						return
					}
					kv.mu.Lock()
					// 同步失败，继续
					break
				}
			}
			raft.ShardInfo.Printf("GID:%2d me:%2d cfg:%2d leader:%6v| Fail to transfer shard %2d to gid:%2d args{%v}\n", kv.gid, kv.me, kv.cfg.Num, kv.isLeader, shard, gid, args)
		}

		kv.mu.Unlock()
		// 发送失败，等等再尝试
		time.Sleep(SendShardWait)
	}
}

type PushShardArgs struct {
	CfgNum   int
	Shard    int
	ShardDB  map[string]string
	ClerkLog map[int64]int
	GID      int
}

type PushShardReply struct {
	WrongLeader bool
}

// 新集群接收shard逻辑
func (kv *ShardKV) PushShard(args *PushShardArgs, reply *PushShardReply) {
	kv.mu.Lock()

	raft.ShardInfo.Printf("GID:%2d me:%2d cfg:%2d leader:%6v| receive shard %2d from gid:%2d cfg:%2d need{%v} args{%v}\n", kv.gid, kv.me, kv.cfg.Num, kv.isLeader, args.Shard, args.GID, args.CfgNum, kv.needShards(), args)

	if kv.isLeader && args.CfgNum < kv.cfg.Num {
		reply.WrongLeader = false
		kv.mu.Unlock()
		return
	}
	if !kv.isLeader || args.CfgNum > kv.cfg.Num || !kv.haveShard(args.Shard) {
		reply.WrongLeader = true
		kv.mu.Unlock()
		return
	}

	if kv.shardInHand(args.Shard) {
		kv.mu.Unlock()
		reply.WrongLeader = false
		return
	}

	kv.mu.Unlock()
	op := Op{
		newShard,
		args.Shard,
		&NewShard{
			args.ShardDB, args.ClerkLog,
		},
		shardmaster.Config{},
		nil,
		-1,
		-1,
	}

	wrongLeader, wrongGroup := kv.executeOp(op)
	reply.WrongLeader = true
	if !wrongLeader && !wrongGroup {
		reply.WrongLeader = false
		raft.ShardInfo.Printf("GID:%2d me:%2d cfg:%2d leader:%6v| add new shard %d done! \n", kv.gid, kv.me, kv.cfg.Num, kv.isLeader, args.Shard)
	}

	return

}

// 配置中是否拥有该 shard
func (kv *ShardKV) haveShard(shard int) bool {
	_, ok := kv.shards[shard]
	return ok
}

// 是否已经拥有该 shard
func (kv *ShardKV) shardInHand(shard int) bool {
	_, ok := kv.skvDB[shard]
	return ok
}

// 需要接收的 shards
func (kv *ShardKV) needShards() (res []int) {
	res = make([]int, 0)
	for shard := range kv.shards {
		if _, ok := kv.skvDB[shard]; !ok {
			res = append(res, shard)
		}
	}
	return
}

func (kv *ShardKV) copyKVDB(src map[string]string, des map[string]string) {
	for k, v := range src {
		des[k] = v
	}
}

func (kv *ShardKV) copyClerkLog(src map[int64]int, des map[int64]int) {
	for k, v := range src {
		des[k] = v
	}
}

func (kv *ShardKV) copyShard(src map[int]struct{}) (des map[int]struct{}) {
	des = make(map[int]struct{})
	for k, v := range src {
		des[k] = v
	}
	return
}
