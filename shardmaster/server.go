package shardmaster

import (
	"distributed-project/labgob"
	"distributed-project/labrpc"
	"distributed-project/raft"
	"sync"
	"time"
)

const (
	// 用于调整 shard
	tLess = 0
	tOk   = 1
	tPlus = 2
	tMore = 3
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// indexed by config num
	configs []Config

	// 每个 group 对应的 shard
	g2shard map[int][]int

	// 记录已经执行的 clerk 的命令，避免重复执行。
	clerkLog map[int64]int

	// 消息通知
	msgCh map[int]chan struct{}
}

type Op struct {
	Clerk    int64
	CmdIndex int64
	// join, leave, move, query
	Operation string
	// join, new GID -> servers mappings
	Servers map[int][]string
	// leave
	GIDs []int
	// move 此时 GID 为 GIDs[0]
	Shard int
	// query
	QueryNum int
}

func (sm *ShardMaster) run() {
	for msg := range sm.applyCh {
		sm.mu.Lock()
		index := msg.CommandIndex
		op := msg.Command.(Op)
		if idx, ok := sm.clerkLog[op.Clerk]; ok && idx > int(op.CmdIndex) {
			// 命令已经执行过了
		} else {
			switch op.Operation {
			case Join:
				flag := false
				sm.addConfig()
				cfg := sm.getLastCfg()
				for gid, svs := range op.Servers {
					// 不存在说明才是新加入的集群
					if _, ok := sm.g2shard[gid]; !ok{
						flag = true
						cfg.Groups[gid] = svs
						sm.g2shard[gid] = []int{}
					}
				}

				if flag {
					sm.naiveAssign()
					raft.ShardInfo.Printf("ShardMaster:%2d | new config:{%v}\n", sm.me, sm.getLastCfg())
				}
			case Leave:
				flag := false
				sm.addConfig()
				cfg := sm.getLastCfg()
				for _, gid := range op.GIDs {
					if svs, ok := sm.g2shard[gid]; ok {
						flag = true
						for _, shard := range svs {
							cfg.Shards[shard] = 0
						}
						delete(sm.g2shard, gid)
						delete(cfg.Groups, gid)
					}
				}

				if flag {
					sm.naiveAssign()
					raft.ShardInfo.Printf("ShardMaster:%2d | new config:{%v}\n", sm.me, sm.getLastCfg())
				}
			case Move:
				sm.addConfig()
				cfg := sm.getLastCfg()
				oldGid := cfg.Shards[op.Shard]
				for idx, shard := range sm.g2shard[oldGid] {
					if shard == op.Shard {
						sm.g2shard[oldGid] = append(sm.g2shard[oldGid][:idx], sm.g2shard[oldGid][idx+1:]...)
						break
					}
				}
				gid := op.GIDs[0]
				sm.g2shard[gid] = append(sm.g2shard[gid], op.Shard)
				cfg.Shards[op.Shard] = gid
			case Query:
			}

			if ch, ok := sm.msgCh[index]; ok {
				ch <- struct{}{}
			}
			sm.mu.Unlock()
		}
	}
}

func (sm *ShardMaster) executeOp(op Op) (res bool) {
	sm.mu.Lock()
	// 命令已经执行
	if cmdIdx, ok := sm.clerkLog[op.Clerk]; ok && cmdIdx >= int(op.CmdIndex) {
		sm.mu.Unlock()
		return false
	}

	sm.mu.Unlock()

	idx, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		return true
	}

	ch := make(chan struct{})
	sm.mu.Lock()
	sm.msgCh[idx] = ch
	sm.mu.Unlock()

	select {
	case <-time.After(raft.RpcCallTimeout):
		res = true
	case <-ch:
		res = false
	}

	go sm.closeCh(idx)

	return
}

func (sm *ShardMaster) addConfig() {
	oldCfg := sm.getLastCfg()
	groups := make(map[int][]string)
	for id, svs := range oldCfg.Groups {
		groups[id] = svs
	}
	sm.configs = append(sm.configs, Config{oldCfg.Num + 1, oldCfg.Shards, groups})
}

func (sm *ShardMaster) getLastCfg() *Config {
	return &sm.configs[len(sm.configs)-1]
}

func (sm *ShardMaster) getLastShard(gid int) int {
	return sm.g2shard[gid][len(sm.g2shard[gid])-1]
}

func (sm *ShardMaster) cutG2shard(gid int) []int {
	return sm.g2shard[gid][:len(sm.g2shard[gid])-1]
}

func (sm *ShardMaster) closeCh(index int) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	close(sm.msgCh[index])
	delete(sm.msgCh, index)
}

// 设 shard 总数为 s，集群总数为 g，那么每个集群至少应该有 t = floor(s / g) 个 shard，且理想情况下，每个集群应该只有 t 或者 t + 1 个 shard。
// 然而由于 move 的情况，有的集群可能会有 t + n(n >= 2)个 shard，同样的有的集群可能会有 t - n(n <= 1)个 shard，导致 shard 分配不均衡。
func (sm *ShardMaster) naiveAssign() {
	if len(sm.g2shard) == 0 {
		return
	}

	gMap := [4][]int{}

	cfg := sm.getLastCfg()
	shardToAssign := make([]int, 0)
	for shard, gid := range cfg.Shards {
		if gid == 0 {
			shardToAssign = append(shardToAssign, shard)
		}
	}

	share := NShards / len(sm.g2shard)
	for gid, shards := range sm.g2shard {
		l := len(shards)
		switch l {
		case share:
			gMap[tOk] = append(gMap[tOk], gid)
		case share + 1:
			gMap[tPlus] = append(gMap[tPlus], gid)
		default:
			if l < share {
				gMap[tLess] = append(gMap[tLess], gid)
			} else {
				gMap[tMore] = append(gMap[tMore], gid)
			}
		}
	}

	// 清空 tMore 平衡 shard 数量分配过多的 group
	for len(gMap[tMore]) != 0 {
		gid := gMap[tMore][0]
		moreShard := sm.getLastShard(gid)
		sm.g2shard[gid] = sm.cutG2shard(gid)
		shardToAssign = append(shardToAssign, moreShard)
		if len(sm.g2shard[gid]) == share+1 {
			gMap[tPlus] = append(gMap[tPlus], gid)
			gMap[tMore] = gMap[tMore][1:]
		}
	}

	// 有新的 shard 需要分配
	for len(shardToAssign) != 0 {
		curShard := shardToAssign[0]
		shardToAssign = shardToAssign[1:]
		if len(gMap[tLess]) != 0 {
			gid := gMap[tLess][0]
			cfg.Shards[curShard] = gid
			sm.g2shard[gid] = append(sm.g2shard[gid], curShard)
			if len(sm.g2shard[gid]) == share {
				// 判断该 gid 所分配的 shard 是否达到最小要求
				gMap[tOk] = append(gMap[tOk], gid)
				gMap[tLess] = gMap[tLess][1:]
			}
		} else {
			//给 tOk 的 group 分配 shard
			gid := gMap[tOk][0]
			cfg.Shards[curShard] = gid
			sm.g2shard[gid] = append(sm.g2shard[gid], curShard)
			gMap[tOk] = gMap[tOk][1:]
			gMap[tPlus] = append(gMap[tPlus], gid)
		}
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	raft.ShardInfo.Printf("ShardMaster:%2d join:%v\n", sm.me, args.Servers)
	op := Op{args.Clerk, int64(args.Index), Join, args.Servers, []int{}, 0, 0}
	reply.WrongLeader = sm.executeOp(op)
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	raft.ShardInfo.Printf("ShardMaster:%2d leave:%v\n", sm.me, args.GIDs)
	op := Op{args.Clerk, int64(args.Index), Leave, map[int][]string{}, args.GIDs, 0, 0}
	reply.WrongLeader = sm.executeOp(op)
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	raft.ShardInfo.Printf("ShardMaster:%2d move shard:%v to gid:%v\n", sm.me, args.Shard, args.GID)
	op := Op{args.Clerk, int64(args.Index), Move, map[int][]string{}, []int{args.GID}, args.Shard, 0}
	reply.WrongLeader = sm.executeOp(op)
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	op := Op{args.Clerk, int64(args.Index), Query, map[int][]string{}, []int{}, 0, args.Num}
	reply.WrongLeader = sm.executeOp(op)
	if !reply.WrongLeader {
		sm.mu.Lock()
		defer sm.mu.Unlock()
		if op.QueryNum == -1 || op.QueryNum > sm.configs[len(sm.configs)-1].Num {
			//所要求的config比拥有的config大，返回最新的config
			reply.Config = *sm.getLastCfg()
		} else {
			reply.Config = sm.configs[op.QueryNum]
		}
	}
}

func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	raft.ShardInfo.Printf("ShardMaster:%2d | I am died\n", sm.me)
}

func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	sm.g2shard = make(map[int][]int)
	sm.msgCh = make(map[int]chan struct{})
	sm.clerkLog = make(map[int64]int)
	raft.ShardInfo.Printf("ShardMaster:%2d |Create a new shardMaster!\n", sm.me)
	go sm.run()

	return sm
}
