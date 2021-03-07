package shardkv

import (
	"distributed-project/labgob"
	"distributed-project/labrpc"
	"distributed-project/raft"
	"distributed-project/shardmaster"
	"sync"
)

type Op struct {
	// Request, NewShard or NewConfig
	Type  string

	Shard int

	NewShard  *NewShard
	NewConfig *shardmaster.Config
	Request   *Request

	Clerk    int64
	CmdIndex int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int

	// 用来接收请求执行结果
	msgCh map[int]chan MsgCh
	// 保存每个 shard 的 kvDB
	skvDB map[int]map[string]string
	// 保存每个 shard 的 AppendEntries
	sClerkLog map[int]map[int64]int
	// 用于获取配置
	sm *shardmaster.Clerk
	// 集群所管理的 shard
	shards map[int]struct{}
	// 配置更新后需要发送的 shards
	shardsToSend []int
	// 当前配置
	cfg *shardmaster.Config
	// 判断自己是不是 Leader
	isLeader  bool
	persister *raft.Persister
	// 等待本配置发送和接收 shard 完成
	waitUntilMoveDone int
	exitCh            chan struct{}
}

type MsgCh struct {
	isOk bool
	Op   Op
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.sm = shardmaster.MakeClerk(kv.masters)
	kv.msgCh = make(map[int]chan MsgCh)
	kv.sClerkLog = make(map[int]map[int64]int)
	kv.skvDB = make(map[int]map[string]string)
	kv.shards = make(map[int]struct{})

	kv.cfg = &shardmaster.Config{
		Num:    0,
		Shards: [10]int{},
		Groups: make(map[int][]string),
	}
	kv.shardsToSend = make([]int, 0)
	kv.persister = persister
	kv.isLeader = false
	// 防止阻塞，大小必须为 2,因为有两个 goroutine 需要关闭
	kv.exitCh = make(chan struct{}, 2)

	kv.waitUntilMoveDone = 0

	raft.ShardInfo.Printf("GID:%2d me:%2d -> Create a new server!\n", kv.gid, kv.me)

	kv.loadSnapshot()

	if kv.persister.SnapshotSize() != 0 {
		kv.parseCfg()
	}

	// 检查配置信息更新
	go kv.checkCfg()
	// 执行 option
	go kv.run()

	return kv
}

func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	kv.exitCh <- struct{}{}
	kv.exitCh <- struct{}{}
	raft.ShardInfo.Printf("GID:%2d me:%2d Cfg:%2d leader:%6v <- I am died!\n", kv.gid, kv.me, kv.cfg.Num, kv.isLeader)
}

func (kv *ShardKV) run() {
	for {
		select {
		case <-kv.exitCh:
			raft.ShardInfo.Printf("GID:%2d me:%2d Cfg:%2d leader:%6v <- close run!\n", kv.gid, kv.me, kv.cfg.Num, kv.isLeader)
			return
		case msg := <-kv.applyCh:
			kv.mu.Lock()
			index := msg.CommandIndex
			term := msg.CommitTerm

			if !msg.CommandValid {
				op := msg.Command.([]byte)
				kv.decodedSnapshot(op)
				// 恢复快照后，生成新的 shards
				kv.parseCfg()
				kv.mu.Unlock()
				continue
			}
			op := msg.Command.(Op)

			msgToChan := CmdOk
			switch op.Type {
			case newSend:
				if !kv.haveShard(op.Shard) && kv.shardInHand(op.Shard) {
					delete(kv.skvDB, op.Shard)
					delete(kv.sClerkLog, op.Shard)
					kv.waitUntilMoveDone--
				}
			case newConfig:
				// 更新配置
				nc := op.NewConfig
				// 防止一个 Cfg 更新两次
				if nc.Num > kv.cfg.Num {
					// 收到新的配置
					kv.cfg = nc
					kv.parseCfg()

					if nc.Num == 1 {
						kv.waitUntilMoveDone = 0
						raft.ShardInfo.Printf("GID:%2d me:%2d Cfg:%2d leader:%6v|Default initialize\n", kv.gid, kv.me, kv.cfg.Num, kv.isLeader)
						for shard := range kv.shards {
							kv.skvDB[shard] = make(map[string]string)
							kv.sClerkLog[shard] = make(map[int64]int)
						}
					}
				}
			case newShard:
				if !kv.haveShard(op.Shard) {
					msgToChan = CmdFail
				} else if !kv.shardInHand(op.Shard) {
					ns := op.NewShard
					kv.skvDB[op.Shard] = make(map[string]string)
					kv.sClerkLog[op.Shard] = make(map[int64]int)
					kv.copyKVDB(ns.ShardDB, kv.skvDB[op.Shard])
					kv.copyClerkLog(ns.ClerkLog, kv.sClerkLog[op.Shard])
					kv.waitUntilMoveDone--
				}
			case request:
				if ind, ok := kv.sClerkLog[op.Shard][op.Clerk]; ok && ind >= op.CmdIndex {
					raft.ShardInfo.Printf("GID:%2d me:%2d Cfg:%2d leader:%6v|Op{%v}have done! index:%4d \n", kv.gid, kv.me, kv.cfg.Num, kv.isLeader, op, index)
				} else if !kv.haveShard(op.Shard) || !kv.shardInHand(op.Shard) {
					raft.ShardInfo.Printf("GID:%2d me:%2d Cfg:%2d leader:%6v|Op{%v}no responsible! index:%4d \n", kv.gid, kv.me, kv.cfg.Num, kv.isLeader, op, index)
					msgToChan = CmdFail
				} else {
					// 执行指令
					raft.ShardInfo.Printf("GID:%2d me:%2d Cfg:%2d leader:%6v|apply Op{%v} index:%4d\n", kv.gid, kv.me, kv.cfg.Num, kv.isLeader, op, index)

					kv.sClerkLog[op.Shard][op.Clerk] = op.CmdIndex
					switch op.Type {
					case Put:
						kv.skvDB[op.Shard][op.Request.Key] = op.Request.Value
					case Append:
						if _, ok := kv.skvDB[op.Shard][op.Request.Key]; ok {
							kv.skvDB[op.Shard][op.Request.Key] = kv.skvDB[op.Shard][op.Request.Key] + op.Request.Value
						} else {
							kv.skvDB[op.Shard][op.Request.Key] = op.Request.Value
						}
					case Get:
					}
				}
			case newLeader:
				if kv.isLeader {
					raft.ShardInfo.Printf("GID:%2d me:%2d Cfg:%2d leader:%6v| I am new leader!\n", kv.gid, kv.me, kv.cfg.Num, kv.isLeader)
				}
			}
			if ch, ok := kv.msgCh[index]; ok {
				ch <- MsgCh{
					msgToChan,
					op,
				}
			} else if kv.isLeader {
				raft.ShardInfo.Printf("GID:%2d me:%2d Cfg:%2d leader:%6v| command{%v} no channel index:%2d!\n", kv.gid, kv.me, kv.cfg.Num, kv.isLeader, op, index)
			}

			kv.checkState(index, term)
			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) convertToLeader(isLeader bool) {
	oldLeader := kv.isLeader

	kv.isLeader = isLeader

	if kv.isLeader != oldLeader && kv.isLeader {
		raft.ShardInfo.Printf("GID:%2d me:%2d cfg:%2d | follower turn to leader!\n", kv.gid, kv.me, kv.cfg.Num)

		kv.mu.Unlock()
		op := Op{
			newLeader,
			1,
			nil,
			nil,
			nil,
			-1,
			-1,
		}
		wl, wg := kv.executeOp(op)
		kv.mu.Lock()
		if wl || wg {
			// 没同步空指令，不是 Leader
			kv.isLeader = false
			return
		}

		kv.parseCfg()
		kv.broadShard()
	}
}

// 判断执行成功的命令是否和发起的相同
func (kv *ShardKV) equal(begin, done Op) bool {
	return begin.Type == done.Type && begin.Shard == done.Shard && begin.Clerk == done.Clerk && begin.CmdIndex == done.CmdIndex
}

func (kv *ShardKV) checkLeader() (isLeader bool) {
	_, isLeader = kv.rf.GetState()
	return
}

func (kv *ShardKV) closeCh(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	close(kv.msgCh[index])
	delete(kv.msgCh, index)
}
