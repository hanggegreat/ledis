package shardkv

import (
	"distributed-project/raft"
	"distributed-project/shardmaster"
	"time"
)

// 周期性检查配置信息
func (kv *ShardKV) checkCfg() {
	for {
		select {
		case <-kv.exitCh:
			raft.ShardInfo.Printf("GID:%2d me:%2d Cfg:%2d leader:%6v <- close checkCfg!\n", kv.gid, kv.me, kv.cfg.Num, kv.isLeader)
			return
		default:
			isLeader := kv.checkLeader()
			kv.mu.Lock()
			kv.convertToLeader(isLeader)
			if kv.isLeader && kv.waitUntilMoveDone == 0 {
				kv.getCfg()
			}
			kv.mu.Unlock()
			time.Sleep(CfgGetTime)
		}
	}
}

func (kv *ShardKV) parseCfg() {
	kv.shards = make(map[int]struct{})
	needAdd := make(map[int]struct{})

	for shard, gid := range kv.cfg.Shards {
		if gid == kv.gid {
			kv.shards[shard] = struct{}{}
			if !kv.shardInHand(shard) {
				needAdd[shard] = struct{}{}
			}
		}
	}

	raft.ShardInfo.Printf("GID:%2d me:%2d Cfg:%2d leader:%6v| New shards:%v\n", kv.gid, kv.me, kv.cfg.Num, kv.isLeader, kv.shards)

	// 待发送列表
	shardToSend := make([]int, 0)
	for shard := range kv.skvDB {
		if !kv.haveShard(shard) {
			shardToSend = append(shardToSend, shard)
		}
	}

	kv.shardsToSend = shardToSend
	kv.waitUntilMoveDone = len(shardToSend) + len(needAdd)

	raft.ShardInfo.Printf("GID:%2d me:%2d Cfg:%2d leader:%6v| total receive/send %d newAdd:{%v} | New shardToSend:%v\n", kv.gid, kv.me, kv.cfg.Num, kv.isLeader, kv.waitUntilMoveDone, needAdd, kv.shardsToSend)
}

func (kv *ShardKV) getCfg() {
	//调用函数默认有锁
	var cfg shardmaster.Config
	//按序执行配置更新
	cfg = kv.sm.Query(kv.cfg.Num + 1)
	for {
		if !kv.isLeader || cfg.Num <= kv.cfg.Num {
			return
		}
		raft.ShardInfo.Printf("GID:%2d me:%2d Cfg:%2d leader:%6v| Update config to %d\n", kv.gid, kv.me, kv.cfg.Num, kv.isLeader, cfg.Num)

		// 同步新配置
		op := Op{
			newConfig,
			1,
			nil,
			cfg,
			nil,
			1,
			1,
		}

		kv.mu.Unlock()

		wl, wg := kv.executeOp(op)
		kv.mu.Lock()

		if !wl && !wg {
			raft.ShardInfo.Printf("GID:%2d me:%2d Cfg:%2d leader:  true| Update to Cfg:%2d successful!\n", kv.gid, kv.me, kv.cfg.Num, kv.cfg.Num)
			// 更新配置成功
			kv.broadShard()
			return
		}
	}
}
