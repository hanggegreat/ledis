package shardkv

import (
	"bytes"
	"distributed-project/labgob"
	"distributed-project/raft"
	"distributed-project/shardmaster"
)

func (kv *ShardKV) checkState(index int, term int) {
	if kv.maxraftstate == -1 || kv.persister.RaftStateSize() < kv.maxraftstate*3/4.0 {
		return
	}

	rawSnapshot := kv.encodeSnapshot()
	go func() { kv.rf.TakeSnapshot(rawSnapshot, index, term) }()
}

func (kv *ShardKV) encodeSnapshot() []byte {
	w := new(bytes.Buffer)
	enc := labgob.NewEncoder(w)
	enc.Encode(kv.skvDB)
	enc.Encode(kv.sClerkLog)
	enc.Encode(kv.cfg)
	return w.Bytes()
}

func (kv *ShardKV) loadSnapshot() {
	data := kv.persister.ReadSnapshot()
	if data == nil || len(data) == 0 {
		return
	}
	kv.decodedSnapshot(data)
}

func (kv *ShardKV) decodedSnapshot(data []byte) {
	r := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(r)

	var db map[int]map[string]string
	var cl map[int]map[int64]int
	var cfg shardmaster.Config

	if dec.Decode(&db) != nil || dec.Decode(&cl) != nil || dec.Decode(&cfg) != nil {
		raft.ShardInfo.Printf("GID:%2d me:%2d leader:%6v| KV Failed to recover by snapshot!\n", kv.gid, kv.me, kv.isLeader)
	} else {
		kv.skvDB = db
		kv.sClerkLog = cl
		kv.cfg = cfg
		raft.ShardInfo.Printf("GID:%2d me:%2d Cfg:%2d leader:%6v| KV recover from snapshot successful! \n", kv.gid, kv.me, kv.cfg.Num, kv.isLeader)
	}
}
