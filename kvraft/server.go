package kvraft

import (
	"bytes"
	"distributed-project/labgob"
	"distributed-project/labrpc"
	"distributed-project/raft"
	"sync"
	"time"
)

type Op struct {
	// Put, Append or Get
	Method string
	Key    string
	Value  string
	// ClerkId
	Clerk int64
	// Clerk 的命令 Index
	Index int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// 阈值，日志长度到达一定阈值就会生成快照
	maxraftstate int

	// 保存每一个 clerk 执行过的最新编号
	clerkLog map[int64]int
	// kv db
	kvDB map[string]string
	// 消息通知管道
	msgCh     map[int]chan int
	persister *raft.Persister
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		"Get",
		args.Key,
		"",
		args.ClerkId,
		args.CmdIndex,
	}

	reply.Err = ErrNoKey
	reply.WrongLeader = true

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		return
	}

	kv.mu.Lock()
	// 指令已经执行过了哦
	if ind, ok := kv.clerkLog[args.ClerkId]; ok && ind >= args.CmdIndex {
		reply.Value = kv.kvDB[args.Key]
		kv.mu.Unlock()
		reply.WrongLeader = false
		reply.Err = OK
		return
	}

	raft.InfoKV.Printf("KVServer:%2d | leader msgIndex:%4d\n", kv.me, index)

	ch := make(chan int)
	kv.msgCh[index] = ch
	kv.mu.Unlock()

	select {
	case <-time.After(raft.RpcCallTimeout):
		// 超时还没有提交，多半是废了
		raft.InfoKV.Printf("KVServer:%2d |Get {index:%4d term:%4d} failed! Timeout!\n", kv.me, index, term)
	case msgTerm := <-ch:
		if msgTerm == term {
			// 命令执行
			kv.mu.Lock()
			raft.InfoKV.Printf("KVServer:%2d | Get {index:%4d term:%4d} OK!\n", kv.me, index, term)
			if val, ok := kv.kvDB[args.Key]; ok {
				reply.Value = val
				reply.Err = OK
			}
			kv.mu.Unlock()
			reply.WrongLeader = false
		} else {
			raft.InfoKV.Printf("KVServer:%2d |Get {index:%4d term:%4d} failed! Not leader any more!\n", kv.me, index, term)
		}
	}

	go func() {
		kv.closeCh(index)
	}()

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		args.Op,
		args.Key,
		args.Value,
		args.ClerkId,
		args.CmdIndex,
	}

	reply.Err = OK
	kv.mu.Lock()

	if ind, ok := kv.clerkLog[args.ClerkId]; ok && ind >= args.CmdIndex {
		kv.mu.Unlock()
		reply.WrongLeader = false
		return
	}

	kv.mu.Unlock()

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	kv.mu.Lock()
	raft.InfoKV.Printf("KVServer:%2d | leader msgIndex:%4d\n", kv.me, index)
	ch := make(chan int)
	kv.msgCh[index] = ch
	kv.mu.Unlock()

	reply.WrongLeader = true
	select {
	case <-time.After(raft.RpcCallTimeout):
		raft.InfoKV.Printf("KVServer:%2d | Put {index:%4d term:%4d} Failed, timeout!\n", kv.me, index, term)
	case msgTerm := <-ch:
		if msgTerm == term {
			raft.InfoKV.Printf("KVServer:%2d | Put {index:%4d term:%4d} OK!\n", kv.me, index, term)
			reply.WrongLeader = false
		} else {
			raft.InfoKV.Printf("KVServer:%2d | Put {index:%4d term:%4d} Failed, not leader!\n", kv.me, index, term)
		}
	}

	go func() {
		kv.closeCh(index)
	}()
}

func (kv *KVServer) closeCh(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	close(kv.msgCh[index])
	delete(kv.msgCh, index)
}

func (kv *KVServer) Kill() {
	kv.rf.Kill()
	kv.mu.Lock()
	raft.InfoKV.Printf("KVServer:%2d | KV server is died!\n", kv.me)
	kv.mu.Unlock()
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.kvDB = make(map[string]string)
	kv.clerkLog = make(map[int64]int)
	kv.msgCh = make(map[int]chan int)
	kv.persister = persister

	kv.loadSnapshot()

	raft.InfoKV.Printf("KVServer:%2d | Create New KV server!\n", kv.me)

	go kv.receiveNewMsg()

	return kv
}

// 接收来自 raft 层的消息，执行响应的操作，改变 kvDB
func (kv *KVServer) receiveNewMsg() {
	for msg := range kv.applyCh {
		kv.mu.Lock()
		// 按需执行指令
		index := msg.CommandIndex
		term := msg.CommitTerm

		if !msg.CommandValid {
			op := msg.Command.([]byte)
			kv.decodedSnapshot(op)
			kv.mu.Unlock()
			continue
		}

		op := msg.Command.(Op)

		if ind, ok := kv.clerkLog[op.Clerk]; !ok || ind < op.Index {
			kv.clerkLog[op.Clerk] = op.Index
			switch op.Method {
			case "Put":
				kv.kvDB[op.Key] = op.Value
			case "Append":
				if _, ok := kv.kvDB[op.Key]; ok {
					kv.kvDB[op.Key] = kv.kvDB[op.Key] + op.Value
				} else {
					kv.kvDB[op.Key] = op.Value
				}
			case "Get":
			}
		}

		if ch, ok := kv.msgCh[index]; ok {
			ch <- term
		}

		kv.checkState(index, term)
		kv.mu.Unlock()
	}
}

func (kv *KVServer) loadSnapshot() {
	data := kv.persister.ReadSnapshot()
	if data == nil || len(data) == 0 {
		return
	}

	kv.decodedSnapshot(data)
}

func (kv *KVServer) decodedSnapshot(data []byte) {
	r := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(r)

	var kvDB map[string]string
	var clerkLog map[int64]int

	if e1, e2 := dec.Decode(&kvDB), dec.Decode(&clerkLog); e1 != nil || e2 != nil {
		raft.InfoKV.Printf("e1:%v,e2:%v\n", e1, e2)
		raft.InfoKV.Printf("KVServer:%2d | KV Failed to recover by snapshot!\n", kv.me)
	} else {
		kv.kvDB = kvDB
		kv.clerkLog = clerkLog
		raft.InfoKV.Printf("KVServer:%2d | KV recover frome snapshot successful! \n", kv.me)
	}
}

func (kv *KVServer) encodeSnapshot() []byte {
	w := new(bytes.Buffer)
	enc := labgob.NewEncoder(w)
	enc.Encode(kv.kvDB)
	enc.Encode(kv.clerkLog)
	return w.Bytes()
}

func (kv *KVServer) checkState(index int, term int) {
	if kv.maxraftstate == -1 {
		return
	}

	if kv.persister.RaftStateSize() < kv.maxraftstate*2/3.0 {
		return
	}

	data := kv.encodeSnapshot()
	go func() {
		kv.rf.TakeSnapshot(data, index, term)
	}()
}
