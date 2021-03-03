package raft

import (
	"bytes"
	"distributed-project/labgob"
	"time"
)

type InstallSnapshotArgs struct {
	// Leader 的 Term
	Term              int
	LeaderId          int
	lastIncludedIndex int
	lastIncludedTerm  int
	// snapshot
	Data []byte
}

type InstallSnapshotReply struct {
	Term int
}

// 保存快照
func (rf *Raft) TakeSnapshot(snapshot []byte, appliedId int, term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 快照数据已经存在了，直接忽略
	if appliedId <= rf.lastIncludedIndex {
		return
	}

	newLogs := make([]Entries, 0)
	newLogs = append(newLogs, rf.logs[rf.subIdx(appliedId):]...)
	rf.logs = newLogs
	rf.lastIncludedTerm = term
	rf.lastIncludedIndex = appliedId
	rf.persistStateAndSnapshot(snapshot)
}

// 接收快照
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	InfoKV.Printf("Raft:%2d term:%3d | receive snapshot from leader:%2d ", rf.me, rf.currentTerm, args.LeaderId)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if rf.currentTerm > reply.Term || rf.lastIncludedIndex >= args.lastIncludedIndex {
		InfoKV.Printf("Raft:%2d term:%3d | stale snapshot from leader:%2d | me:{index%4d term%4d} leader:{index%4d term%4d}", rf.me, rf.currentTerm, args.LeaderId, rf.lastIncludedIndex, rf.lastIncludedTerm, args.lastIncludedIndex, args.lastIncludedTerm)
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.convertRole(Follower)
	}

	dropAndSet(rf.appendCh)

	logs := make([]Entries, 0)
	// 直接从 rf.logs 截取新 logs
	if args.lastIncludedIndex <= rf.getLastLogIndex() {
		logs = append(logs, rf.logs[rf.subIdx(args.lastIncludedIndex):]...)
	} else {
		logs = append(logs, Entries{args.lastIncludedTerm, args.lastIncludedIndex, -1})
	}

	rf.logs = logs
	rf.lastIncludedIndex = args.lastIncludedIndex
	rf.lastIncludedTerm = args.lastIncludedTerm

	rf.lastApplied = max(rf.lastApplied, args.lastIncludedIndex)
	rf.commitIndex = max(rf.commitIndex, args.lastIncludedIndex)

	rf.persistStateAndSnapshot(args.Data)

	rf.applyCh <- ApplyMsg{
		CommandValid: false,
		Command:      args.Data,
		CommandIndex: rf.lastIncludedIndex,
		CommitTerm:   rf.lastIncludedTerm,
		Role:         rf.role,
	}

	InfoKV.Printf("Raft:%2d term:%3d | Install snapshot Done!\n", rf.me, rf.currentTerm)
}

func (rf *Raft) sendSnapshot(server int) {
	InfoKV.Printf("Raft:%2d term:%3d | Leader send snapshot{index:%4d term:%4d} to follower %2d\n", rf.me, rf.currentTerm, rf.lastIncludedIndex, rf.lastIncludedTerm, server)

	rf.mu.Lock()
	args := InstallSnapshotArgs{
		rf.currentTerm,
		rf.me,
		rf.lastIncludedIndex,
		rf.lastIncludedTerm,
		rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()

	replyCh := make(chan struct{})
	reply := InstallSnapshotReply{}

	go func() {
		if ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply); ok {
			replyCh <- struct{}{}
		}
	}()

	select {
	case <-time.After(RpcCallTimeout):
		InfoKV.Printf("Raft:%2d term:%3d | Timeout! Leader send snapshot to follower %2d failed\n", args.LeaderId, args.Term, server)
	case <-replyCh:
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.convertRole(Follower)
		return
	}

	// 已不再是 Leader
	if !rf.checkState(Leader, args.Term) {
		return
	}

	rf.nextIndex[server] = args.lastIncludedIndex + 1
	rf.matchIndex[server] = args.lastIncludedIndex
}

// 持久化 raft 层的状态和 server 层的快照
func (rf *Raft) persistStateAndSnapshot(snapshot []byte) {
	w := new(bytes.Buffer)
	enc := labgob.NewDecoder(w)
	enc.Decode(rf.currentTerm)
	enc.Decode(rf.votedFor)
	enc.Decode(rf.logs)
	enc.Decode(rf.lastIncludedIndex)
	enc.Decode(rf.lastIncludedTerm)
	rf.persister.SaveStateAndSnapshot(w.Bytes(), snapshot)
}
