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
	LastIncludedIndex int
	LastIncludedTerm  int
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
	if rf.currentTerm > args.Term || rf.lastIncludedIndex >= args.LastIncludedIndex {
		InfoKV.Printf("Raft:%2d term:%3d | stale snapshot from leader:%2d | me:{index%4d term%4d} leader:{index%4d term%4d}", rf.me, rf.currentTerm, args.LeaderId, rf.lastIncludedIndex, rf.lastIncludedTerm, args.LastIncludedIndex, args.LastIncludedTerm)
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.convertRole(Follower)
	}

	dropAndSet(rf.appendCh)

	logs := make([]Entries, 0)
	// 直接从 rf.logs 截取新 logs
	if args.LastIncludedIndex <= rf.getLastLogIndex() {
		logs = append(logs, rf.logs[rf.subIdx(args.LastIncludedIndex):]...)
	} else {
		logs = append(logs, Entries{args.LastIncludedTerm, args.LastIncludedIndex, -1})
	}

	rf.logs = logs

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	rf.lastApplied = max(rf.lastIncludedIndex, rf.lastApplied)
	rf.commitIndex = max(rf.lastIncludedIndex, rf.commitIndex)

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
		return
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

	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = args.LastIncludedIndex
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
