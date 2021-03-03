package raft

import "sync"

type Entries struct {
	// 该日志所属的Term
	Term int
	// 该日志在 log 的 index
	Index   int
	Command interface{}
}

type AppendEntriesArgs struct {
	// Leader 的 Term
	Term     int
	LeaderId int
	// PreLogIndex 和 PrevLogTerm 用来确定 Leader 和收到这条信息的 Follower 上一条同步的信息，方便回滚
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entries
	LeaderCommit int
}

type AppendEntriesReply struct {
	// Follower 的 Term
	Term    int
	Success bool

	// 冲突日志 index
	ConflictIndex int
	// 冲突日志处 Term
	ConflictTerm int
}

func (rf *Raft) getLastLogIndex() int {
	return rf.addIdx(len(rf.logs) - 1)
}

func (rf *Raft) getLastLogTerm() int {
	return rf.logs[len(rf.logs)-1].Term
}

func (rf *Raft) getPrevLogIndex(server int) int {
	return rf.nextIndex[server] - 1
}

func (rf *Raft) getPrevLogTerm(server int) int {
	return rf.logs[rf.subIdx(rf.getPrevLogIndex(server))].Term
}

func (rf *Raft) subIdx(i int) int {
	return i - rf.lastIncludedIndex
}

func (rf *Raft) addIdx(i int) int {
	return i + rf.lastIncludedIndex
}

// Follower 追加日志的RPC调用
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//if len(rf.logs) == 1 {
	//	reply.Term = rf.lastIncludedTerm
	//} else {
	//	reply.Term = rf.logs[len(rf.logs) - 1].Term
	//}

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	// Leader 的 Term 失效了
	if args.Term < rf.currentTerm {
		return
	}

	// 需要更新 Term
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.convertRole(Follower)
	}

	dropAndSet(rf.appendCh)

	// Follower 日志少了，需要 Leader 将 nextIndex 设置为 conflictIndex
	if rf.getLastLogIndex() < args.PrevLogIndex {
		reply.ConflictIndex = rf.getLastLogIndex() + 1
		InfoRaft.Printf("Raft:%d term:%d | receive leader:[%d] message but lost any message! curLen:%d prevLoglen:%d len(Entries):%d\n", rf.me, rf.currentTerm, args.LeaderId, rf.getLastLogIndex(), args.PrevLogIndex, len(args.Entries))
		return
	}

	// 已经快照了
	if rf.lastIncludedIndex > args.PrevLogIndex {
		reply.ConflictIndex = rf.lastIncludedIndex + 1
		return
	}

	// 接收者日志大于等于 Leader 发来的日志 且日志项不匹配
	if args.PrevLogTerm != rf.logs[rf.subIdx(args.PrevLogIndex)].Term {
		reply.ConflictTerm = rf.logs[rf.subIdx(args.PrevLogIndex)].Term
		for i := args.PrevLogIndex; i > rf.lastIncludedIndex; i-- {
			if rf.logs[rf.subIdx(i)].Term != reply.ConflictTerm {
				break
			}
			reply.ConflictIndex = i
		}
		InfoRaft.Printf("Raft:%d term:%d | receive leader:[%d] message but not match!\n", rf.me, rf.currentTerm, args.LeaderId)
		return
	}

	// 接收者的日志大于等于 prevLogIndex，且在 prevLogIndex 处日志匹配
	// 修改防止 Follower 的 Term 大于 Leader 影响集群工作
	rf.currentTerm = args.Term
	reply.Success = true

	// 找到 Follower 和 Leader 第一个不相同的日志 index，将其及之后全部删除
	for i := range args.Entries {
		subIndex := rf.subIdx(i + args.PrevLogIndex + 1)
		if subIndex < len(rf.logs) && rf.logs[subIndex].Term != args.Entries[i].Term {
			rf.logs = rf.logs[:subIndex]
			rf.logs = append(rf.logs, args.Entries[i:]...)
			break
		} else if subIndex >= len(rf.logs) {
			rf.logs = append(rf.logs, args.Entries[i:]...)
			break
		}
	}

	if len(args.Entries) != 0 {
		InfoRaft.Printf("Raft:%d term:%d | receive new command from leader:%d, term:%d, size:%d curLogLen:%d LeaderCommit:%d rfCommit:%d\n", rf.me, rf.currentTerm, args.LeaderId, args.Term, len(args.Entries), len(rf.logs)-1, args.LeaderCommit, rf.commitIndex)
		rf.persist()
	}

	// 修改 commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.addIdx(len(rf.logs)-1))
		rf.applyLogs()
	}
}

func (rf *Raft) applyLogs() {
	InfoRaft.Printf("Raft:%d term:%d | start apply log curCommit:%d total:%d!\n", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex)
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[rf.subIdx(i)].Command,
			CommandIndex: i,
			CommitTerm:   rf.currentTerm,
			Role:         rf.role,
		}
	}

	InfoRaft.Printf("Raft:%d term:%d | apply log {%d => %d} Done!\n", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex)
	InfoRaft.Printf("Raft:%d term:%d | index{%d} cmd{%v}\n", rf.me, rf.currentTerm, rf.commitIndex, rf.logs[rf.subIdx(rf.commitIndex)].Command)
	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) broadcastEntries() {
	rf.mu.Lock()
	term := rf.currentTerm
	rf.mu.Unlock()

	finished := false
	majority := 1 + len(rf.peers)/2
	commitCnt := 1
	commitLock := sync.Mutex{}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) {
			for {
				rf.mu.Lock()
				if !rf.checkState(Leader, term) {
					rf.mu.Unlock()
					return
				}

				next := rf.nextIndex[server]
				// nextIndex 已经被快照了，需要发送完整日志
				if next <= rf.lastIncludedIndex {
					rf.sendSnapshot(server)
					return
				}

				appendArgs := &AppendEntriesArgs{
					term,
					rf.me,
					rf.getPrevLogIndex(server),
					rf.getPrevLogTerm(server),
					rf.logs[rf.subIdx(next):],
					rf.commitIndex,
				}

				rf.mu.Unlock()
				reply := &AppendEntriesReply{}

				ok := rf.sendAppendEntries(server, appendArgs, reply)

				if !ok {
					return
				}

				rf.mu.Lock()

				if reply.Term > term {
					rf.currentTerm = reply.Term
					rf.convertRole(Follower)
					InfoRaft.Printf("Raft:%d term:%d | leader done! become follower\n", rf.me, rf.currentTerm)
					rf.mu.Unlock()
					return
				}

				// 不是 Leader 或者 Term 发生修改 或者 还没有新的日志写入就不用提交
				if !rf.checkState(Leader, term) || len(appendArgs.Entries) == 0 {
					rf.mu.Unlock()
					return
				}

				if !reply.Success {
					// 缺日志了
					if reply.ConflictTerm == -1 {
						rf.nextIndex[server] = reply.ConflictIndex
					} else {
						// 日志不匹配
						rf.nextIndex[server] = rf.addIdx(1)
						i := reply.ConflictIndex
						for ; i > rf.lastIncludedIndex; i-- {
							if rf.logs[rf.subIdx(i)].Term == reply.ConflictTerm {
								rf.nextIndex[server] = i + 1
								break
							}
						}

						if i <= rf.lastIncludedIndex && rf.lastIncludedIndex != 0 {
							rf.nextIndex[server] = rf.lastIncludedIndex
						}
					}
				} else {
					curCommitLen := appendArgs.PrevLogIndex + len(appendArgs.Entries)

					// 已经提交过了，直接忽略
					if curCommitLen < rf.commitIndex {
						rf.mu.Unlock()
						return
					}

					if curCommitLen >= rf.matchIndex[server] {
						rf.matchIndex[server] = curCommitLen
						rf.nextIndex[server] = rf.matchIndex[server] + 1
					}

					commitLock.Lock()

					commitCnt++

					if finished || commitCnt < majority {
						commitLock.Unlock()
						rf.mu.Unlock()
						return
					}

					finished = true
					commitLock.Unlock()
					rf.commitIndex = curCommitLen
					rf.applyLogs()
				}

				InfoRaft.Printf("Raft:%d term:%d | Msg to %d fail,decrease nextIndex to:%d\n", rf.me, rf.currentTerm, server, rf.nextIndex[server])
				rf.mu.Unlock()
			}
		}(i)
	}
}
