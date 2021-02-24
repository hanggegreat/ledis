package raft

import "time"

// 将 Leader 发送个 Follower 的命令封装为 LogEntry
type LogEntry struct {
	Term    int
	Command interface{}
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// 心跳 rpc 请求参数
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
}

// 心跳 rpc 响应结果
type AppendEntriesReply struct {
	Term int
}

func (rf *Raft) sync() {
	for i := range rf.peers {
		if i == rf.me {
			// Leader 需要重置自己的选举超时器
			rf.resetElectTimer()
			continue
		}

		go func(server int) {
			for {
				rf.mu.Lock()
				rf.syncConds[server].Wait()
				args := AppendEntriesArgs{
					Term:         rf.curTerm,
					LeaderId:     rf.me,
					PrevLogIndex: 0,
					PrevLogTerm:  0,
					Entries:      []LogEntry{},
				}

				rf.mu.Unlock()

				var reply AppendEntriesReply
				replyCh := make(chan struct{})

				go func() {
					rf.sendAppendEntries(server, &args, &reply)
					replyCh <- struct{}{}
				}()

				select {
				case <-time.After(RpcCallTimeout):
					continue
				case <-replyCh:
				}

				if reply.Term > rf.curTerm {
					rf.back2Follower(reply.Term, VoteNil)
					return
				}
			}
		}(i)
	}
}

// 定时发送心跳
func (rf *Raft) heartbeat() {
	ch := time.Tick(HeartbeatInterval)

	for {
		if !rf.isLeader() {
			return
		}

		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			rf.syncConds[i].Broadcast()
		}

		<-ch
	}
}

// 接收 Leader 心跳的 rpc 方法
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Term = rf.curTerm

	if rf.curTerm > args.Term {
		return
	}

	rf.back2Follower(args.Term, args.LeaderId)
}

// 发送心跳给 Follower 的 rpc 调用
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}
