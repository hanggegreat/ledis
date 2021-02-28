package raft

import "sync"

type RequestVoteArgs struct {
	Term        int
	CandidateId int
	// Candidate 的最后一个日志 index
	LastLogIndex int
	// Candidate 的最后一个日志 Term
	LastLogTerm int
}

type RequestVoteReply struct {
	// 接收者的 currentTerm，一般是针对过期 Leader
	Term        int
	// 接收者投票之后会改变自己的 votedFor
	VoteGranted bool
}

//收到请求投票时的逻辑
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//if len(rf.logs) == 1 {
	//	reply.Term = rf.lastIncludedTerm
	//} else {
	//	reply.Term = rf.logs[len(rf.logs) - 1].Term
	//}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.convertRole(Follower)
	}

	// Candidate 日志比本节点的日志新
	newEntries := args.LastLogTerm > rf.getLastLogTerm() || (args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex >= rf.getLastLogIndex())
	// 投票回复可能会丢失
	votedNot := rf.votedFor == VoteNil || rf.votedFor == args.CandidateId

	if !newEntries || !votedNot {
		return
	}

	reply.VoteGranted = true
	rf.votedFor = args.CandidateId

	dropAndSet(rf.voteCh)
	WarnRaft.Printf("Raft:%d term:%d | vote to candidate %d\n", rf.me, rf.currentTerm, args.CandidateId)
	rf.persist()
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) leaderElection() {
	rf.mu.Lock()
	args := &RequestVoteArgs{rf.currentTerm, rf.me, rf.getLastLogIndex(), rf.getLastLogTerm()}
	rf.mu.Unlock()

	voteCnt := 1
	finished := false
	voteLock := sync.Mutex{}
	majority := 1 + len(rf.peers)/2

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		// 检查是否还是 Candidate
		rf.mu.Lock()
		if !rf.checkState(Candidate, args.Term) {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		go func(server int) {
			reply := &RequestVoteReply{}
			if ok := rf.sendRequestVote(server, args, reply); ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > rf.currentTerm {
					rf.convertRole(Follower)
					return
				}

				if !rf.checkState(Candidate, args.Term) || !reply.VoteGranted || finished {
					return
				}

				voteLock.Lock()
				defer voteLock.Unlock()

				voteCnt++
				if voteCnt >= majority {
					finished = true
					rf.convertRole(Leader)
					dropAndSet(rf.leaderCh)
				}
			}
		}(i)
	}
}
