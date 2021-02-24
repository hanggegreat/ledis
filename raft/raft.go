package raft

import (
	"distributed-project/labrpc"
	"sync"
	"sync/atomic"
	//"6.824/labgob"
)

type Raft struct {
	// 通过锁来共享结点状态
	mu *sync.Mutex
	// 每个结点的 rpc end point
	peers []*labrpc.ClientEnd
	// 保存结点持久化状态的对象
	persister *Persister
	// 当前 peer 在 peers[] 中的 index
	me int
	// set by Kill()
	dead int32

	// current term
	curTerm  int
	votedFor int
	// log entries; each entry contains command for state machine, and term when entry was received by leader(first index is 1)
	logs []LogEntry

	// 结点状态(Follower, Candidate, Leader)
	state PeerState
	// 选举超时器
	timer *Timer
	// every Raft peer has a condition, use for trigger AppendEntries RPC
	syncConds []*sync.Cond

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

func (rf *Raft) GetState() (int, bool) {
	return rf.curTerm, rf.state == Leader
}

func (rf *Raft) isLeader() bool {
	return rf.state == Leader
}

// 成为 Term 为 term，Leader 为 voteFor 的 Follower
func (rf *Raft) back2Follower(term int, voteFor int) {
	rf.mu.Lock()
	rf.state = Follower
	rf.curTerm = term
	rf.votedFor = voteFor
	rf.resetElectTimer()
	rf.mu.Unlock()
}

// 创建并初始化 Raft
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.mu = &sync.Mutex{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.curTerm = 0
	rf.votedFor = VoteNil
	rf.state = Follower
	rf.timer = NewElectTimer()
	rf.dead = 0
	rf.logs = make([]LogEntry, 0)
	rf.syncConds = make([]*sync.Cond, len(peers))
	for i := range peers {
		rf.syncConds[i] = sync.NewCond(rf.mu)
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
