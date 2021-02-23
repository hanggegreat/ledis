package raft

import (
	"distributed-project/labrpc"
	"fmt"
	"sync"
	"time"
)

type RequestVoteArgs struct {
	// 当前节点的 term
	Term int
	// 当前节点的 id
	CandidateID int
}

type RequestVoteReply struct {
	// 请求的节点的 term
	Term int
	// 请求的节点的节点是否同意投票给当前节点
	VoteGranted bool
}

type Timer struct {
	t        *time.Timer
	duration time.Duration
	genFunc  func() time.Duration
}

func (timer *Timer) Stop() {
	timer.t.Stop()
}

func (timer *Timer) Reset() {
	timer.duration = timer.genFunc()
	timer.t = time.NewTimer(timer.duration)
}

func (timer *Timer) String() string {
	return fmt.Sprintf("RaftTimer(duration=%s)", timer.duration)
}

func NewElectTimer() *Timer {
	timer := &Timer{}
	timer.genFunc = func() time.Duration {
		return time.Duration(RandomRange(400, 800)) * time.Millisecond
	}
	timer.Reset()
	return timer
}

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

	// persistent states
	curTerm  int
	votedFor int
	logs     []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader(first index is 1)

	// 结点状态(Follower, Candidate, Leader)
	state PeerState
	// 选举定时器
	timer     *Timer
	syncConds []*sync.Cond // every Raft peer has a condition, use for trigger AppendEntries RPC

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

func (rf *Raft) resetElectTimer() {
	rf.timer.Reset()
}

// 投票给 candidate 的 rpc 方法
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	reply.Term = rf.curTerm
	reply.VoteGranted = false

	if args.Term < rf.curTerm {
		return
	}

	if args.Term > rf.curTerm {
		rf.back2Follower(args.Term, VoteNil)
	}

	if rf.votedFor == VoteNil || rf.votedFor == args.CandidateID {
		reply.VoteGranted = true
		rf.back2Follower(args.Term, args.CandidateID)
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.timer.t.C:
			rf.resetElectTimer()
			rf.vote()
		}
	}
}

func (rf *Raft) back2Follower(term int, voteFor int) {
	rf.mu.Lock()
	rf.state = Follower
	rf.curTerm = term
	rf.votedFor = voteFor
	rf.mu.Unlock()
}

func (rf *Raft) vote() {
	DPrintf("Vote|Timeout|%v", rf)
	rf.curTerm++
	rf.state = Candidate
	rf.votedFor = rf.me

	args := RequestVoteArgs{
		Term:        rf.curTerm,
		CandidateID: rf.me,
	}

	replyCh := make(chan *RequestVoteReply, len(rf.peers)-1)
	wg := sync.WaitGroup{}

	for i := range rf.peers {
		if i == rf.me {
			rf.resetElectTimer()
			continue
		}

		wg.Add(1)
		go func(serverId int) {
			defer wg.Done()
			reply := RequestVoteReply{}
			respCh := make(chan struct{})
			go func() {
				rf.RequestVote(&args, &reply)
				respCh <- struct{}{}
			}()

			select {
			case <-time.After(RpcCallTimeout):
				return
			case <-respCh:
				replyCh <- &reply
			}
		}(i)
	}

	go func() {
		wg.Wait()
		close(replyCh)
	}()

	votes := 1
	majority := len(rf.peers)/2 + 1
	for reply := range replyCh {
		if reply.Term > rf.curTerm {
			DPrintf("Vote|Higher Term:%d|%v", reply.Term, rf)
			rf.back2Follower(reply.Term, VoteNil)
			return
		}

		if reply.VoteGranted {
			votes++
		}

		if votes >= majority {
			rf.state = Leader
			// TODO
			//go rf.heartbeat()
			//go rf.sync()
			DPrintf("Vote|Win|%v", rf)
			return
		}
	}

	DPrintf("Vote|Split|%v", rf)
	rf.back2Follower(rf.curTerm, VoteNil)

}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
