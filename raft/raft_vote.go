package raft

import (
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

// 选举超时器
type Timer struct {
	// 定时器
	t *time.Timer
	// 定时器间隔
	duration time.Duration
	// 生成一个随机时间间隔的函数
	genFunc func() time.Duration
}

func (timer *Timer) Stop() {
	timer.t.Stop()
}

func (timer *Timer) Reset() {
	timer.duration = timer.genFunc()
	if timer.t == nil {
		timer.t = time.NewTimer(timer.duration)
	} else {
		timer.t.Reset(timer.duration)
	}
}

func (timer *Timer) String() string {
	return fmt.Sprintf("RaftTimer(duration=%s)", timer.duration)
}

// 生成一个选举超时器
func NewElectTimer() *Timer {
	timer := &Timer{}
	timer.genFunc = genRandomRangeDuration
	timer.Reset()
	return timer
}

// 重置选举超时器
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

// 检查选举超时
func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		// 选举超时，成为 Candidate，进行投票请求
		case <-rf.timer.t.C:
			rf.resetElectTimer()
			rf.voteForMe()
		}
	}
}

func (rf *Raft) voteForMe() {
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
			go rf.heartbeat()
			go rf.sync()
			DPrintf("Vote|Win|%v", rf)
			return
		}
	}

	DPrintf("Vote|Split|%v", rf)
	rf.back2Follower(rf.curTerm, VoteNil)
}

// 请求投票给自己的 rpc 调用
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}
