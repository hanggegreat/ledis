package raft

import (
	"bytes"
	"distributed-project/labgob"
	"distributed-project/labrpc"
	"sync"
	"time"
)

type ApplyMsg struct {
	// true 为正常信息交付，false 为快照
	CommandValid bool
	Command      interface{}
	CommandIndex int
	// 交付信息时 raft 的 term
	CommitTerm int
	// 交付信息时 raft 的角色
	Role string
}

// 每一个 raft peer 都叫 server，分为 Leader, Candidate, Follower 三种角色
type Raft struct {
	// 通过锁来共享结点状态
	mu *sync.Mutex
	// 每个结点的 rpc end point
	peers []*labrpc.ClientEnd
	// 保存结点持久化状态的对象
	persister *Persister
	// 当前 peer 在 peers[] 中的 index
	me int
	// Leader, Candidate or Follower
	role string
	// 结点当前 Term
	currentTerm int
	// 所投的 server index, 初始化为 VoteNil(-1), 没有投票给其他人
	votedFor int
	// 保存执行的命令，index 从 1 开始
	logs []Entries
	// 最后一个提交的日志 index
	commitIndex int
	// 最后一个应用到状态机的日志 index, 从 0 开始
	lastApplied int

	// Leader 才会初始化，每一次 election 后重置
	// 保存发给每个 Follower 的下一条日志 index，初始为 Leader 的最后一条日志 index + 1
	nextIndex []int
	// 保存每个 Follower 已经接受到的最后一条日志的 index
	matchIndex []int

	// 接收到心跳后写入
	appendCh chan bool
	// 投票后写入
	voteCh chan bool
	// 结束实例写入
	exitCh chan bool
	// candidate 竞选 leader 写入
	leaderCh chan bool

	// commit log 后写入数据
	applyCh chan ApplyMsg

	// 现存快照对应的最后一个日志 index
	lastIncludedIndex int
	// 现存快照对应的最后一个日志所属 Term
	lastIncludedTerm int
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	InfoRaft.Printf("Raft:%d term:%d | Persist data! Size:%5v logs:%4v\n", rf.me, rf.currentTerm, len(data), len(rf.logs)-1)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var term int
	var votedFor int
	var logs []Entries
	var lastIncludedIndex int
	var lastIncludedTerm int

	if d.Decode(&term) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil || d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		InfoRaft.Printf("Raft:%d term:%d | Failed to read persist data!\n", rf.me, rf.currentTerm)
	} else {
		rf.currentTerm = term
		rf.votedFor = votedFor
		rf.logs = logs
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.lastApplied = rf.lastIncludedIndex
		rf.commitIndex = rf.lastIncludedIndex
		InfoRaft.Printf("Raft:%d term:%d | Read persist data{%d bytes} successful! VotedFor:%d len(Logs):%d\n", rf.me, rf.currentTerm, len(data), rf.votedFor, len(rf.logs))
	}
}

// 角色转换
func (rf *Raft) convertRole(role string) {
	defer rf.persist()
	switch role {
	case Follower:
		rf.votedFor = VoteNil
		WarnRaft.Printf("Raft:%d term:%d | %s convert role to Follower!\n", rf.me, rf.currentTerm, rf.role)
	case Candidate:
		rf.currentTerm = rf.currentTerm + 1
		rf.votedFor = rf.me
		WarnRaft.Printf("Raft:%d term:%d | %s convert role to Candidate!\n", rf.me, rf.currentTerm, rf.role)
	case Leader:
		WarnRaft.Printf("Raft:%d term:%d | %s convert role to Leader!\n", rf.me, rf.currentTerm, rf.role)
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			rf.nextIndex[i] = len(rf.logs) + rf.lastIncludedIndex
			rf.matchIndex[i] = 0
		}
	}
	rf.role = role
}

// 检查 server 的状态是否与之前一致
func (rf *Raft) checkState(role string, term int) bool {
	return rf.role == role && rf.currentTerm == term
}

// 客户端执行命令
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isLeader := rf.role == Leader

	if isLeader {
		index = rf.getLastLogIndex() + 1
		rf.logs = append(rf.logs, Entries{rf.currentTerm, index, command})
		InfoRaft.Printf("Raft:%d term:%d | Leader receive a new command:%v cmdIndex:%v commitIndex:%v\n", rf.me, rf.currentTerm, command, index, rf.commitIndex)
		rf.persist()
	}

	return index, term, isLeader
}

func (rf *Raft) Kill() {
	rf.exitCh <- true
}

func (rf *Raft) ticker() {
	for {
		select {
		case <-rf.exitCh:
			return
		default:
		}
		rf.mu.Lock()
		role := rf.role
		rf.mu.Unlock()

		switch role {
		case Follower:
			select {
			case <-rf.voteCh:
			case <-rf.appendCh:
			case <-time.After(electionTimeout()):
				rf.mu.Lock()
				rf.convertRole(Candidate)
				rf.mu.Unlock()
			}
		case Candidate:
			go rf.leaderElection()

			select {
			case <-rf.voteCh:
			case <-rf.appendCh:
			case <-rf.leaderCh:
			case <-time.After(electionTimeout()):
				rf.mu.Lock()
				rf.convertRole(Candidate)
				rf.mu.Unlock()
			}
		case Leader:
			rf.broadcastEntries()
			<-time.After(HeartbeatInterval)
		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.mu = &sync.Mutex{}
	rf.role = Follower
	rf.currentTerm = 0
	rf.votedFor = VoteNil


	// 第 0 个占位
	rf.logs = make([]Entries, 1)

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// 容量为 1 防止写入阻塞
	rf.appendCh = make(chan bool, 1)
	rf.voteCh = make(chan bool, 1)
	rf.exitCh = make(chan bool, 1)
	rf.leaderCh = make(chan bool, 1)

	rf.applyCh = applyCh

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	rf.readPersist(persister.ReadRaftState())
	rf.logs[0] = Entries{rf.lastIncludedTerm, rf.lastIncludedTerm, -1}

	InfoRaft.Printf("Create a new Raft:[%d]! term:[%d]! Log length:[%d]\n", rf.me, rf.currentTerm, rf.getLastLogIndex())

	go rf.ticker()

	return rf
}