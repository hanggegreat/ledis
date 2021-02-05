package mr

import (
	"fmt"
	"net"
	"sync"
)

// 保存所有 master 结点需要追踪的状态
type Master struct {
	// 组合一个 Mutex 结构体，用来进行并发加锁操作
	sync.Mutex
	// master 的地址
	address string
	// 在执行完 job 时，主线程从 doneChannel 阻塞中解除
	doneChannel chan bool
	// 用来进行线程 wait/notify 操作
	newCond *sync.Cond
	// 所有 worker 节点的地址
	workers []string
	// mapreduce 任务名称
	jobName string
	// 输入文件，用 string 表示单个文件
	files []string
	// reduce 节点数
	nReduce int
	// 写入数据用来终结 rpc server，从而结束 rpc server 线程
	shutdown chan struct{}
	// rpc listener
	l net.Listener
	// 用来统计执行结果
	stats []int
}

// Master 结构体的构造函数
func newMaster(master string) (mr *Master) {
	mr = new(Master)
	// 地址使用传进来的
	mr.address = master
	// 写入数据用来终结 rpc server，从而结束 rpc server 线程
	mr.shutdown = make(chan struct{})
	// 用来进行线程 wait/notify 操作
	mr.newCond = sync.NewCond(mr)
	// 在执行完 job 时，主线程从 doneChannel 阻塞中解除
	mr.doneChannel = make(chan bool)
	return
}

// 开启一个分布式 mapreduce 任务
func Distributed(
	jobName string,
	files []string,
	nReduce int,
	master string,
) (mr *Master) {
	mr = newMaster(master)
	mr.startRPCServer()
	go mr.run(jobName, files, nReduce,
		func(phase string) {
			ch := make(chan string)
			go mr.forwardRegistrations(ch)
			schedule(mr.jobName, mr.files, mr.nReduce, phase, ch)
		},
		func() {
			mr.stats = mr.killWorkers()
			mr.stopRPCServer()
		})
	return
}

func schedule(jobName string, mapFiles []string, nReduce int, phase string, registerChan chan string) {
	// map/reduce 类型时表示有多少个 map/reduce 类型任务
	var nTasks int
	// map/reduce 类型时表示有多少个 reduce/map 类型任务
	var nOther int
	switch phase {
	case mapPhase:
		nTasks = len(mapFiles)
		nOther = nReduce
	case reducePhase:
		nTasks = nReduce
		nOther = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", nTasks, phase, nOther)
	fmt.Printf("Schedule: %v done\n", phase)
}

func (m *Master) run(
	jobName string,
	files []string,
	nReduce int,
	schedule func(phase string),
	finish func(),
) {
	m.jobName = jobName
	m.files = files
	m.nReduce = nReduce

	fmt.Printf("%s: Starting Map/Reduce task %s\n", m.address, m.jobName)

	schedule(mapPhase)
	schedule(reducePhase)
	finish()
	m.merge()

	fmt.Printf("%s: Map/Reduce task completed\n", m.address)

	m.doneChannel <- true
}

func (m *Master) forwardRegistrations(ch chan string) {
	i := 0
	for {
		m.Lock()
		if i < len(m.workers) {
			work := m.workers[i]
			i++
			ch <- work
		} else {
			m.newCond.Wait()
		}
		m.Unlock()
	}
}

func (m *Master) killWorkers() []int {
	res := make([]int, len(m.workers) * 2)
	for _, worker := range m.workers {
		var reply ShutdownReply
		call(worker, "Worker.Shutdown", new(struct{}), &reply)
		res = append(res, reply.nTasks)
	}
	return res
}

func (m *Master) merge() {

}

//
// 一个用来计算 +1 的 RPC 方法
//
func (m *Master) RpcAddOne(args *RpcArgs, reply *RpcReply) error {
	reply.Y = args.X + 1
	return nil
}




//
// 返回该 worker 结点上是否所有任务都已经执行完成
//
func (m *Master) Done() bool {
	ret := false

	ret = true

	return ret
}

//
// 创建一个 Master 并返回， files 是输入文件， nReduce 是 reduce worker 的数量
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// 在 master 进程上开启 rpc 服务
	m.startRPCServer()
	return &m
}