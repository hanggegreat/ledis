package worker

import (
	"distributed-project/mapreduce/common"
	pb "distributed-project/mapreduce/protos"
	"net"
	"sync"
)

type Worker struct {
	pb.UnimplementedWorkerServer
	// 用于同步
	sync.Mutex
	// worker 节点地址
	address string
	// mapFunc
	MapFunc func(string, string) []common.KeyValue
	// reduceFunc
	ReduceFunc func(string, []string) string
	// 还能执行 nRPC 个 rpc 方法
	nRPC int32
	// 一共执行了多少个任务
	nTasks int32
	// 当前并发执行的任务个数
	concurrent int32
	// 只能并发执行这么多个任务
	nConcurrent int32
	// tcp listener
	l net.Listener
	// 用来记录 worker 结点上的并发执行情况
	parallelism *common.Parallelism
	doTaskChan  chan bool
}

func StartWorker(masterAddress, address string, nRpc int32, max int32) *Worker {
	w := new(Worker)
	w.address = address
	w.MapFunc = common.MapFunc
	w.ReduceFunc = common.ReduceFunc
	w.nRPC = nRpc
	w.doTaskChan = make(chan bool, 10)
	w.nConcurrent = max
	w.parallelism = &common.Parallelism{Max: max}
	go w.Run()
	w.Register(masterAddress)
	return w
}
