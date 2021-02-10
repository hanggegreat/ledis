package worker

import (
	"distributed-project/mapreduce/common"
	pb "distributed-project/mapreduce/protos"
	"log"
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
	doneChan  chan bool
}

func StartWorker(masterAddress, address string, max int32) *Worker {
	w := new(Worker)
	w.address = address
	w.MapFunc = common.MapFunc
	w.ReduceFunc = common.ReduceFunc
	w.doneChan = make(chan bool)
	w.nConcurrent = max
	w.parallelism = &common.Parallelism{Max: max}
	go w.run()
	w.Register(masterAddress)
	return w
}

// 启动一个 worker 结点，同时启动 rpc server
func (w *Worker) run() {
	// 启动一个 rpc server
	w.StartRpcServer()
	defer w.l.Close()
	<-w.doneChan
	log.Printf("RunWorker %s exit\n", w.address)
}