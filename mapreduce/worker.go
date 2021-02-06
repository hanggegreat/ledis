package mr

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

type Worker struct {
	// 用于同步
	sync.Mutex
	// worker 节点地址
	address string
	// mapFunc
	MapFunc func(string, string) []KeyValue
	// reduceFunc
	ReduceFunc func(string, []string) string
	// 还能执行 nRPC 个 rpc 方法
	nRPC int
	// 一共执行了多少个任务
	nTasks int
	// 当前并发执行的任务个数
	concurrent int
	// 只能并发执行这么多个任务
	nConcurrent int
	// tcp listener
	l net.Listener
	// 用来记录 worker 结点上的并发执行情况
	parallelism *Parallelism
}

func StartWorker(masterAddress, address string, nRpc int, max int) *Worker {
	w := new(Worker)
	w.address = address
	w.MapFunc = mapFunc
	w.ReduceFunc = reduceFunc
	w.nRPC = nRpc
	w.parallelism = &Parallelism{Max: max}
	w.Run(masterAddress)
	return w
}

func (wk *Worker) DoTask(arg *DoTaskArgs, _ *struct{}) error {
	fmt.Printf("%s: given %v task #%d on file %s (nios: %d)\n", wk.address, arg.Phase, arg.TaskNo, arg.File, arg.NumOtherPhase)

	wk.Lock()
	wk.nTasks += 1
	wk.concurrent += 1
	nc := wk.concurrent
	wk.Unlock()

	if nc > wk.nConcurrent {
		log.Fatal("workerAddress.DoTask: more than one DoTask sent concurrently to a single worker\n")
	}

	pause := false
	if wk.parallelism != nil {
		wk.parallelism.mu.Lock()
		wk.parallelism.Now += 1
		if wk.parallelism.Now > wk.parallelism.Max {
			wk.parallelism.Max = wk.parallelism.Now
		}
		if wk.parallelism.Max < 2 {
			pause = true
		}
		wk.parallelism.mu.Unlock()
	}

	if pause {
		// 睡一秒钟，给个机会证明一下可以并发执行多个任务的。。
		time.Sleep(time.Second)
	}

	switch arg.Phase {
	case mapPhase:
		doMap(arg.JobName, arg.TaskNo, arg.File, arg.NumOtherPhase, wk.MapFunc)
	case reducePhase:
		doReduce(arg.JobName, arg.TaskNo, mergeFileName(arg.JobName, arg.TaskNo), arg.NumOtherPhase, wk.ReduceFunc)
	}

	wk.Lock()
	wk.concurrent -= 1
	wk.Unlock()

	if wk.parallelism != nil {
		wk.parallelism.mu.Lock()
		wk.parallelism.Now -= 1
		wk.parallelism.mu.Unlock()
	}

	fmt.Printf("%s: %v task #%d done\n", wk.address, arg.Phase, arg.TaskNo)
	return nil
}
