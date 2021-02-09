package worker

import (
	"context"
	"distributed-project/mapreduce/common"
	pb "distributed-project/mapreduce/protos"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
	"log"
	"net"
	"sync"
	"time"
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

// 注册到 master 的方法，执行 rpc 调用
func (w *Worker) Register(masterAddress string) {
	registerArgs := pb.RegisterRequest{Address: w.address}
	_, err := common.CallMaster(masterAddress, "Register", &registerArgs, new(struct{}))
	if err != nil {
		log.Fatalf("Register: RPC %s register error\n", masterAddress)
	} else {
		log.Println("worker register to master ...")
	}
}

// Rpc 方法，用来关闭 worker 结点的方法
func (w *Worker) Shutdown(ctx context.Context, request *empty.Empty) (reply *pb.ShutdownReply, err error) {
	reply = &pb.ShutdownReply{}
	w.Mutex.Lock()
	defer w.Mutex.Unlock()
	reply.NTasks = w.nTasks
	w.nRPC = 1
	w.doTaskChan <- true
	return reply, err
}

// Rpc 方法，用来分配任务给 worker
func (w *Worker) DoTask(ctx context.Context, request *pb.DoTaskRequest) (reply *empty.Empty, err error) {
	log.Printf("%s: given %v task #%d on file %s (nios: %d)\n", w.address, request.Phase, request.TaskNo, request.Filename, request.NumOtherPhase)

	w.Lock()
	w.nTasks += 1
	w.doTaskChan <- true
	w.concurrent += 1
	nc := w.concurrent
	w.Unlock()

	if nc > w.nConcurrent {
		log.Fatal("workerAddress.DoTask: more than one DoTask sent concurrently to a single worker\n")
	}

	pause := false
	if w.parallelism != nil {
		w.parallelism.Mu.Lock()
		w.parallelism.Now += 1
		if w.parallelism.Now > w.parallelism.Max {
			w.parallelism.Max = w.parallelism.Now
		}
		if w.parallelism.Max < 2 {
			pause = true
		}
		w.parallelism.Mu.Unlock()
	}

	if pause {
		// 睡一秒钟，给个机会证明一下可以并发执行多个任务的。。
		time.Sleep(time.Second)
	}

	switch request.Phase {
	case common.MapPhase:
		common.DoMap(request.JobName, request.TaskNo, request.Filename, request.NumOtherPhase, w.MapFunc)
	case common.ReducePhase:
		common.DoReduce(request.JobName, request.TaskNo, common.MergeFileName(request.JobName, request.TaskNo), request.NumOtherPhase, w.ReduceFunc)
	}

	w.Lock()
	w.concurrent -= 1
	w.Unlock()

	if w.parallelism != nil {
		w.parallelism.Mu.Lock()
		w.parallelism.Now -= 1
		w.parallelism.Mu.Unlock()
	}

	log.Printf("%s: %v task #%d done\n", w.address, request.Phase, request.TaskNo)
	return reply, err
}

func (w *Worker) StartRpcServer() {
	lis, err := net.Listen("tcp", w.address)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterWorkerServer(s, w)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

// 启动一个 worker 结点，同时启动 rpc server
func (w *Worker) Run() {
	// 启动一个 rpc server
	w.StartRpcServer()
	defer w.l.Close()

	for {
		<-w.doTaskChan
		w.Lock()
		w.nRPC--
		if w.nRPC == 0 {
			break
		}
	}

	log.Printf("RunWorker %s exit\n", w.address)
}
