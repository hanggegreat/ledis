package worker

import (
	"context"
	"distributed-project/mapreduce/common"
	pb "distributed-project/mapreduce/protos"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
	"log"
	"net"
	"time"
)

// 注册到 master 的方法，执行 rpc 调用
func (w *Worker) Register(masterAddress string) {
	registerRequest := pb.RegisterRequest{Address: w.address}
	_, err := common.CallMaster(masterAddress, "Register", &registerRequest)
	if err != nil {
		log.Fatalf("Register: RPC %s register error\n", masterAddress)
	} else {
		log.Println("worker register to master ...")
	}
}

// Rpc 方法，用来分配任务给 worker
func (w *Worker) DoTask(ctx context.Context, request *pb.DoTaskRequest) (reply *empty.Empty, err error) {
	log.Printf("%s: given %v task #%d on file %s (nios: %d)\n", w.address, request.Phase, request.TaskNo, request.Filename, request.NumOtherPhase)

	w.Lock()
	w.nTasks += 1
	w.concurrent += 1
	nc := w.concurrent
	w.Unlock()

	if nc > w.nConcurrent {
		log.Fatal("workerAddress.DoTask: more than one DoTask sent concurrently to a single worker\n")
	}

	pause := false
	if w.parallelism != nil {
		w.parallelism.Lock()
		w.parallelism.Now += 1
		if w.parallelism.Now > w.parallelism.Max {
			w.parallelism.Max = w.parallelism.Now
		}
		if w.parallelism.Max < 2 {
			pause = true
		}
		w.parallelism.Unlock()
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
		w.parallelism.Lock()
		w.parallelism.Now -= 1
		w.parallelism.Unlock()
	}

	log.Printf("%s: %v task #%d done\n", w.address, request.Phase, request.TaskNo)
	return &empty.Empty{}, nil
}

// Rpc 方法，用来关闭 worker 结点的方法
func (w *Worker) Shutdown(ctx context.Context, request *empty.Empty) (reply *pb.ShutdownReply, err error) {
	reply = &pb.ShutdownReply{}
	w.Lock()
	reply.NTasks = w.nTasks
	w.doneChan <- true
	w.Unlock()
	return reply, nil
}

func (w *Worker) StartRpcServer() {
	l, err := net.Listen("tcp", w.address)
	w.l = l
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterWorkerServer(s, w)
	go func() {
		if err := s.Serve(l); err != nil {
			log.Printf("failed to serve: %v", err)
		}
	}()
}
