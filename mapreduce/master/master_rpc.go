package master

import (
	"context"
	pb "distributed-project/mapreduce/protos"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
	"log"
	"net"
	"sync"
	"time"
)

// 保存 master 的所有状态信息
type Master struct {
	pb.UnimplementedMasterServer
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
	nReduce int32
	// 写入数据用来终结 rpc server，从而结束 rpc server 线程
	shutdown chan struct{}
	// rpc listener
	l net.Listener
	// 用来统计执行结果
	stats            []int32
	nextMapTaskNo    int32
	nextReduceTaskNo int32
}

// Master 的构造方法
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

func (m *Master) Register(ctx context.Context, request *pb.RegisterRequest) (reply *empty.Empty, err error) {
	m.Lock()
	defer m.Unlock()
	log.Printf("Register: worker %s\n", request.GetAddress())
	m.workers = append(m.workers, request.GetAddress())
	m.newCond.Broadcast()
	return reply, err
}

func (m *Master) Shutdown(ctx context.Context, request *empty.Empty) (reply *empty.Empty, err error) {
	log.Printf("Shutdown: registration server\n")
	close(m.shutdown)
	m.l.Close()
	return reply, err
}

func (m *Master) StartRpcServer(port string) {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterMasterServer(s, m)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func (m *Master) StopRpcServer() {
	conn, err := grpc.Dial(m.address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewMasterClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = c.Shutdown(ctx, &empty.Empty{})
	if err != nil {
		log.Fatalf("Stop master rpc server failed: %v", err)
	} else {
		log.Printf("Stop master rpc server succeed")
	}
}
