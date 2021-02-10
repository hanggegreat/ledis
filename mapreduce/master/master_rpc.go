package master

import (
	"context"
	pb "distributed-project/mapreduce/protos"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
	"log"
	"net"
	"time"
)

func (m *Master) Register(ctx context.Context, request *pb.RegisterRequest) (reply *empty.Empty, err error) {
	m.Lock()
	defer m.Unlock()
	log.Printf("Register: worker %s\n", request.GetAddress())
	m.workers = append(m.workers, request.GetAddress())
	m.newCond.Broadcast()
	return &empty.Empty{}, nil
}

func (m *Master) Shutdown(ctx context.Context, request *empty.Empty) (reply *empty.Empty, err error) {
	log.Printf("Shutdown: registration server\n")
	close(m.shutdown)
	m.l.Close()
	return &empty.Empty{}, err
}

func (m *Master) StartRpcServer(port string) {
	l, err := net.Listen("tcp", port)
	m.l = l
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterMasterServer(s, m)
	go func() {
		if err := s.Serve(l); err != nil {
			log.Printf("failed to serve: %v", err)
		}
	}()
}

func (m *Master) StopRpcServer() {
	conn, err := grpc.Dial(m.Address, grpc.WithInsecure(), grpc.WithBlock())
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
