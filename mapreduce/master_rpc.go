package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

// Worker 结点用来注册到 Master 结点上的 RPC 方法
func (m *Master) Register(args *RpcArgs, _ *RpcReply) error {
	m.Lock()
	defer m.Unlock()
	fmt.Printf("Register: worker %s\n", args.Worker)
	m.workers = append(m.workers, args.Worker)
	m.newCond.Broadcast()
	return nil
}

// 关闭 rpc server 的 rpc 方法
func (m *Master) Shutdown() {
	close(m.shutdown)
}

//
// 利用io等多路
//
func (m *Master) startRPCServer() {
	// 启动一个 rpc server
	rpc.Register(m)
	// 使用 http 来进行 rpc数据编解码和传输
	rpc.HandleHTTP()
	// 或者也可以采用 tpc + 端口 形式
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)

	if e != nil {
		log.Fatal("listen error:", e)
	}

	// 开启一个协程，利用IO多路复用去监听并处理新的连接请求和数据
	go http.Serve(l, nil)

	go func() {
		<- m.shutdown
		l.Close()
	}()
}

// 关闭 Master rpc server
func (m *Master) stopRPCServer() {
	var reply ShutdownReply
	ok := call(m.address, "Master.Shutdown", new(struct{}), &reply)
	if ok == false {
		fmt.Printf("Cleanup: RPC %s error\n", m.address)
	}
	fmt.Println("Clean up Registration: done")
}