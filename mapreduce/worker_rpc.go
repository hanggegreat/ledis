package mr

import (
	"log"
	"net"
	"net/rpc"
	"os"
)

// 注册到 master 的方法，执行 rpc 调用
func (w *Worker) register(masterAddress string) {
	registerArgs := RegisterArgs{WorkerAddress: w.address}
	ok := Call(masterAddress, "Master.Register", registerArgs, new(struct{}))
	if ok == false {
		log.Fatalf("Register: RPC %s register error\n", masterAddress)
	} else {
		log.Println("worker register to master ...")
	}
}

// Rpc 方法，master 调用用来关闭 worker 结点的方法
func (w *Worker) Shutdown(_ *struct{}, reply *ShutdownReply) {
	w.Mutex.Lock()
	defer w.Mutex.Unlock()
	reply.NTasks = w.nTasks
	w.nRPC = 1
}

// 启动一个 worker 结点，同时启动 rpc server
func (w *Worker) Run(masterAddress string) {
	// 启动一个 rpc server
	rpcs := rpc.NewServer()
	rpcs.Register(w)
	os.Remove(w.address)
	l, e := net.Listen("unix", w.address)
	if e != nil {
		log.Fatal("RunWorker: worker ", w.address, " error: ", e)
	}
	w.l = l
	w.register(masterAddress)

	for {
		w.Lock()
		if w.nRPC == 0 {
			w.Unlock()
			break
		}
		w.Unlock()
		conn, err := w.l.Accept()
		if err == nil {
			w.Lock()
			w.nRPC--
			w.Unlock()
			go rpcs.ServeConn(conn)
		} else {
			break
		}
	}
	w.l.Close()
	log.Printf("RunWorker %s exit\n", w.address)
}
