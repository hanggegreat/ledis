package mr

import (
	"fmt"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"sync"
)

const (
	mapPhase    string = "mapPhase"
	reducePhase        = "reducePhase"
)

// 关闭时用来返回完成了多少个任务
type ShutdownReply struct {
	NTasks int
}

// rpc参数
type RegisterArgs struct {
	// WorkerAddress 结点的地址
	WorkerAddress string
}

type DoTaskArgs struct {
	JobName       string
	File          string
	Phase         string
	TaskNo        int
	NumOtherPhase int
}

// rpc响应
type RpcReply struct {
	Status int
}

// 用来封装mapreduce单个数据的结构体
type KeyValue struct {
	Key   string
	Value string
}

// 用来记录 worker 节点的并发数据
type Parallelism struct {
	mu sync.Mutex
	// 当前有多少任务并发执行
	Now int
	// worker 节点上最高并发执行的任务数
	Max int
}

// rpc 调用 并阻塞等待返回结果
func Call(masterAddress, rpcName string, args interface{}, reply interface{}) bool {
	// 创建一个 rpc client
	c, err := rpc.DialHTTP("unix", masterAddress)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	// rpc 调用
	err = c.Call(rpcName, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func reduceFileName(jobName string, mapTaskNo int, reduceTaskNo int) string {
	return "lollipop-mrtmp." + jobName + "-" + strconv.Itoa(mapTaskNo) + "-" + strconv.Itoa(reduceTaskNo)
}

func mergeFileName(jobName string, reduceTaskNo int) string {
	return "lollipop-mrtmp." + jobName + "-res-" + strconv.Itoa(reduceTaskNo)
}

func SockAddress(suffix string) string {
	s := "/var/tmp/824-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	s += "mr"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += suffix
	return s
}