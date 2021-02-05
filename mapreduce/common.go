package mr

import (
	"os"
	"strconv"
)

const (
	mapPhase    string = "mapPhase"
	reducePhase        = "reducePhase"
)

// 关闭时用来返回完成了多少个任务
type ShutdownReply struct {
	nTasks int
}

// rpc参数
type RpcArgs struct {
	// Worker 结点的地址
	Worker string
	X      int
}

// rpc响应
type RpcReply struct {
	Y int
}

// 返回一个unix域套接字名称
func masterSock() string {
	s := "/var/tmp/lollipop"
	s += strconv.Itoa(os.Getuid())
	return s
}
