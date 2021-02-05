package mr

//
// RPC 相关定义，RPC 方法名要以大写字母开头
//

import "os"
import "strconv"

type ShutdownReply struct {
	nTasks int
}

// rpc参数
type RpcArgs struct {
	// Worker 结点的地址
	Worker string
	X int
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
