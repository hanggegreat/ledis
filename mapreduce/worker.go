package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"

//
// 用来封装mapreduce单个数据的结构体
//
type KeyValue struct {
	Key   string
	Value string
}

//
// hash算法，用来将key hash到 n 个 reduce 上
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// mapf 就是 map worker 具体执行的方法
// reducef 就是 reduce worker 具体执行的方法
//
func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	CallDemo()
}

//
// RPC 调用
//
func CallDemo() {
	args := RpcArgs{X: 99}
	reply := RpcReply{}

	// 调用 Master 上的 RpcAddOne 方法，返回的 reply->y = args->x + 1
	call(masterSock(), "Master.RpcAddOne", &args, &reply)
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// rpc 调用 并阻塞等待返回结果
//
func call(masterAddress, rpcName string, args interface{}, reply interface{}) bool {
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
