package main

import (
	mr "com.lollipop/distributed-project/mapreduce"
	"fmt"
)

func main() {
	files := make([]string, 10)
	for i := 0; i < 10; i++ {
		files[i] = fmt.Sprintf("/Users/zhangyuanhang/go/src/com/lollipop/distributed-project/files/input-%v", i)
	}
	masterAddress := mr.SockAddress("master")
	mr.Distributed("lollipop-mapreduce-demo1", files, 3, masterAddress)
	mr.StartWorker(masterAddress, mr.SockAddress("0"), 5, 3)
	mr.StartWorker(masterAddress, mr.SockAddress("1"), 5, 3)
	mr.StartWorker(masterAddress, mr.SockAddress("2"), 5, 3)
}