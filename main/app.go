package main
//
//import (
//	mr "com.lollipop/distributed-project/mapreduce"
//	"fmt"
//)
//
//func main() {
//	fileCount := 1
//	files := make([]string, fileCount)
//	for i := 0; i < fileCount; i++ {
//		files[i] = fmt.Sprintf("/Users/zhangyuanhang/go/src/com/lollipop/distributed-project/files/input-%v", i)
//	}
//	masterAddress := mr.SockAddress("master")
//	master := mr.Distributed("lollipop-mapreduce-demo1", files, 3, masterAddress)
//	mr.StartWorker(masterAddress, mr.SockAddress("0"), 5, 3)
//	//mr.StartWorker(masterAddress, mr.SockAddress("1"), 5, 3)
//	//mr.StartWorker(masterAddress, mr.SockAddress("2"), 5, 3)
//	master.Wait()
//}