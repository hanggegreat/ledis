package main

import (
	"distributed-project/mapreduce/master"
	"distributed-project/mapreduce/worker"
	"fmt"
)

const jobName = "lollipop-mapreduce"
const masterAddress = ":1111"

var workerAddresses = []string{":2222", ":3333", ":4444", ":5555", ":6666", ":7777"}

func main() {
	fileCount := 2
	files := make([]string, 0)
	for i := 0; i < fileCount; i++ {
		files = append(files, fmt.Sprintf("/Users/zhangyuanhang/go/src/distributed-project/files/input-%v", i))
	}
	m := master.Distributed(jobName, files, 2, masterAddress)
	worker.StartWorker(m.Address, workerAddresses[0], 5, 3)
	worker.StartWorker(m.Address, workerAddresses[1], 5, 3)
	//worker.StartWorker(m.Address, workerAddresses[1], 8, 3)
	//worker.StartWorker(masterAddress, m.Address, 5, 3)
	m.Wait()
}
