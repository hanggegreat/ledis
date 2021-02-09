package worker

import (
	"distributed-project/mapreduce/common"
)

func StartWorker(masterAddress, address string, nRpc int32, max int32) *Worker {
	w := new(Worker)
	w.address = address
	w.MapFunc = common.MapFunc
	w.ReduceFunc = common.ReduceFunc
	w.nRPC = nRpc
	w.parallelism = &common.Parallelism{Max: max}
	go w.Run()
	w.Register(masterAddress)
	return w
}
