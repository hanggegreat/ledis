package master

import (
	"bufio"
	"distributed-project/mapreduce/common"
	pb "distributed-project/mapreduce/protos"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync/atomic"
)

// 开启一个分布式 mapreduce 任务
func Distributed(
	jobName string,
	files []string,
	nReduce int32,
	master string,
) (m *Master) {
	m = newMaster(master)
	m.StartRpcServer(master)
	go m.run(jobName, files, nReduce,
		func(phase string) {
			ch := make(chan string)
			go m.forwardRegistrations(ch)
			schedule(m, m.jobName, m.files, m.nReduce, phase, ch)
		},
		func() {
			m.stats = m.killWorkers()
			m.StopRpcServer()
		})
	return
}

func schedule(mr *Master, jobName string, inputFiles []string, nReduce int32, phase string, registerChan chan string) {
	workerAddress := <-registerChan
	// map/reduce 类型时表示有多少个 map/reduce 类型任务
	var nTasks int32
	// map/reduce 类型时表示有多少个 reduce/map 类型任务
	var nOther int32
	switch phase {
	case common.MapPhase:
		nTasks = int32(len(inputFiles))
		nOther = nReduce
		mapTaskNo := mr.nextMapTaskNo
		rpcRequest := pb.DoTaskRequest{
			JobName:       jobName,
			Filename:      inputFiles[mapTaskNo],
			Phase:         phase,
			TaskNo:        mapTaskNo,
			NumOtherPhase: nOther,
		}
		atomic.AddInt32(&mr.nextMapTaskNo, 1)
		common.CallMaster(workerAddress, "DoTask", &rpcRequest, new(struct{}))
	case common.ReducePhase:
		nTasks = nReduce
		nOther = int32(len(inputFiles))
		reduceTaskNo := mr.nextReduceTaskNo
		rpcRequest := pb.DoTaskRequest{
			JobName:       jobName,
			Phase:         phase,
			TaskNo:        reduceTaskNo,
			NumOtherPhase: nOther,
		}
		atomic.AddInt32(&mr.nextReduceTaskNo, 1)
		common.CallMaster(workerAddress, "DoTask", &rpcRequest, new(struct{}))
	}

	log.Printf("Schedule: %v %v tasks (%d I/Os)\n", nTasks, phase, nOther)
	log.Printf("Schedule: %v done\n", phase)
}

func (m *Master) run(
	jobName string,
	files []string,
	nReduce int32,
	schedule func(phase string),
	finish func(),
) {
	m.jobName = jobName
	m.files = files
	m.nReduce = nReduce

	log.Printf("%s: Starting Map/Reduce task %s\n", m.address, m.jobName)

	schedule(common.MapPhase)
	schedule(common.ReducePhase)
	finish()
	m.merge()

	log.Printf("%s: Map/Reduce task completed\n", m.address)

	m.doneChannel <- true
}

func (m *Master) forwardRegistrations(ch chan string) {
	i := 0
	for {
		m.Lock()
		if i < len(m.workers) {
			work := m.workers[i]
			i++
			ch <- work
		} else {
			m.newCond.Wait()
		}
		m.Unlock()
	}
}

func (m *Master) killWorkers() []int32 {
	res := make([]int32, len(m.workers)*2)
	for _, worker := range m.workers {
		var reply pb.ShutdownReply
		common.CallMaster(worker, "Shutdown", new(struct{}), &reply)
		res = append(res, reply.NTasks)
	}
	return res
}

func (m *Master) merge() {
	files := make([]*os.File, m.nReduce)
	decoders := make([]*json.Decoder, m.nReduce)
	for i := int32(0); i < m.nReduce; i++ {
		file, err := os.Open(common.MergeFileName(m.jobName, i))
		if err != nil {
			log.Fatal("open file failed", err)
		}

		decoder := json.NewDecoder(file)
		files[i] = file
		decoders[i] = decoder
	}

	outputFile, err := os.Create("lollipop-mrtmp." + m.jobName)
	if err != nil {
		log.Fatal("Merge: create file error ", err)
	}

	writer := bufio.NewWriter(outputFile)
	defer outputFile.Close()
	defer writer.Flush()

	kvMap := make(map[*common.KeyValue]*json.Decoder, m.nReduce)
	var kvHeap common.KeyValueHeap
	for i := int32(0); i < m.nReduce; i++ {
		kv := new(common.KeyValue)
		err := decoders[i].Decode(kv)
		if err != nil {
			log.Println("json decode error ", err)
		} else {
			kvMap[kv] = decoders[i]
			kvHeap.Push(kv)
		}
	}

	for kvHeap.Len() != 0 {
		kv := kvHeap.Pop().(*common.KeyValue)
		fmt.Fprintf(writer, "%v: %v\n", kv.Key, kv.Value)
		err := kvMap[kv].Decode(kv)
		if err != nil {
			log.Println("json decode error ", err)
		} else {
			kvHeap.Push(kv)
		}
	}

	for _, file := range files {
		file.Close()
	}
}

func (m *Master) Wait() {
	<-m.doneChannel
}