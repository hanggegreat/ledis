package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
)

// 保存所有 master 结点需要追踪的状态
type Master struct {
	// 组合一个 Mutex 结构体，用来进行并发加锁操作
	sync.Mutex
	// master 的地址
	address string
	// 在执行完 job 时，主线程从 doneChannel 阻塞中解除
	doneChannel chan bool
	// 用来进行线程 wait/notify 操作
	newCond *sync.Cond
	// 所有 worker 节点的地址
	workers []string
	// mapreduce 任务名称
	jobName string
	// 输入文件，用 string 表示单个文件
	files []string
	// reduce 节点数
	nReduce int
	// 写入数据用来终结 rpc server，从而结束 rpc server 线程
	shutdown chan struct{}
	// rpc listener
	l net.Listener
	// 用来统计执行结果
	stats []int
	nextMapTaskNo int32
	nextReduceTaskNo int32
}

// Master 结构体的构造函数
func newMaster(master string) (mr *Master) {
	mr = new(Master)
	// 地址使用传进来的
	mr.address = master
	// 写入数据用来终结 rpc server，从而结束 rpc server 线程
	mr.shutdown = make(chan struct{})
	// 用来进行线程 wait/notify 操作
	mr.newCond = sync.NewCond(mr)
	// 在执行完 job 时，主线程从 doneChannel 阻塞中解除
	mr.doneChannel = make(chan bool)
	return
}

// 开启一个分布式 mapreduce 任务
func Distributed(
	jobName string,
	files []string,
	nReduce int,
	master string,
) (mr *Master) {
	mr = newMaster(master)
	mr.startRPCServer()
	go mr.run(jobName, files, nReduce,
		func(phase string) {
			ch := make(chan string)
			go mr.forwardRegistrations(ch)
			schedule(mr, mr.jobName, mr.files, mr.nReduce, phase, ch)
		},
		func() {
			mr.stats = mr.killWorkers()
			mr.stopRPCServer()
		})
	return
}

func schedule(mr *Master, jobName string, inputFiles []string, nReduce int, phase string, registerChan chan string) {
	workerAddress := <- registerChan
	// map/reduce 类型时表示有多少个 map/reduce 类型任务
	var nTasks int
	// map/reduce 类型时表示有多少个 reduce/map 类型任务
	var nOther int
	switch phase {
	case mapPhase:
		nTasks = len(inputFiles)
		nOther = nReduce
		mapTaskNo := int(atomic.AddInt32(&mr.nextMapTaskNo, 1))
		rpcArgs := DoTaskArgs{
			JobName: jobName,
			File: inputFiles[mapTaskNo],
			Phase: phase,
			TaskNo: mapTaskNo,
			NumOtherPhase: nOther,
		}
		Call(workerAddress, "Worker.DoTask", &rpcArgs, new(struct{}))
	case reducePhase:
		nTasks = nReduce
		nOther = len(inputFiles)
		reduceTaskNo := int(atomic.AddInt32(&mr.nextReduceTaskNo, 1))
		rpcArgs := DoTaskArgs{
			JobName: jobName,
			Phase: phase,
			TaskNo: reduceTaskNo,
			NumOtherPhase: nOther,
		}
		Call(workerAddress, "Worker.DoTask", &rpcArgs, new(struct{}))
	}

	log.Printf("Schedule: %v %v tasks (%d I/Os)\n", nTasks, phase, nOther)
	log.Printf("Schedule: %v done\n", phase)
}

func (m *Master) run(
	jobName string,
	files []string,
	nReduce int,
	schedule func(phase string),
	finish func(),
) {
	m.jobName = jobName
	m.files = files
	m.nReduce = nReduce

	log.Printf("%s: Starting Map/Reduce task %s\n", m.address, m.jobName)

	schedule(mapPhase)
	schedule(reducePhase)
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

func (m *Master) killWorkers() []int {
	res := make([]int, len(m.workers) * 2)
	for _, worker := range m.workers {
		var reply ShutdownReply
		Call(worker, "WorkerAddress.Shutdown", new(struct{}), &reply)
		res = append(res, reply.NTasks)
	}
	return res
}

func (m *Master) merge() {
	files := make([]*os.File, m.nReduce)
	decoders := make([]*json.Decoder, m.nReduce)
	for i := 0; i < m.nReduce; i++ {
		file, err := os.Open(mergeFileName(m.jobName, i))
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

	kvMap := make(map[*KeyValue]*json.Decoder, m.nReduce)
	var kvHeap keyValues
	for i := 0; i < m.nReduce; i++ {
		kv := new(KeyValue)
		err:= decoders[i].Decode(kv)
		if err != nil {
			log.Println("json decode error ", err)
		} else {
			kvMap[kv] = decoders[i]
			kvHeap.Push(kv)
		}
	}

	for kvHeap.Len() != 0 {
		kv := kvHeap.Pop().(*KeyValue)
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