package master

import (
	"bufio"
	"distributed-project/mapreduce/common"
	pb "distributed-project/mapreduce/protos"
	"encoding/json"
	"fmt"
	"github.com/golang/protobuf/ptypes/empty"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
)

// 保存 master 的所有状态信息
type Master struct {
	pb.UnimplementedMasterServer
	sync.Mutex
	Wg *sync.WaitGroup
	// master 的地址
	Address string
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
	nReduce int32
	// 写入数据用来终结 rpc server，从而结束 rpc server 线程
	shutdown chan bool
	// rpc listener
	l net.Listener
	// 用来统计执行结果
	stats            []int32
	nextMapTaskNo    int32
	nextReduceTaskNo int32
}

// Master 的构造方法
func newMaster(master string) (mr *Master) {
	mr = new(Master)
	// 地址使用传进来的
	mr.Address = master
	// 写入数据用来终结 rpc server，从而结束 rpc server 线程
	mr.shutdown = make(chan bool)
	// 用来进行线程 wait/notify 操作
	mr.newCond = sync.NewCond(mr)
	// 在执行完 job 时，主线程从 doneChannel 阻塞中解除
	mr.doneChannel = make(chan bool)
	mr.Wg = new(sync.WaitGroup)
	return
}

// 开启一个分布式 mapreduce 任务
func Distributed(
	jobName string,
	files []string,
	nReduce int32,
	master string,
) (m *Master) {
	m = newMaster(master)
	m.Wg.Add(len(files))
	m.StartRpcServer(master)
	go m.run(jobName, files, nReduce,
		func(phase string, ch chan string) {
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
		_, err := common.CallWorker(workerAddress, "DoTask", &rpcRequest)
		if err != nil {
			return
		}
		mr.Wg.Done()
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
		_, err := common.CallWorker(workerAddress, "DoTask", &rpcRequest)
		if err != nil {
			return
		}
		mr.Wg.Done()
	}

	registerChan <- workerAddress

	log.Printf("Schedule: %v %v tasks (%d I/Os)\n", nTasks, phase, nOther)
	log.Printf("Schedule: %v done\n", phase)
}

func (m *Master) run(
	jobName string,
	files []string,
	nReduce int32,
	schedule func(phase string, ch chan string),
	finish func(),
) {
	m.jobName = jobName
	m.files = files
	m.nReduce = nReduce

	log.Printf("%s: Starting Map/Reduce task %s\n", m.Address, m.jobName)

	ch := make(chan string, 20)
	go m.forwardRegistrations(ch)

	for i := 0; i < len(files); i++ {
		go func() {
			schedule(common.MapPhase, ch)
		}()
	}

	m.Wg.Wait()
	m.Wg.Add(int(nReduce))

	for i := int32(0); i < nReduce; i++ {
		go func() {
			schedule(common.ReducePhase, ch)
		}()
	}

	m.Wg.Wait()

	finish()
	m.merge()

	log.Printf("%s: Map/Reduce task completed\n", m.Address)

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
	res := make([]int32, 0)
	for _, worker := range m.workers {
		calReply, err := common.CallWorker(worker, "Shutdown", &empty.Empty{})
		if err != nil {
			log.Println("call worker shutdown function failed, err: ", err)
		}
		reply := calReply.Interface().(*pb.ShutdownReply)
		res = append(res, reply.NTasks)
	}
	return res
}

func (m *Master) merge() {
	files := make([]*os.File, 0)
	decoders := make([]*json.Decoder, 0)
	for i := int32(0); i < m.nReduce; i++ {
		file, err := os.Open(common.MergeFileName(m.jobName, i))
		if err != nil {
			log.Fatal("open file failed", err)
		}

		decoder := json.NewDecoder(file)
		files = append(files, file)
		decoders = append(decoders, decoder)
	}

	outputFile, err := os.Create("lollipop-mrtmp." + m.jobName)
	if err != nil {
		log.Fatal("Merge: create file error ", err)
	}

	writer := bufio.NewWriter(outputFile)
	defer outputFile.Close()
	defer writer.Flush()

	kvMap := make(map[*common.KeyValue]*json.Decoder)
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