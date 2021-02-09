package common

import (
	"context"
	pb "distributed-project/mapreduce/protos"
	"google.golang.org/grpc"
	"log"
	"reflect"
	"strconv"
	"sync"
	"time"
)

const (
	MapPhase    string = "mapPhase"
	ReducePhase        = "reducePhase"
)

// 用来封装mapreduce单个数据的结构体
type KeyValue struct {
	Key   string
	Value string
}

// 用来记录 worker 节点的并发数据
type Parallelism struct {
	Mu sync.Mutex
	// 当前有多少任务并发执行
	Now int32
	// worker 节点上最高并发执行的任务数
	Max int32
}

func ReduceFileName(jobName string, mapTaskNo int32, reduceTaskNo int32) string {
	return "lollipop-mrtmp." + jobName + "-" + strconv.Itoa(int(mapTaskNo)) + "-" + strconv.Itoa(int(reduceTaskNo))
}

func MergeFileName(jobName string, reduceTaskNo int32) string {
	return "lollipop-mrtmp." + jobName + "-res-" + strconv.Itoa(int(reduceTaskNo))
}

func CallMaster(address string, methodName string, args ...interface{}) (reflect.Value, error) {
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	c := pb.NewMasterClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	results := callReflect(c, methodName, ctx, args)
	return results[0], results[1].Interface().(error)
}

func CallWorker(address string, methodName string, args ...interface{}) (reflect.Value, error) {
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	c := pb.NewWorkerClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	results := callReflect(c, methodName, ctx, args)
	return results[0], results[1].Interface().(error)
}

func callReflect(any interface{}, name string, args ...interface{}) []reflect.Value {
	inputs := make([]reflect.Value, len(args))
	for i, _ := range args {
		inputs[i] = reflect.ValueOf(args[i])
	}

	if v := reflect.ValueOf(any).MethodByName(name); v.String() == "<invalid Value>" {
		return nil
	} else {
		return v.Call(inputs)
	}
}
