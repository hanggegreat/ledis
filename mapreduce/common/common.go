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
	sync.Mutex
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

func CallMaster(address string, methodName string, arg interface{}) (v reflect.Value, e error) {
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	c := pb.NewMasterClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	results := callReflect(c, methodName, ctx, arg)
	if results[1].Interface() == nil {
		e = nil
	} else {
		e = results[1].Interface().(error)
	}
	return results[0], e
}

func CallWorker(address string, methodName string, arg interface{}) (v reflect.Value, e error) {
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	c := pb.NewWorkerClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	results := callReflect(c, methodName, ctx, arg)
	if results[1].Interface() == nil {
		e = nil
	} else {
		e = results[1].Interface().(error)
	}
	return results[0], e
}

func callReflect(any interface{}, name string, context context.Context, arg interface{}) []reflect.Value {
	inputs := []reflect.Value {reflect.ValueOf(context), reflect.ValueOf(arg)}

	if v := reflect.ValueOf(any).MethodByName(name); v.String() == "<invalid Value>" {
		return nil
	} else {
		return v.Call(inputs)
	}
}
