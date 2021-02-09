package common

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
)

// doReduce 用来执行一个 reduce 任务
// 首先根据 reduceTaskNo 和 nMaps 找到 nMaps 个文件读入排序并合并
// 将合并后的每个key 和 value 传入 reduceFunc 计算并返回结果
// 最后将 reduceFunc 输出到 outFilename 对应的文件中
func DoReduce(
	jobName string,
	reduceTaskNo int32,
	outFilename string,
	nMaps int32,
	reduceFunc func(key string, values []string) string,
) {
	kvs := make([]KeyValue, 0)
	for i := int32(0); i < nMaps; i++ {
		file, err := os.Open(ReduceFileName(jobName, i, reduceTaskNo))
		if err != nil {
			log.Fatal("Open file failed: ", err)
		}

		decoder := json.NewDecoder(file)
		for {
			kv := new(KeyValue)
			err := decoder.Decode(kv)
			if err != nil {
				break
			}

			kvs = append(kvs, *kv)
		}
	}

	kvHeap := KeyValueHeap(kvs)
	sort.Sort(kvHeap)

	if len(kvHeap) == 0 {
		return
	}

	outputFile, err := os.Create(outFilename)
	if err != nil {
		log.Fatal("Create file failed", err)
	}

	writer := bufio.NewWriter(outputFile)
	defer outputFile.Close()
	defer writer.Flush()
	values := make([]string, 0)
	values = append(values, kvHeap[0].Value)
	for i := 1; i < len(kvHeap); i++ {
		if kvHeap[i].Key != kvHeap[i - 1].Key {
			fmt.Fprintf(writer, "%v: %v\n", kvHeap[i - 1].Key, reduceFunc(kvHeap[i - 1].Key, values))
			values = values[0:0]
		}
		values = append(values, kvHeap[i].Value)
	}

	if len(values) > 0 {
		fmt.Fprintf(writer, "%v: %v\n", kvHeap[len(kvHeap) - 1].Key, reduceFunc(kvHeap[len(kvHeap) - 1].Key, values))
	}
}

func ReduceFunc(key string, values []string) string {
	count := 0
	for _, value := range values {
		val, err := strconv.Atoi(value)
		if err != nil {
			log.Fatalf("Atoi failed, value: %v, err: %v", value, err)
		}
		count += val
	}
	return strconv.Itoa(count)
}
