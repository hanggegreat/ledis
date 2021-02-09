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
	kvs := make([]KeyValue, 10)
	for i := int32(0); i < nMaps; i++ {
		file, err := os.Open(ReduceFileName(jobName, i, reduceTaskNo))
		if err != nil {
			log.Fatal("Open file failed", err)
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
		file.Close()
	}

	kKvs := KeyValueHeap(kvs)
	sort.Sort(kKvs)

	if len(kKvs) == 0 {
		return
	}

	outputFile, err := os.Create(outFilename)
	if err != nil {
		log.Fatal("Create file failed", err)
	}

	writer := bufio.NewWriter(outputFile)
	defer outputFile.Close()
	defer writer.Flush()

	key := ""
	values := make([]string, 10)
	for i := 0; i <= len(kKvs); i++ {
		if key == kKvs[i].Key {
			values = append(values, kKvs[i].Value)
			continue
		} else if i < len(kKvs) {
			fmt.Fprintf(writer, "%v: %v\n", key, reduceFunc(key, values))
			values = values[0:0]
			key = ""
		} else {
			if key != "" {
				fmt.Fprintf(writer, "%v: %v\n", key, reduceFunc(key, values))
			}
		}
	}
}

func ReduceFunc(key string, values []string) string {
	count := 0
	for _, value := range values {
		val, err := strconv.Atoi(value)
		if err != nil {
			log.Fatal("atoi failed", err)
		}
		count += val
	}
	return strconv.Itoa(count)
}
