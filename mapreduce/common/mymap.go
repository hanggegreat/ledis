package common

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"unicode"
)

// doMap 用来执行一个 map 任务
// 首先读取输入文件 inFile，并将文件名和文件内容传入 mapFunc 来执行 map 任务
// mapFunc 对输入内容进行运算并返回一个 KeyValue 切片结果
// doMap 再将 KeyValue 切片数据 hash 到 nReduce 个文件中，输出文件名使用 reduceFileName(jobName, mapTaskNo, reduceTaskNo) 函数生成
func DoMap(
	jobName string,
	mapTaskNo int32,
	inputFilename string,
	nReduce int32,
	mapFunc func(filename string, content string) []KeyValue,
) {

	file, err := os.Open(inputFilename)
	if err != nil {
		log.Fatal("Open inputFile failed", err)
	}

	defer file.Close()
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatal("Read inputFile failed", err)
	}

	kvs := mapFunc(inputFilename, string(content))

	files := make([]*os.File, nReduce)

	for i := 0; i < len(files); i++ {
		tempFile, e := os.Create(ReduceFileName(jobName, mapTaskNo, nReduce))
		if e != nil {
			log.Fatal("Create reduce failed", err)
		}
		files[i] = tempFile
	}

	for i := 0; i < len(kvs); i++ {
		fmt.Fprintf(files[i], "%v %v\n", kvs[i].Key, kvs[i].Value)
	}

	for _, file := range files {
		file.Close()
	}
}

func MapFunc(filename string, content string) (res []KeyValue) {
	words := strings.FieldsFunc(content, func(r rune) bool { return !unicode.IsLetter(r) })
	dict := make(map[string]int)

	for _, word := range words {
		dict[word] += 1
	}

	for key, val := range dict {
		res = append(res, KeyValue{Key: key, Value: strconv.Itoa(val)})
	}
	return
}

// hash算法，用来将key hash到 n 个 reduce 上
func IHash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
