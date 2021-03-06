package raft

import (
	"io"
	"log"
	"os"
)

var (
	InfoRaft *log.Logger
	WarnRaft *log.Logger

	InfoKV *log.Logger
	ShardInfo *log.Logger
)

func init() {
	//每次必须打开新的文件
	infoFile, err := os.OpenFile("raftInfo.log", os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil{
		log.Fatalln("Open infoFile failed.\n", err)
	}
	warnFile, err := os.OpenFile("raftWarn.log", os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil{
		log.Fatalln("Open warnFile failed.\n", err)
	}

	InfoKVFile, err := os.OpenFile("kvInfo.log", os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil{
		log.Fatalln("Open infoKVFile failed.\n", err)
	}

	ShardInfoFile, err := os.OpenFile("shardInfo.log", os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil{
		log.Fatalln("Open shardInfoFile failed.\n", err)
	}

	//log.Lshortfile打印出错的函数位置
	InfoRaft = log.New(io.MultiWriter(os.Stderr, infoFile), "InfoRaft:", log.Ldate | log.Ltime | log.Lshortfile)
	WarnRaft = log.New(io.MultiWriter(os.Stderr, warnFile), "WarnRaft:", log.Ldate | log.Ltime | log.Lshortfile)
	InfoKV = log.New(io.MultiWriter(os.Stderr, InfoKVFile), "InfoKV:", log.Ldate | log.Ltime | log.Lshortfile)

	//lab4中，lab2,lab3的日志只输出到文件，不在屏幕显示
	//InfoRaft = log.New(io.MultiWriter(infoFile), "InfoRaft:", log.Ldate | log.Ltime | log.Lshortfile)
	//WarnRaft = log.New(io.MultiWriter(warnFile), "WarnRaft:", log.Ldate | log.Ltime | log.Lshortfile)
	//InfoKV = log.New(io.MultiWriter(InfoKVFile), "InfoKV:", log.Ldate | log.Ltime | log.Lshortfile)
	ShardInfo = log.New(io.MultiWriter(os.Stderr, ShardInfoFile), "InfoShard:", log.Ldate | log.Ltime |log.Lshortfile)
}

// 排除通道内已有元素
func dropAndSet(ch chan bool) {

	select {
	case <-ch:
	default:
	}
	ch <- true
}