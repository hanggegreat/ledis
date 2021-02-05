package main

import mr "com.lollipop/distributed-project/mapreduce"

func main() {
	files := make([]string, 16)
	mr.MakeMaster(files, 3)
	mr.Worker(nil, nil)
}
