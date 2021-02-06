package mr

import "strings"

type keyValues []KeyValue

func (kvs keyValues) Less(i, j int) bool {
	return strings.Compare(kvs[i].Key, kvs[j].Key) == -1
}

func (kvs keyValues) Len() int {
	return len(kvs)
}

func (kvs keyValues) Swap(i, j int) {
	kvs[i], kvs[j] = kvs[j], kvs[i]
}

func (kvs *keyValues) Push(x interface{}) {
	*kvs = append(*kvs, x.(KeyValue))
}

func (kvs *keyValues) Pop() interface{} {
	old := *kvs
	n := len(old)
	x := old[n-1]
	*kvs = old[0 : n-1]
	return x
}
