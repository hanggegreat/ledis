package common

import "strings"

type KeyValueHeap []KeyValue

func (kvs KeyValueHeap) Less(i, j int) bool {
	return strings.Compare(kvs[i].Key, kvs[j].Key) == -1
}

func (kvs KeyValueHeap) Len() int {
	return len(kvs)
}

func (kvs KeyValueHeap) Swap(i, j int) {
	kvs[i], kvs[j] = kvs[j], kvs[i]
}

func (kvs *KeyValueHeap) Push(x interface{}) {
	*kvs = append(*kvs, x.(KeyValue))
}

func (kvs *KeyValueHeap) Pop() interface{} {
	old := *kvs
	n := len(old)
	x := old[n-1]
	*kvs = old[0 : n-1]
	return x
}
