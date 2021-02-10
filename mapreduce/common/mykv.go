package common

import "strings"

type KeyValues []KeyValue

func (kvs KeyValues) Less(i, j int) bool {
	return strings.Compare(kvs[i].Key, kvs[j].Key) == -1
}

func (kvs KeyValues) Len() int {
	return len(kvs)
}

func (kvs KeyValues) Swap(i, j int) {
	kvs[i], kvs[j] = kvs[j], kvs[i]
}

func (kvs *KeyValues) Push(x interface{}) {
	*kvs = append(*kvs, x.(KeyValue))
}

func (kvs *KeyValues) Pop() interface{} {
	old := *kvs
	n := len(old)
	x := old[n-1]
	*kvs = old[0 : n-1]
	return x
}

