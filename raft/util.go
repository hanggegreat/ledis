package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DInfo(format string, a ...interface{}) (n int, err error) {
	log.Printf(format, a...)
	return
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func init() {
	rand.Seed(time.Now().UnixNano())
	log.SetFlags(log.Lmicroseconds)
}

// Generate int with range in [from, to)
func RandomRange(from, to int) int {
	return rand.Intn(to-from) + from
}
