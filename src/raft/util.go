package raft

import "log"

// Debugging
var Debug = 1

func min(x int, y int) int {
	if x > y {
		return y
	}
	return x
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
