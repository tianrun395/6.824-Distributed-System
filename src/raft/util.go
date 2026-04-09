package raft

import "log"

// Debugging
const Debug = 1 // Also, use ctrl + \ to quit all goroutines and print stack traces.
// run 'GO111MODULE=off go test -race -run 2B' to show race conditions.

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
