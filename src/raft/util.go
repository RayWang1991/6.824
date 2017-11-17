package raft

import (
	"runtime"
	"fmt"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		//log.Printf(format, a...)// std err
		fmt.Printf(format, a...)
	}
	return
}

const VoteDebugFlag = true

func DVotePrintf(fmt string, a ...interface{}) (n int, err error) {
	if VoteDebugFlag {
		return DPrintf(fmt, a...)
	}
	return
}

const HeartBeatDebugFlag = true

func DHBPrintf(fmt string, a ...interface{}) (n int, err error) {
	if HeartBeatDebugFlag {
		return DPrintf(fmt, a...)
	}
	return
}

const LogDebugFlag = true

func DLogPrintf(fmt string, a ...interface{}) (n int, err error) {
	if LogDebugFlag {
		return DPrintf(fmt, a...)
	}
	return
}

const RPCDebugFlag = false

func DRPCPrintf(fmt string, a ...interface{}) () {
	if RPCDebugFlag {
		DPrintf(fmt, a...)
	}
}

func printStack() {
	buf := make([]byte, 16384)
	buf = buf[:runtime.Stack(buf, true)]
	fmt.Printf("=== BEGIN goroutine stack dump ===\n%s\n=== END goroutine stack dump ===", buf)
}
