package raft

import "log"

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
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

const HeartBeatDebugFlag = false

func DHBPrintf(fmt string, a ...interface{}) (n int, err error) {
	if HeartBeatDebugFlag {
		return DPrintf(fmt, a...)
	}
	return
}

const LogDebugFlag = false

func DLogPrintf(fmt string, a ...interface{}) (n int, err error) {
	if LogDebugFlag {
		return DPrintf(fmt, a...)
	}
	return
}