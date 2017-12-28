package raftkv

import (
	"fmt"
	"runtime"
)

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		//log.Printf(format, a...)// std err
		fmt.Printf(format, a...)
	}
	return
}

const ServerDebugFlag = true

func DServPrintf(fmt string, a ...interface{}) (n int, err error) {
	if ServerDebugFlag {
		return DPrintf(fmt, a...)
	}
	return
}

const ClientDebugFlag = true

func DClientPrintf(fmt string, a ...interface{}) (n int, err error) {
	if ClientDebugFlag {
		return DPrintf(fmt, a...)
	}
	return
}

const AplRecvDebugFlag = true

func DAplRecvPrintf(fmt string, a ...interface{}) (n int, err error) {
	if AplRecvDebugFlag {
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
