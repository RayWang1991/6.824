package mapreduce

import (
	"fmt"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//

	for i := 0; i < ntasks; i++ {
		fn := ""
		if phase == mapPhase {
			fn = mr.files[i]
		}
		dtas := &DoTaskArgs{
			JobName:       mr.jobName,
			File:          fn,
			Phase:         phase,
			TaskNumber:    i,
			NumOtherPhase: nios,
		}
		waitToDispatchOneTask(mr, i, dtas)
	}
	fmt.Printf("Schedule: %v phase done\n", phase)
}

// should not assume rpc call() always return true
func runAndRestartWorker(w string, m *Master, taskId int, dargs *DoTaskArgs) {
	suc := call(w, "Worker.DoTask", dargs, new(struct{}))
	if !suc {
		fmt.Printf("Call %s Dotask failed", w)
		waitToDispatchOneTask(m, taskId, dargs)
		return
	}
	rargs := new(RegisterArgs)
	rargs.Worker = w
	ok := call(m.address, "Master.Register", rargs, new(struct{}))
	if ok == false {
		fmt.Printf("Register: RPC %s register error\n", m.address)
	}
}

func waitToDispatchOneTask(m *Master, taskId int, dtas *DoTaskArgs) {
	w := <-m.registerChannel
	debug("To Task for %s NO%d", w, taskId)
	go runAndRestartWorker(w, m, taskId, dtas)
}
