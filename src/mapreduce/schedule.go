package mapreduce

import (
	"fmt"
	"sync"
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
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

	var waitgroup sync.WaitGroup
	for i := 0; i< ntasks;i++{
		waitgroup.Add(1)
		debug("START TASK %v",i)
		go func(taskNum int, nios int, phase jobPhase){
			defer waitgroup.Done()

				var args DoTaskArgs
				args.JobName = mr.jobName
				args.File = mr.files[taskNum]
				args.TaskNumber = taskNum
				args.Phase = phase
				args.NumOtherPhase = nios

				worker := <-mr.registerChannel

				ok := call(worker, "Worker.DoTask", &args, nil)
				if ok != true {
					debug("failed task I %v",i)
					i = i-1
				} else{
					go func() {
						mr.registerChannel <- worker}()
				}




		}(i, nios, phase)
	}

	// xni;12/01/2018
	// Make main thread waiting rather than exiting.
	waitgroup.Wait()

	fmt.Printf("Schedule: %v phase done\n", phase)
}
