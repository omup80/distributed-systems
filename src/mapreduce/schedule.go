package mapreduce

import (
	"fmt"
	"sync"
)
var weightGroup sync.WaitGroup
//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//

/*
Your job is to implement schedule() within mapreduce/schedule.go. This function is called twice
by the master for each MapReduce job, once for the Map phase and once for the Reduce phase.
schedule()’s job is to distribute tasks to available workers. Usually, there will be more tasks
than worker threads so schedule() must give each worker a sequence of tasks. The function should wait
until all tasks have completed before returning.

To learn about the set of workers, schedule() reads off its registerChan argument.
The channel yields a string for each worker, containing the RPC address of the worker.
While some workers may exist before schedule() is called and some may start while schedule() is running,
all will appear on registerChan. schedule() should use all the workers.

schedule() tells a worker to execute a task by sending a RPC to the worker in the
format of Worker.DoTask. This RPC’s arguments are defined by DoTaskArgs in mapreduce/common_rpc.go.
The File element is only used by Map tasks as the name of the file to read. schedule() can find these file names
in mapFiles.

To send an RPC to a worker use the call() function in mapreduce/common_rpc.go.
The first argument of the call is the worker’s address, received from registerChan.
The second argument should be “Worker.DoTask”. Finally, the third argument should be the DoTaskArgs structure
and the last argument should be nil.
 */
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part 2, 2B).
	//


	tasksArguments := make([] DoTaskArgs, 0)



	for i := 0; i < ntasks; i++ {
		file := mapFiles[i]
		tasksArgument := DoTaskArgs{jobName, file, phase, i, n_other}
		tasksArguments = append(tasksArguments, tasksArgument)
	}


	for _, taskArgument := range tasksArguments{
		weightGroup.Add(1)
		go executeTask(registerChan, "Worker.DoTask", taskArgument)
	}

	weightGroup.Wait()


	fmt.Printf("Schedule: %v done\n", phase)

}

func executeTask(registerChan chan string,rpcname string, taskArgument DoTaskArgs){

		srv := <- registerChan
		isTaskCompleted := call(srv, rpcname, taskArgument, nil)

		if isTaskCompleted{
			weightGroup.Done()
			registerChan <- srv
		} else {
			go executeTask(registerChan, rpcname, taskArgument)
		}

}
