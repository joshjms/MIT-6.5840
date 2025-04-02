package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	statusIdle       = 0
	statusInProgress = 1
	statusCompleted  = 2
)

type Status struct {
	state      int
	assignedAt time.Time
	assignedTo int
}

type Coordinator struct {
	mapStatus         map[string]Status
	reduceStatus      map[int]Status
	intermediateFiles map[int][]string

	autoIncrement int
	nReduce       int

	mu sync.RWMutex
}

func (c *Coordinator) RequestJob(args *RequestJobArgs, reply *RequestJobReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for filename, status := range c.mapStatus {
		if status.state == statusInProgress && time.Since(status.assignedAt) > 5*time.Second {
			c.mapStatus[filename] = Status{
				state: statusIdle,
			}
		}

		if status.state == statusIdle {
			c.autoIncrement += 1

			c.mapStatus[filename] = Status{
				state:      statusInProgress,
				assignedAt: time.Now(),
				assignedTo: c.autoIncrement,
			}

			mj := MapJob{
				FileName:     filename,
				MapJobNumber: c.autoIncrement,
				Reducers:     c.nReduce,

				ID: c.autoIncrement,
			}

			reply.MapJob = &mj
			reply.ReduceJob = nil

			return nil
		}
	}

	allMapJobsCompleted := true
	for _, status := range c.mapStatus {
		if status.state != statusCompleted {
			allMapJobsCompleted = false
			break
		}
	}

	if allMapJobsCompleted {
		for reducer, status := range c.reduceStatus {
			if status.state == statusIdle {
				c.reduceStatus[reducer] = Status{
					state:      statusInProgress,
					assignedAt: time.Now(),
				}

				rj := ReduceJob{
					IntermediateFiles: c.intermediateFiles[reducer],
					ReduceId:          reducer,
				}

				reply.MapJob = nil
				reply.ReduceJob = &rj

				return nil
			}
		}
	}

	reply.MapJob = nil
	reply.ReduceJob = nil

	return nil
}

func (c *Coordinator) ReportMapJob(args *ReportMapJobArgs, reply *ReportMapJobReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.mapStatus[args.FileName].state != statusInProgress || c.mapStatus[args.FileName].assignedTo != args.ID {
		return nil
	}

	for reducer, filename := range args.IntermediateFiles {
		c.intermediateFiles[reducer] = append(c.intermediateFiles[reducer], filename)
	}

	c.mapStatus[args.FileName] = Status{
		state:      statusCompleted,
		assignedAt: c.mapStatus[args.FileName].assignedAt,
		assignedTo: c.mapStatus[args.FileName].assignedTo,
	}

	return nil
}

func (c *Coordinator) ReportReduceJob(args *ReportReduceJobArgs, reply *ReportReduceJobReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.reduceStatus[args.ReduceId].state != statusInProgress || c.reduceStatus[args.ReduceId].assignedTo != args.ID {
		return nil
	}

	c.reduceStatus[args.ReduceId] = Status{
		state:      statusCompleted,
		assignedAt: c.reduceStatus[args.ReduceId].assignedAt,
		assignedTo: c.reduceStatus[args.ReduceId].assignedTo,
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)

	fmt.Println("server started")
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	for _, status := range c.reduceStatus {
		if status.state != statusCompleted {
			return false
		}
	}

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapStatus:         make(map[string]Status),
		reduceStatus:      make(map[int]Status),
		intermediateFiles: make(map[int][]string),
		autoIncrement:     0,
		nReduce:           nReduce,
	}

	for _, file := range files {
		c.mapStatus[file] = Status{
			state: statusIdle,
		}
	}

	for i := 0; i < nReduce; i++ {
		c.reduceStatus[i] = Status{
			state: statusIdle,
		}
	}

	c.server()
	return &c
}
