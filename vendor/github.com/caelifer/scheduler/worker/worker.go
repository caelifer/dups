package worker

import (
	"log"

	"github.com/caelifer/scheduler/job"
)

// Worker is an interface type required for Scheduler to schedule its work load.
type Interface interface {
	Run(job.Interface)
}

// private implementation for simple worker
type simpleWorker struct {
	jobs chan job.Interface
	done chan<- Interface
}

// New constructs new Worker. It takes one parameter - a done channel to
// signal to Scheduler that this worker is done with its work.
func New(done chan<- Interface, quit <-chan struct{}) Interface {
	w := new(simpleWorker) // Heap
	w.done = done
	w.jobs = make(chan job.Interface)

	// Start worker's thread
	go func() {
		for {
			select {
			case j := <-w.jobs:
				j() // Execute new job
			case <-quit:
				return // shutdown
			}
		}
	}()
	return w
}

// Run method Implements Worker interface
func (w *simpleWorker) Run(j job.Interface) {
	// Wrap provided job to handle job's panic and send it to the jobs queue
	w.jobs <- func() {
		defer func() {
			// Handle job's panic.
			if r := recover(); r != nil {
				log.Println("Job failed:", r)
			}
			// Return worker to the worker pool.
			w.done <- w
		}()
		// Execute actual payload
		j()
	}
}
