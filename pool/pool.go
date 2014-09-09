package pool

import (
	"runtime"
	"sync"

	"github.com/caelifer/dups/heap"
)

const (
	WorkerMultiplier = 50
)

type Result interface {
	Value() interface{}
}

type Task func() Result

type Job struct {
	Task   Task
	Output chan<- Result
}

func NewJob(task Task, out chan<- Result) *Job {
	return &Job{
		Task:   task,
		Output: out,
	}
}

type Jobs struct {
	sync.RWMutex
	jobs []*Job
}

func NewJobQueue() *Jobs {
	return &Jobs{
		jobs: make([]*Job, 1),
	}
}

func (jobs *Jobs) Put(j *Job) {
	// Exclusive lock
	jobs.Lock()
	defer jobs.Unlock()

	jobs.jobs = append(jobs.jobs, j)
}

func (jobs *Jobs) Get() *Job {
	// Exclusive lock
	jobs.Lock()
	defer jobs.Unlock()

	var j *Job = nil

	if jobs.Size() > 0 {
		j = jobs.jobs[0]
		jobs.jobs = jobs.jobs[1:]
	}
	return j
}

func (jobs *Jobs) Size() int {
	// Exclusive lock
	jobs.Lock()
	defer jobs.Unlock()

	return len(jobs.jobs)
}

type Worker struct {
	Jobs *Jobs
}

func (w *Worker) QueueSize() int {
	return w.Jobs.Size()
}

// Implement heap.Node interface for Worker
func (w *Worker) Less(other heap.Node) bool {
	return w.QueueSize() < other.(*Worker).QueueSize()
}

func (w *Worker) Execute(done chan<- *Worker) {
	// Work on the background
	go func() {
		if j := w.Jobs.Get(); j != nil {
			j.Output <- j.Task()
		}
		done <- w
	}()
}

// Maximum worker limit
var MaxWorkerNum = runtime.NumCPU() * WorkerMultiplier

type Pool struct {
	sync.RWMutex
	size    int
	workers heap.Interface
	done    chan *Worker
}

func NewPool() *Pool {
	return &Pool{
		workers: heap.New(),
		done:    make(chan *Worker),
	}
}

func (p *Pool) Enqeue(j *Job, out <-chan Result) {
	p.Lock()
	defer p.Unlock()

	var w *Worker

	if p.size < MaxWorkerNum {
		// Create new worker
		w = &Worker{Jobs: NewJobQueue()}
		p.size++
	} else {
		// Get least loaded worker
		w = p.workers.Pop().(*Worker)
	}

	// Add job to the worker
	w.Jobs.Put(j)

	// Add worker back to the pull
	p.workers.Push(w)
}

func (p *Pool) Dequeue() *Worker {
	p.Lock()
	defer p.Unlock()

	return p.workers.Pop().(*Worker)
}

func (p *Pool) Run(done <-chan bool) {
	for {
		select {
		case <-done:
			// Quit signal recieved
			return
		default:
			// Get least loaded worker
			if w := p.Dequeue(); w != nil {
				// Run task
				w.Execute(p.done)
			}
		}
	}
}
