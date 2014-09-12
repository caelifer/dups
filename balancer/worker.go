package balancer

import "errors"

type Request func()

type Worker struct {
	reqs    chan Request
	pending int // managed by the balancer.Balancer
	index   int // heap index
}

const maxWorkQueueDepth = 10

func NewWorker() *Worker {
	return &Worker{
		reqs: make(chan Request, maxWorkQueueDepth),
	}
}

func (w *Worker) Enqueue(r Request) error {
	// Add Request to the queue
	select {
	case w.reqs <- r:
		return nil
	default:
		return errors.New("job queue capacity limit")
	}
}

func (w Worker) QueueSize() int {
	return w.pending
}

func (w *Worker) Run(done chan *Worker) {
	for {
		req := <-w.reqs
		req() // run job
		done <- w
	}
}
