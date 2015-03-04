package balancer

import (
	"errors"
	"fmt"
	"sync"
)

const maxWorkQueueDepth = 200

type Request func()

// Worker interface specification
type Worker interface {
	Enqueue(Request) error
	QueueSize() int
	Run(chan<- Worker)
	Index() int
	SetIndex(int)
}

type worker struct {
	reqs chan Request

	il    sync.RWMutex
	index int
}

func NewWorker() Worker {
	return &worker{
		reqs: make(chan Request, maxWorkQueueDepth),
	}
}

func (w *worker) String() string {
	idx := w.Index()
	pnd := w.QueueSize()
	return fmt.Sprintf("Worker (%p) { Index: %d; Pending: %d }", w, idx, pnd)
}

func (w *worker) Enqueue(r Request) error {
	// Add Request to the queue
	select {
	case w.reqs <- r:
		// log.Printf("Enqueued work for %s", w)

		return nil
	default:
		return errors.New("job queue capacity limit")
	}
}

func (w *worker) QueueSize() int {
	return len(w.reqs)
}

func (w *worker) Run(done chan<- Worker) {
	for {
		job := <-w.reqs
		job() // run job
		done <- w
		// log.Printf("Done with job for %s", w)
	}
}

func (w *worker) Index() int {
	w.il.RLock()
	idx := w.index
	w.il.RUnlock()

	return idx
}

func (w *worker) SetIndex(idx int) {
	w.il.Lock()
	w.index = idx
	w.il.Unlock()
}
