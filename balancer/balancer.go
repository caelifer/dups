package balancer

import (
	"container/heap"
	"fmt"
)

type Balancer struct {
	pool *Pool
	done chan *Worker
}

type WorkQueue chan<- Request

// New() creates and initializes new Balancer object using provide channel of Requests as a work queue.
// It returns pointer to the newly creatd object. As a part of the implementation, this method creates
// a pool of workers and start each of them in a separate gorutine. Once the worker pool is operational,
// it sets up a job dispatcher also running in its own goroutine.
func NewWorkQueue(nWorkers int) chan<- Request {
	// Create new pool memory structure
	pool := NewPool(nWorkers)

	// Create workQueue
	workQueue := make(chan Request)

	// Balancer
	b := &Balancer{
		pool: &pool,
		done: make(chan *Worker),
	}

	// Create all workers
	for i := 0; i < pool.Cap(); i++ {
		w := NewWorker()
		go w.Run(b.done) // Run worker on the background
		heap.Push(b.pool, w)
	}

	// log.Printf("Complete balancer construction: %s\n", b)

	// Run balancer
	go func(wq <-chan Request) {
		for {
			select {
			case req := <-wq:
				for {
					err := b.dispatch(req)
					if err != nil {
						// We reached capacity, clean-up first
						// log.Println("Blocking on", err)
						b.completed(<-b.done)
					} else {
						break
					}
				}
			case w := <-b.done:
				b.completed(w)
			}
			// log.Printf("Load: %s\n", b)
		}
	}(workQueue)

	return WorkQueue(workQueue)
}

// String() pretty prints Balancer object state
func (b Balancer) String() string {
	return fmt.Sprintf("Balancer {%s}", b.pool)
}

// Assign new work to the least-loaded worker
func (b *Balancer) dispatch(r Request) (err error) {
	// Get least loaded worker
	w := heap.Pop(b.pool).(*Worker)

	// Send it the task
	err = w.Enqueue(r) // will never block
	if err == nil {
		// Update count
		w.pending++
	}
	// Put it back to the heap
	heap.Push(b.pool, w)

	return err
}

// Update worker stats after job was completed
func (b *Balancer) completed(w *Worker) {
	w.pending-- // Reduce pending work counter
	// Remove from the heap
	heap.Remove(b.pool, w.index)
	// Put it back at the right position
	heap.Push(b.pool, w)
}
