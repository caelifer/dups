package balancer

import (
	"container/heap"
	"errors"
	"fmt"
	"log"
	"runtime"
)

type Request func()

type Worker struct {
	reqs    chan Request
	pending int
	index   int // heap index
}

const maxWorkQueueDepth = 10

var maxWorkerNumber = runtime.NumCPU()

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

type Pool []*Worker

func NewPool() Pool {
	return make([]*Worker, 0, maxWorkerNumber)
}

func (p Pool) getStats() []int {
	stats := make([]int, len(p))
	for i := 0; i < len(p); i++ {
		stats[i] = p[i].QueueSize()
	}
	return stats
}

func (p Pool) String() string {
	stats := p.getStats()
	return fmt.Sprintf("pool: %+v, load: {avg: %.1f, mean: %.1f, stddev: %.1f}",
		stats, avg(stats), mean(stats), stddev(stats))
}

func (p Pool) Cap() int {
	return cap(p)
}

// Implement heap.Interface
func (p Pool) Len() int           { return len(p) }
func (p Pool) Less(i, j int) bool { return p[i].QueueSize() < p[j].QueueSize() }
func (p Pool) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
	p[i].index = i
	p[j].index = j
}

func (p *Pool) Push(x interface{}) {
	// log.Println("Adding new element to the heap")
	w := x.(*Worker)
	w.index = len(*p)
	*p = append(*p, w)
	// log.Println(p)
}

func (p *Pool) Pop() interface{} {
	old := *p
	n := len(old) - 1
	x := old[n]
	x.index = -1 // for safety
	*p = old[0:n]
	return x
}

type Balancer struct {
	pool *Pool
	done chan *Worker
}

func NewBalancer(work chan Request) *Balancer {
	// Create new pool memory structure
	pool := NewPool()

	// Convert pool to priority queue
	// heap.Init(&pool)

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
	go func(work chan Request) {
		for {
			select {
			case req := <-work:
				for {
					err := b.dispatch(req)
					if err == nil {
						break
					}
					// We reached capacity, clean-up first
					log.Println("Blocking on", err)
					b.completed(<-b.done)
				}
			case w := <-b.done:
				b.completed(w)
			}
			// log.Printf("Load: %s\n", b)
		}
	}(work)

	return b
}

func (b Balancer) String() string {

	return fmt.Sprintf("Balancer {%s}", b.pool)
}

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

func (b *Balancer) completed(w *Worker) {
	w.pending-- // Reduce pending work counter
	// Remove from the heap
	heap.Remove(b.pool, w.index)
	// Put it back at the right position
	heap.Push(b.pool, w)
}
