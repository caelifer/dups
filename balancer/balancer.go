package balancer

import (
	"container/heap"
	"fmt"
	"log"
)

type Balancer struct {
	pool *Pool
	done chan Worker
}

type WorkQueue chan<- Request

// NewWorkQueue creates and initializes new Balancer object using provide channel of Requests as a work queue.
// It returns pointer to the newly creatd object. As a part of the implementation, this method creates
// a pool of workers and start each of them in a separate gorutine. Once the worker pool is operational,
// it sets up a job dispatcher also running in its own goroutine.
func NewWorkQueue(nWorkers int) WorkQueue {
	// Create new pool memory structure
	pool := NewPool(nWorkers)

	// Create workQueue
	workQueue := make(chan Request)

	// Balancer
	b := &Balancer{
		pool: &pool,
		done: make(chan Worker),
	}

	// Create all workers
	for i := 0; i < pool.Cap(); i++ {
		w := NewWorker()
		go w.Run(b.done) // Run worker on the background
		heap.Push(b.pool, w)
	}

	// log.Printf("Complete balancer construction: %s\n", b)

	//	go func() {
	//		for {
	//			log.Printf("Load: %s\n", b)
	//			time.Sleep(time.Millisecond * 100)
	//		}
	//	}()

	// Run balancer
	go func(wq <-chan Request) {
		for {

			select {
			// Make sure we have capacity first
			case w := <-b.done:
				b.completed(w)
				// log.Printf("Completed for %s", w)

			default:
				// ... then check if we have more work
				select {
				case req, ok := <-wq:
					if ok {
						for {
							err := b.dispatch(req)
							if err != nil {
								// log.Printf("Unable to dispatch because of %q; blocking...", err)

								// Non-blocking busy wait
								select {
								// We reached capacity, clean-up first
								case w := <-b.done:
									// log.Printf("Got worker done %s", w)
									b.completed(w)
									// ... and try to reschedule the received request

									// log.Println("Got capacity - retrying dispatch...")
								default:
									// log.Println("Busy wait")
								}
							} else {
								break
							}
						}
					} else {
						log.Printf("Request queue is closed. Done")
						return
					}
				default:
					break
				}
			}
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
	w := heap.Pop(b.pool).(Worker)
	defer heap.Push(b.pool, w) // Always put back before we return

	// Send it the task
	err = w.Enqueue(r) // will never block

	return
}

// Update worker stats after job was completed
func (b *Balancer) completed(w Worker) {
	// Remove from the heap
	heap.Remove(b.pool, w.Index())
	// Put it back at the right position
	heap.Push(b.pool, w)
}
