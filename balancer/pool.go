package balancer

import (
	"fmt"
	"runtime"
)

// Max number of workers
var maxWorkerNumber = runtime.NumCPU()

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
