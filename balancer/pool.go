package balancer

import "fmt"

type Pool []Worker

func NewPool(nWorkers int) Pool {
	return make([]Worker, 0, nWorkers)
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
	p[i].SetIndex(i)
	p[j].SetIndex(j)
}

func (p *Pool) Push(x interface{}) {
	// log.Println("Adding new element to the heap")
	w := x.(Worker)
	w.SetIndex(len(*p))
	*p = append(*p, w)
	// log.Println(p)
}

func (p *Pool) Pop() interface{} {
	n := len(*p) - 1
	x := (*p)[n]
	x.SetIndex(-1) // to be safe
	*p = (*p)[0:n]
	return x
}
