package heap

//Node - minimal necessary interface to be implemented on the client side
type Node interface {
	Less(other Node) bool
}

type Interface interface {
	// Public interface

	// Push(n Node) adds heap.Node value to the priority queue
	Push(n Node)

	// Pop() returns a top element of the "heap". The priority is determined
	// by the Node.Less() interface implemnentation.
	Pop() Node

	// Size() returns number of Nodes currently in the queue
	Size() int

	// Private interface
	swap(int, int)
	siftup(int)
	siftdown(int)
}

// Generic *genHeap implimentation
type genHeap struct {
	data []Node
}

func New() Interface {
	data := make([]Node, 1)
	data[0] = new(sentinel)

	return &genHeap{
		data: data,
	}
}

func (h *genHeap) Push(n Node) {
	// Add new eliment to the data array
	h.data = append(h.data, n)

	// Adjust sentinel
	h.getSentinel().incr()

	// Sort heap from tail up
	h.siftup(h.getSentinel().val())
}

func (h *genHeap) Pop() Node {
	sz := h.getSentinel().val()

	if sz > 0 {
		val := h.data[1] // get value at the head

		h.swap(1, sz) // swap head with the last element

		// decrement elem counts
		h.getSentinel().decr()

		// Sort heap from head down
		h.siftdown(h.getSentinel().val())

		// Be nice to memory
		h.shrink()

		return val
	}

	// return nil if heap is empty
	return nil
}

func (h *genHeap) Size() int {
	return len(h.data)
}

// non-public interface

// getSentinel() returns pointer to the sentinel implementation
func (h *genHeap) getSentinel() *sentinel {
	s := h.data[0].(*sentinel)
	return s
}

// siftup() sorts heap from tail up
func (h *genHeap) siftup(cur int) {
	for p := parent(cur); cur > 1 && h.data[cur].Less(h.data[p]); p = parent(cur) {
		h.swap(cur, p)
		cur = p
	}
}

// siftdown() sorts heap from head down
func (h *genHeap) siftdown(max int) {
	cur := 1

	for {
		l := left(cur) // get left child index
		if l > max {
			break
		}

		// get right child
		r := right(cur)

		// find if right is lesser child
		if r <= max && h.data[r].Less(h.data[l]) {
			l = r
		}

		// Always compare ourselves with the lesser child
		if !h.data[l].Less(h.data[cur]) {
			break // Found our spot
		}

		// swap curent node and "left" child
		h.swap(cur, l)
		cur = l       // point to "left" child
		l = left(cur) // recalculate left child index
	}
}

// swap() safely swaps elements at provided indexes
func (h *genHeap) swap(a, b int) {
	sz := h.getSentinel().val()

	if a == b || b < 1 || b > sz || a < 1 || a > sz {
		return // NOOP
	}
	h.data[a], h.data[b] = h.data[b], h.data[a]
}

// shrink() releases unused memory
func (h *genHeap) shrink() {
	// our strategy to release memory if we are utilizing less then 50% of
	// allocated memory
	sz := h.getSentinel().val() + 1 // +1 is to account for sentinel
	if cap(h.data)/sz > 2 {
		newData := make([]Node, sz, sz)

		// Copy all datum items
		copy(newData, h.data[:sz])

		// Assign new slice and let GC take care of the rest
		h.data = newData
	}
}

// Sentinel helper type to control *genHeap structure
type sentinel int

func (s sentinel) Less(other Node) bool {
	// Dummy interface
	return true
}

func (s sentinel) val() int {
	return int(s)
}

func (s *sentinel) incr() {
	*s++
}

func (s *sentinel) decr() {
	*s--
}

// small helper functions

func parent(i int) int {
	return i / 2
}

func left(i int) int {
	return 2 * i
}

func right(i int) int {
	return 2*i + 1
}
