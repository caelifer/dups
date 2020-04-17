package mapreduce

// Map-Reduce implementation

// MapFn provided by the client code. It is responsible to perform actual work
// and send it to the out channel as KeyValue tuple.
type MapFn func(out chan<- KeyValue, in <-chan Value)

// Map run provided Key/Value generator in a separate go-routine. It returns channel of
// KeyValue values.
func Map(in <-chan Value, mapFn MapFn) <-chan KeyValue {
	out := make(chan KeyValue)
	go func() {
		mapFn(out, in)
		close(out) // always clean-up
	}()
	return out
}

// ReduceFn a function provided by the client code. It expects to aggregate
// over the values provided by the in channel of KeyValue objects and send its
// result to out Value channel.
type ReduceFn func(out chan<- Value, in <-chan KeyValue)

// Reduce takes input KeyValue channel and reduce function. It returns Value channel
// used by reducer for output.
func Reduce(in <-chan KeyValue, reduceFn ReduceFn) <-chan Value {
	out := make(chan Value)
	go func() {
		if reduceFn == nil {
			out <- <-in
		} else {
			reduceFn(out, in)
		}
		close(out) // always clean-up
	}()
	return out
}

// MapReducePair is a necessary type for pipeline builder
type MapReducePair struct {
	Map    MapFn
	Reduce ReduceFn
}

// Pipeline builds a pipeline by chaining together provided Map/Reducer pairs.
// It returns a <-chan of Value.
func Pipeline(pairs ...MapReducePair) <-chan Value {
	var out <-chan Value
	for _, pair := range pairs {
		out = Reduce(Map(out, pair.Map), pair.Reduce)
	}
	return out
}

// FilterOutUniques is a standard reducer that drops values with unique keys sending out
// the rest of the values.
func FilterOutUniques(out chan<- Value, in <-chan KeyValue) {
	byHash := make(map[KeyType][]Value)

	for x := range in {
		hash := x.Key()

		if vec, ok := byHash[hash]; ok {
			// Found node with the same hash
			// Send out aggregated results
			if len(vec) == 1 {
				// First time we found duplicate, send first node too
				out <- vec[0]
			}
			// Add new node to a list
			byHash[hash] = append(vec, x)
			// Send new node
			out <- x
		} else {
			byHash[hash] = []Value{x}
		}
	}
}

// FilterOutDuplicates is a standard reducer that drops values with duplicate keys sending
// out only unique matches.
func FilterOutDuplicates(out chan<- Value, in <-chan KeyValue) {
	byHash := make(map[KeyType]Value)

	for x := range in {
		key := x.Key()
		if _, ok := byHash[key]; !ok {
			// New value, send it record and send it out
			byHash[key] = x
			out <- x
		}
	}
}
