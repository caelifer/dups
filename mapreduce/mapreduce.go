package mapreduce

// Value interface for objects that Map expects it its input stream
type Value interface {
	Value() interface{}
}

// Key interace is there to make sure that the value can be used in go built-in
// map as a key.
type Key interface {
	Key() string
}

// KeyValue interface describes objects produced by Map and used by Reduce as
// its input stream
type KeyValue interface {
	Key
	Value
}

// Map-Reduce implementation

// MapFn provided by the client code. It is responsible to perform actual work
// and send it to the out channel as KeyValue tuple.
type MapFn func(out chan<- KeyValue)

// Map is a primary interface to Map functionality
func Map(mapFn MapFn) <-chan KeyValue {
	out := make(chan KeyValue)
	go func() {
		mapFn(out)
		close(out) // always clean-up
	}()
	return out
}

// ReduceFn a function provided by the client code. It expects to aggregate
// over the values povided by the in channel of KeyValue objects and send its
// result to out Value channel
type ReduceFn func(out chan<- Value, in <-chan KeyValue)

func Reduce(in <-chan KeyValue, reduceFn ReduceFn) <-chan Value {
	out := make(chan Value)
	go func() {
		reduceFn(out, in)
		close(out) // always clean-up
	}()
	return out
}
