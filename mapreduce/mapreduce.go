package mapreduce

import "strconv"

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

// Helper concrete types for Key and KeyValue interfaces

type KeyType string

func (kt KeyType) Key() string {
	return string(kt)
}

// Helper functions

func KeyTypeFromString(s string) KeyType {
	return KeyType(s)
}

func KeyTypeFromInt64(i int64) KeyType {
	return KeyType(strconv.FormatInt(i, 10))
}

func KeyTypeFromInt(i int) KeyType {
	return KeyType(strconv.FormatInt(int64(i), 10))
}

type KVType struct {
	key Key
	val Value
}

func NewKVType(k Key, v Value) *KVType {
	return &KVType{
		key: k,
		val: v,
	}
}

func (kvt KVType) Key() string {
	return kvt.key.Key()
}

func (kvt KVType) Value() interface{} {
	return kvt.val.Value()
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
