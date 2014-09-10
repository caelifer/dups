package mapreduce

// Value interface for objects that Map expects it its input stream
type Value interface {
	Value() interface{}
}

// Key interace is there to make sure that the value can be used in go built-in map as a key.
type Key interface {
	Key() string
}

// KeyValue interface describes objects produced by Map and used by Reduce as its input stream
type KeyValue interface {
	Key
	Value
}
