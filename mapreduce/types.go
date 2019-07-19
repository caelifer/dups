package mapreduce

import "strconv"

// Value interface for objects that Map expects it its input stream
type Value interface {
	Value() interface{}
}

// Key interface is there to make sure that the value can be used in go built-in
// map as a key.
type Key interface {
	Key() KeyType
}

// KeyValue interface describes objects produced by Map and used by Reduce as
// its input stream
type KeyValue interface {
	Key
	Value
}

// Helper concrete types for Key and KeyValue interfaces

type KeyType string

func (kt KeyType) Key() KeyType {
	return kt
}

func (kt KeyType) String() string {
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

func (kvt KVType) Key() KeyType {
	return kvt.key.Key()
}

func (kvt KVType) Value() interface{} {
	return kvt.val.Value()
}
