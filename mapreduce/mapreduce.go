package mapreduce

type MapFn func(out chan<- Result)

func Map(mapFn MapFn) <-chan Result {
	ch := make(chan Result)
	go mapFn(ch)
	return ch
}

type ReduceFn func(out chan<- Result, in <-chan Result)

func Reduce(in <-chan Result, reduceFn ReduceFn) <-chan Result {
	out := make(chan Result)
	go reduceFn(out, in)
	return out
}
