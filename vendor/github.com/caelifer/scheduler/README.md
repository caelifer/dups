scheduler
=========

`scheduler` is a simple load-balancer across n-workers. Work-units are values of type jobs.Interface that is a type alias for func(). 

# Installation
```
go get github.com/caelifer/scheduler
```

# Usage
```
package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/caelifer/scheduler"
)

const (
	NWORKERS = 10
	NSAMPS   = 100
)

func main() {
	var wg sync.WaitGroup
	sch := scheduler.New(NWORKERS)
	t0 := time.Now()
	rand.Seed(t0.UnixNano())
	for i := 0; i < NSAMPS; i++ {
		wg.Add(1)
		sch.Schedule(func() {
			defer wg.Done()
			time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
		})
	}
	wg.Wait()
	fmt.Printf("Ran %d samples in %s\n", NSAMPS, time.Since(t0))
}
```
This code should produce output similar to
```
$ go run test.go
Ran 100 samples in 546.21049ms
```
