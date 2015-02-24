package fstree_test

import (
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/caelifer/dups/balancer"
	"github.com/caelifer/dups/fstree"
)

func TestTreeWalk(t *testing.T) {
	// Recover from panic
	defer func() {
		if r := recover(); r != nil {
			t.Fatal(r)
		}
	}()

	testRoot := "/Users/timour/golang/src/github.com/caelifer/dups/t/"

	nprocs := runtime.NumCPU()
	t.Logf("Using %d CPU threads\n", nprocs)

	for i := 10; i < 20; i++ {
		results := make([]string, 0, 256)
		testdir := testRoot + strconv.Itoa(i)

		err := fstree.Walk(balancer.NewWorkQueue(nprocs), testdir, func(path string, info os.FileInfo, err error) error {
			results = append(results, path)
			return nil
		})

		if err != nil {
			t.Fatal(err)
		}

		t.Logf("Results for %d entries:\n%s\n", i+1, strings.Join(results, "\n"))
		if n := len(results); n != i+1 {
			t.Errorf("Expected %d nodes, got %d\n", i+1, n)
		}
	}
}

func TestTreeWalk11(t *testing.T) {
	// Recover from panic
	defer func() {
		if r := recover(); r != nil {
			t.Fatal(r)
		}
	}()

	testRoot := "/Users/timour/golang/src/github.com/caelifer/dups/t/"
	i := 11

	results := make([]string, 0, 256)
	testdir := testRoot + strconv.Itoa(i)

	nprocs := runtime.NumCPU()
	t.Logf("Using %d CPU threads\n", nprocs)

	err := fstree.Walk(balancer.NewWorkQueue(nprocs), testdir, func(path string, info os.FileInfo, err error) error {
		results = append(results, path)
		return nil
	})

	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Results for %d entries:\n%s\n", i+1, strings.Join(results, "\n"))
	if n := len(results); n != i+1 {
		t.Errorf("Expected %d nodes, got %d\n", i+1, n)
	}
}

func TestTreeWalk12(t *testing.T) {
	// Recover from panic
	defer func() {
		if r := recover(); r != nil {
			t.Fatal(r)
		}
	}()

	testRoot := "/Users/timour/golang/src/github.com/caelifer/dups/t/"
	i := 12

	results := make([]string, 0, 256)
	testdir := testRoot + strconv.Itoa(i)

	nprocs := runtime.NumCPU()
	t.Logf("Using %d CPU threads\n", nprocs)

	err := fstree.Walk(balancer.NewWorkQueue(nprocs), testdir, func(path string, info os.FileInfo, err error) error {
		results = append(results, path)
		return nil
	})

	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Results for %d entries:\n%s\n", i+1, strings.Join(results, "\n"))
	if n := len(results); n != i+1 {
		t.Errorf("Expected %d nodes, got %d\n", i+1, n)
	}
}

func TestTreeWalk13(t *testing.T) {
	// Recover from panic
	defer func() {
		if r := recover(); r != nil {
			t.Fatal(r)
		}
	}()

	testRoot := "/Users/timour/golang/src/github.com/caelifer/dups/t/"
	i := 13

	results := make([]string, 0, 256)
	testdir := testRoot + strconv.Itoa(i)

	nprocs := runtime.NumCPU()
	t.Logf("Using %d CPU threads\n", nprocs)

	err := fstree.Walk(balancer.NewWorkQueue(nprocs), testdir, func(path string, info os.FileInfo, err error) error {
		results = append(results, path)
		return nil
	})

	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Results for %d entries:\n%s\n", i+1, strings.Join(results, "\n"))
	if n := len(results); n != i+1 {
		t.Errorf("Expected %d nodes, got %d\n", i+1, n)
	}
}

func TestTreeWalkEmpty(t *testing.T) {
	// Recover from panic
	defer func() {
		if r := recover(); r != nil {
			t.Fatal(r)
		}
	}()

	testRoot := "/Users/timour/golang/src/github.com/caelifer/dups/t/1/1"
	i := 0

	results := make([]string, 0, 256)
	testdir := testRoot

	nprocs := runtime.NumCPU()
	t.Logf("Using %d CPU threads\n", nprocs)

	err := fstree.Walk(balancer.NewWorkQueue(nprocs), testdir, func(path string, info os.FileInfo, err error) error {
		results = append(results, path)
		return nil
	})

	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Results for %d entries:\n%s\n", i+1, strings.Join(results, "\n"))
	if n := len(results); n != i+1 {
		t.Errorf("Expected %d nodes, got %d\n", i+1, n)
	}
}
