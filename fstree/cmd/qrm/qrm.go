package main

// Quick remove tool

import (
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"regexp"
	"runtime"
	"sync"
	"time"
	"path/filepath"

	"github.com/caelifer/dups/balancer"
	// "github.com/caelifer/dups/fstree"
)

var (
	fregexp = flag.String("name", "", "Regular expression for file name.")
	faction = flag.String("action", "print", "Avaliable actions are 'print' and 'remove'.")
)

func main() {
	flag.Parse()

	var matcher *regexp.Regexp
	var actor Executer

	// Process flags

	// Regexp matcher
	if *fregexp != "" {
		matcher = regexp.MustCompile(*fregexp)
	}

	// Action
	switch *faction {
	case "print":
		actor = Printer{os.Stdout} // Default
	case "remove":
		actor = Remover{}
	default:
		log.Fatalf("Action %q is not supported\n", *faction)
	}

	// If no parameters are submitted, assume cwd
	roots := flag.Args()
	if len(roots) == 0 {
		roots = []string{"."}
	}

	// Create WorkQueue
	wq := balancer.NewWorkQueue(runtime.NumCPU() * 16)
	// wq := balancer.NewWorkQueue(1)

	// Sentinel
	var wg sync.WaitGroup
	defer wg.Wait() // Wait for all the worker to finish

	// Start
	for _, root := range roots {
		// err := fstree.Walk(wq, root, func(path string, info os.FileInfo, err error) error {
		err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			selected := true // Default

			// Run through filter
			if matcher != nil {
				if !matcher.Match([]byte(path)) {
					selected = false
				}
			}
			// log.Println(path, selected)

			// Do work
			if selected {
				wg.Add(1)
				// log.Println("Sending work for", path)
				go func(p string, i os.FileInfo) {
					wq <- func() {
						// Mark end of work unit in the correct go routine
						defer wg.Done()
						// log.Println("Workin on", p)
						if err := actor.Execute(p, i); err != nil {
							log.Println(err)
						}
					}
				}(path, info)
			}

			return nil
		})
		if err != nil {
			log.Fatal(err)
		}
	}
}

type Executer interface {
	Execute(path string, finfo os.FileInfo) error
}

type Remover struct{}

func (Remover) Execute(path string, fi os.FileInfo) error {

	// Skip directories and new files
	if !fi.IsDir() && math.Abs(float64(time.Since(fi.ModTime()))/24) > 3 {
		return os.Remove(path)
	}

	return nil
}

type Printer struct {
	io.Writer
}

func (p Printer) Execute(path string, fi os.FileInfo) error {
	// log.Println("Executed for", path)
	if !fi.IsDir() {
		fmt.Fprintln(p, path)
	}
	return nil
}
