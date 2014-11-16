package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/caelifer/dups/finder"
)

// Internal constant to control number of Worker threads in balancer's worker pool
const workerPoolMultiplier = 2 // Use twice the available cores

// Flags
var (
	cpuprofile  = flag.String("cpuprofile", "", "write cpu profile to file")
	memprofile  = flag.String("memprofile", "", "write memory profile to file")
	workerCount = flag.Int("jobs", runtime.NumCPU()*workerPoolMultiplier, "Number of parallel jobs")
	output      = flag.String("output", "-", "write output to a file. Default: STDOUT")
	stats       = flag.Bool("stats", false, "display runtime statistics on STDERR")
)

// Start of execution
func main() {
	// First parse flags
	flag.Parse()

	// Prep runtime to use the workerCount real threads
	runtime.GOMAXPROCS(*workerCount)

	// CPU profile
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	// Memory profile
	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal(err)
		}
		defer func() {
			pprof.WriteHeapProfile(f)
			f.Close()
		}()
	}

	// Process command line params
	paths := flag.Args()
	if len(paths) == 0 {
		// Default is current directory
		paths = []string{"."}
	}

	// Get output writer
	out, err := getOutput(*output)
	if err != nil {
		log.Fatal(err)
	}
	defer out.Close()

	// Trace time spent
	t1 := time.Now()

	// Find all dups and report to output
	find := finder.NewFinder(*workerCount)
	for d := range find.AllDups(paths) {
		fmt.Fprintln(out, d)
	}

	// Update stats
	find.SetTimeSpent(time.Since(t1))

	// Display runtime stats if requested
	if *stats {
		log.Println(find.Stats())
	}
}

// Get output handle
func getOutput(path string) (io.WriteCloser, error) {
	switch path {
	case "-":
		return os.Stdout, nil // default
	default:
		return os.OpenFile(*output, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
	}
}
