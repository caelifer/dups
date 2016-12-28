package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"time"

	"github.com/caelifer/dups/finder"
)

// Internal constant to control number of Worker threads in balancer's worker pool
const workerPoolMultiplier = 8 // Use eight times the available cores

// Start of execution
func main() {
	// Flags
	var (
		cpuprofile  = flag.String("cpuprofile", "", "write cpu profile to file")
		memprofile  = flag.String("memprofile", "", "write memory profile to file")
		tracefile   = flag.String("tracefile", "", "write trace output to a file")
		workerCount = flag.Int("workers", runtime.NumCPU()*workerPoolMultiplier, "Number of parallel jobs")
		output      = flag.String("output", "-", "write output to a file. Default: STDOUT")
		stats       = flag.Bool("stats", false, "display runtime statistics on STDERR")
	)

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

	// Run with execution tracer
	if *tracefile != "" {
		f, err := os.Create(*tracefile)
		if err != nil {
			log.Fatal(err)
		}
		trace.Start(f)
		defer trace.Stop()
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
	find := finder.New(*workerCount)
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
	case "/dev/null":
		return os.OpenFile(os.DevNull, os.O_CREATE|os.O_WRONLY, 0666)
	default:
		return os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
	}
}
