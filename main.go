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

// Scale number of workers 8 times the number of cores
const workerPoolMultiplier = 8 // Use eight times the available cores
// Default workers count
var defaultWorkerCount = runtime.NumCPU() * workerPoolMultiplier

// Start of execution
func main() {
	// Flags
	var (
		cpuprofile  = flag.String("cpuprofile", "", "write cpu profile to file")
		memprofile  = flag.String("memprofile", "", "write memory profile to file")
		tracefile   = flag.String("tracefile", "", "write trace output to a file")
		workerCount = flag.Int("workers", defaultWorkerCount, "Number of parallel jobs")
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
		errHandle(err, "failed to create CPU profiler output")
		err = pprof.StartCPUProfile(f)
		errHandle(err, "failed to start CPU profiler")
		defer pprof.StopCPUProfile()
	}

	// Memory profile
	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		errHandle(err, "failed to create Memory profiler output")
		defer func() {
			err := pprof.WriteHeapProfile(f)
			errHandle(err, "failed to write heap profile")
			err = f.Close()
			errHandle(err, "failed to close profiler")
		}()
	}

	// Run with execution tracer
	if *tracefile != "" {
		f, err := os.Create(*tracefile)
		errHandle(err, "failed to create trace file")
		err = trace.Start(f)
		errHandle(err, "failed to start trace")
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
	errHandle(err, "failed to create output file")
	defer func() {
		err := out.Close()
		errHandle(err, "failed to close output file")
	}()

	// Trace time spent
	t1 := time.Now()

	// Find all duplicate files and report to output
	find := finder.New(*workerCount)
	for d := range find.AllDuplicateFiles(paths) {
		fmt.Fprintln(out, d)
	}

	// Update stats
	find.SetTimeSpent(time.Since(t1))

	// Display runtime stats if requested
	if *stats {
		log.Printf("INFO stats: %s", find.Stats())
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

// Helper to handle errors
func errHandle(err error, msg string) {
	if err != nil {
		log.Fatalf(msg+": %v", err)
	}
}

// vim: :sw=4:ts=4:noexpandtab
