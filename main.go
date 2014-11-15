package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"runtime/pprof"

	"github.com/caelifer/dups/balancer"
	"github.com/caelifer/dups/finder"
)

// Internal constant to control number of Worker threads in balancer's worker pool
const workerPoolMultiplier = 2 // Use twice the available cores

// Flags
var (
	cpuprofile      = flag.String("cpuprofile", "", "write cpu profile to file")
	memprofile      = flag.String("memprofile", "", "write memory profile to file")
	maxWorkerNumber = flag.Int("jobs", runtime.NumCPU()*workerPoolMultiplier, "Number of parallel jobs")
	output          = flag.String("output", "os.Stdout", "write output to a file")
)

// Global pool manager interfaced via WorkQueue
var WorkQueue = balancer.NewWorkQueue(*maxWorkerNumber)

func main() {
	// First parse flags
	flag.Parse()

	// Prep runtime to use the maxWorkerNumber real threads
	runtime.GOMAXPROCS(*maxWorkerNumber)

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

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
	outfile, err := getOutput(*output)
	if err != nil {
		log.Fatal(err)
	}
	defer outfile.Close()

	// Find all dups and report to output
	find := finder.NewFinder(*maxWorkerNumber)
	for d := range find.AllDups(paths) {
		fmt.Fprintln(outfile, d)
	}
}

func getOutput(path string) (io.WriteCloser, error) {
	switch path {
	case "os.Stdout", "-":
		return os.Stdout, nil // default
	default:
		return os.OpenFile(*output, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0666)
	}

}
