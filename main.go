package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"sync/atomic"

	"github.com/caelifer/dups/balancer"
	"github.com/caelifer/dups/fstree"
	"github.com/caelifer/dups/mapreduce"
	"github.com/caelifer/dups/node"
)

// Global stats for activity report
var stats struct {
	TotalDirs        uint64
	TotalFiles       uint64
	TotalCopies      uint64
	TotalWastedSpace uint64
}

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

	// Build a processing pipeline
	dups := mapreduce.Pipeline(
		[]mapreduce.MapReducePair{
			{
				makeNodeMapFnWithPaths(paths),
				reduceByFileName(),
			}, {
				makeFileSizeMapFn(),
				reduceByFileSize(),
			}, {
				makeFileHashMapFn(true),
				reduceByHash(),
			}, {
				makeFileHashMapFn(false),
				reduceByHash(),
			}, {
				reportMapDups(),
				reportReduceDups(),
			},
		}...,
	)

	// Get output writer
	outfile, err := getOutput(*output)
	if err != nil {
		log.Fatal(err)
	}
	defer outfile.Close()

	// Report to os.Stdout
	for d := range dups {
		fmt.Fprintln(outfile, d)
	}

	// Stats report
	log.Printf("Examined %d files in %d directories, found %d dups, total wasted space %.2fGB\n",
		stats.TotalFiles, stats.TotalDirs, stats.TotalCopies, float64(stats.TotalWastedSpace)/(1024*1024*1024))
}

func getOutput(path string) (io.WriteCloser, error) {
	switch path {
	case "os.Stdout", "-":
		return os.Stdout, nil // default
	default:
		return os.OpenFile(*output, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0666)
	}

}

// fanal map
func reportMapDups() mapreduce.MapFn {
	return func(out chan<- mapreduce.KeyValue, in <-chan mapreduce.Value) {
		for x := range in {
			n := x.Value().(*node.Node) // Type assert
			h := n.Hash
			out <- mapreduce.NewKVType(
				mapreduce.KeyTypeFromString(h),
				n,
			)
		}
	}
}

// final reduce
func reportReduceDups() mapreduce.ReduceFn {
	return func(out chan<- mapreduce.Value, in <-chan mapreduce.KeyValue) {
		byHash := make(map[string][]*node.Node)

		for x := range in {
			n := x.Value().(*node.Node) // Type assert

			// Aggregate
			if v, ok := byHash[n.Hash]; ok {
				// Found node with the same content
				byHash[n.Hash] = append(v, n)
			} else {
				byHash[n.Hash] = []*node.Node{n}
			}
		}

		// Reduce
		for _, nodes := range byHash {
			count := len(nodes)
			if count > 1 {
				// Update free size stats
				atomic.AddUint64(&stats.TotalWastedSpace, uint64(nodes[0].Size*int64(count-1)))

				for _, n := range nodes {
					// Update dups number stats
					atomic.AddUint64(&stats.TotalCopies, 1)
					out <- node.Dup{n, count}
				}
			}
		}
	}
}

// makeNodeMapFnWithPaths
func makeNodeMapFnWithPaths(paths []string) mapreduce.MapFn {
	return func(out chan<- mapreduce.KeyValue, _ <-chan mapreduce.Value) {
		// Process all command line paths
		for _, path_ := range paths {
			// err := filepath.Walk(path_, func(path string, info os.FileInfo, err error) error {
			err := fstree.Walk(WorkQueue, path_, func(path string, info os.FileInfo, err error) error {
				// Handle passthrough error
				if err != nil {
					log.Println("WARN", err)
					return nil
				}

				// Only process simple files
				if info.IsDir() {
					// Increase seen directory counter
					atomic.AddUint64(&stats.TotalDirs, 1)
				}

				// Only process simple files
				if IsRegularFile(info) {
					size := info.Size()
					out <- mapreduce.NewKVType(
						mapreduce.KeyTypeFromString(path),
						&node.Node{Path: path, Size: size},
					)
					// Increase seen files counter
					atomic.AddUint64(&stats.TotalFiles, 1)
				}
				return nil
			})

			if err != nil {
				log.Fatal(err)
			}
		}
	}
}

// reduceByFileName custom function remove nodes with duplicate paths
func reduceByFileName() mapreduce.ReduceFn {
	return func(out chan<- mapreduce.Value, in <-chan mapreduce.KeyValue) {
		byName := make(map[mapreduce.KeyType]*node.Node)

		for x := range in {
			path := x.Key()                // Get key
			node := x.Value().(*node.Node) // Assert type

			// Add values to the map for aggregation, skip nodes with the same path
			if _, ok := byName[path]; !ok {
				byName[path] = node
				out <- node // send first copy
			}
		}
	}
}

// Very simple function to map nodes by size
func makeFileSizeMapFn() mapreduce.MapFn {
	return func(out chan<- mapreduce.KeyValue, in <-chan mapreduce.Value) {
		for x := range in {
			node := x.Value().(*node.Node) // Assert type

			out <- mapreduce.NewKVType(
				mapreduce.KeyTypeFromInt64(node.Size),
				node,
			)
		}
	}
}

// reduceByFileSize custom function to filter files by size
func reduceByFileSize() mapreduce.ReduceFn {
	return func(out chan<- mapreduce.Value, in <-chan mapreduce.KeyValue) {
		bySize := make(map[mapreduce.KeyType][]*node.Node)

		for x := range in {
			size := x.Key()             // Get key
			n := x.Value().(*node.Node) // Assert type

			// Add values to the map for aggregation
			if v, ok := bySize[size]; ok {
				// Found node with the same file size
				if len(v) == 1 {
					// First time we found duplicate, send first node too
					out <- v[0]
				}
				// Add new node to a list
				bySize[size] = append(v, n)
				// Send duplicate downstream
				out <- n
			} else {
				// Store first copy
				bySize[size] = []*node.Node{n}
			}
		}
	}
}

func makeFileHashMapFn(fast bool) mapreduce.MapFn {
	return func(out chan<- mapreduce.KeyValue, in <-chan mapreduce.Value) {
		var wg sync.WaitGroup
		for x := range in {
			node_ := x.Value().(*node.Node) // Assert type

			// Add to wait group
			wg.Add(1)

			// Calculate hash using balancer
			go func(n *node.Node) {
				WorkQueue <- func() {
					defer wg.Done() // Signal done

					// Calculate hash using fast calculation if required
					err := n.CalculateHash(fast) // Fast hash calculation - SHA1 of first 1024 bytes
					if err != nil {
						// Skip files for which we failed to calculate SHA1 hash
						// log.Printf("WARN Unable calculate SHA1 hash for %q\n", node.Path)
						return
					}

					out <- mapreduce.NewKVType(
						mapreduce.KeyTypeFromString(n.Hash),
						n,
					)
				}
			}(node_)
		}
		// Wait for all results be submitted
		wg.Wait()
	}
}

func reduceByHash() mapreduce.ReduceFn {
	return func(out chan<- mapreduce.Value, in <-chan mapreduce.KeyValue) {
		byHash := make(map[mapreduce.KeyType][]*node.Node)

		for x := range in {
			hash := x.Key()
			n := x.Value().(*node.Node) // Assert type

			// Add hash value to a node
			n.Hash = hash.String()

			if v, ok := byHash[hash]; ok {
				// Found node with the same SHA1 hash
				// Send out aggregeted results
				if len(v) == 1 {
					// First time we found duplicate, send first node too
					out <- v[0]
				}
				// Add new node to a list
				byHash[hash] = append(v, n)
				// Send new node
				out <- n
			} else {
				byHash[hash] = []*node.Node{n}
			}
		}
	}
}

func IsRegularFile(fi os.FileInfo) bool {
	return fi.Mode()&os.ModeType == 0
}
