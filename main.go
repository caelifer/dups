package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/caelifer/dups/balancer"
	"github.com/caelifer/dups/mapreduce"
)

// Dup type describes found duplicat file
type Dup struct {
	Hash  string // Crypto signature
	Count int    // Number of identical copies for the hash
	Path  string // Paths with matching signatures
}

// Value implements mapreduce.Value interface
func (d Dup) Value() interface{} {
	return d
}

func (d Dup) String() string {
	return fmt.Sprintf("%d:%s:%q", d.Count, d.Hash, d.Path)
}

// Global pool manager
var WorkQueue = make(chan balancer.Request)

// Start balancer on the background
var _ = balancer.New(WorkQueue)

func main() {
	// Use all available CPU cores
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Process command line params
	paths := os.Args[1:]

	if len(paths) == 0 {
		// Default is current directory
		paths = []string{"."}
	}

	// Channel interfaces between Map() and Reduce() functions
	var keyValChan <-chan mapreduce.KeyValue
	var valChan <-chan mapreduce.Value

	// Start map-reduce
	keyValChan = mapreduce.Map(makeFileSizeMapFnWithPaths(paths))
	valChan = mapreduce.Reduce(keyValChan, reduceByFileSize)

	keyValChan = mapreduce.Map(makeFileHashMapFnFrom(valChan, true))
	valChan = mapreduce.Reduce(keyValChan, reduceByHash)

	keyValChan = mapreduce.Map(makeFileHashMapFnFrom(valChan, false))
	valChan = mapreduce.Reduce(keyValChan, reduceByHash)

	// Final reduce before reporting
	dups := make(chan Dup)
	go func(out chan<- Dup) {
		byHash := make(map[string][]string)
		for x := range valChan {
			n := x.Value().(Node) // Type assert

			// Aggregate
			if v, ok := byHash[n.Hash]; ok {
				// Found node with the same file size
				byHash[n.Hash] = append(v, n.Path)
			} else {
				byHash[n.Hash] = []string{n.Path}
			}
		}

		// Reduce
		for hash, paths := range byHash {
			count := len(paths)
			if count > 1 {
				for _, path := range paths {
					out <- Dup{Hash: hash, Count: count, Path: path}
				}
			}
		}
		close(out)
	}(dups)

	// Report
	for d := range dups {
		fmt.Println(d)
	}
}

// makeFileSizeMapFnWithPaths
func makeFileSizeMapFnWithPaths(paths []string) mapreduce.MapFn {
	return func(out chan<- mapreduce.KeyValue) {

		// Process all command line paths
		for _, p := range paths {
			err := filepath.Walk(p, func(path string, info os.FileInfo, err error) error {
				// Handle passthrough error
				if err != nil {
					log.Println("WARN", err)
					return nil
				}

				// Only process simple files
				if IsFile(info) {
					size := info.Size()
					out <- mapreduce.NewKVType(
						mapreduce.KeyTypeFromInt64(size),
						Node{Path: path, Size: size},
					)
				}
				return nil
			})

			if err != nil {
				log.Fatal(err)
			}
		}
	}
}

// reduceByFileSize custom function to filter files by size
func reduceByFileSize(out chan<- mapreduce.Value, in <-chan mapreduce.KeyValue) {
	bySize := make(map[string][]mapreduce.Value)

	for x := range in {
		size := x.Key()          // Get key
		node := x.Value().(Node) // Assert type

		// Add values to the map for aggregation
		if v, ok := bySize[size]; ok {
			// Found node with the same file size
			bySize[size] = append(v, node)
		} else {
			bySize[size] = []mapreduce.Value{node}
		}
	}

	// Send potential duplicates one-by-one to the downstream processing
	for _, nodes := range bySize {
		// Found cluster
		if len(nodes) > 1 {
			for _, n := range nodes {
				out <- n
			}
		}
	}
}

func makeFileHashMapFnFrom(in <-chan mapreduce.Value, fast bool) mapreduce.MapFn {
	return func(out chan<- mapreduce.KeyValue) {
		var wg sync.WaitGroup
		for x := range in {
			node := x.Value().(Node) // Assert type

			// Add to wait group
			wg.Add(1)

			// Calculate hash using balancer
			go func(n Node) {
				WorkQueue <- func() {
					defer wg.Done()               // Signal done
					hash := n.calculateHash(fast) // Fast hash calculation - SHA1 has of first 1024 bytes

					// Don't process files for which we failed to calculate SHA1 hash
					if hash == "" {
						return
					}

					out <- mapreduce.NewKVType(
						mapreduce.KeyTypeFromString(hash),
						n,
					)
				}
			}(node)
		}
		// Wait for all results be submitted
		wg.Wait()
	}
}

func reduceByHash(out chan<- mapreduce.Value, in <-chan mapreduce.KeyValue) {
	byHash := make(map[string][]Node)

	for x := range in {
		hash := x.Key()
		node := x.Value().(Node) // Assert type

		if v, ok := byHash[hash]; ok {
			// Found node with the same file size
			byHash[hash] = append(v, node)
		} else {
			byHash[hash] = []Node{node}
		}
	}

	// Send out aggregeted results
	for hash, nodes := range byHash {
		if len(nodes) > 1 {
			// Send output for potential duplicates
			for _, node := range nodes {
				node.Hash = hash
				out <- mapreduce.Value(node)
			}
		}
	}
}

func IsFile(fi os.FileInfo) bool {
	return fi.Mode()&os.ModeType == 0
}
