package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/caelifer/dups/balancer"
	"github.com/caelifer/dups/mapreduce"
)

// Dup type describes found duplicat file
type Dup struct {
	Count int    // Number of identical copies for the hash
	Size  int64  // File size
	Hash  string // Crypto signature
	Path  string // Paths with matching signatures
}

// Value implements mapreduce.Value interface
func (d Dup) Value() interface{} {
	return d
}

func (d Dup) String() string {
	return fmt.Sprintf("%d:%s:%q:%d", d.Count, d.Hash, d.Path, d.Size)
}

// Global stats for activity report
var stats struct {
	TotlalNodes   uint64
	TotalCopies   uint64
	TotalFreeSize uint64
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

	// Start map-reduce and remove duplicate path nodes
	keyValChan = mapreduce.Map(makeNodeMapFnWithPaths(paths))
	valChan = mapreduce.Reduce(keyValChan, reduceByFileName)

	// Map by filesize
	keyValChan = mapreduce.Map(makeFileSizeMapFnFrom(valChan))
	valChan = mapreduce.Reduce(keyValChan, reduceByFileSize)

	// Map by fast SHA1 hash (first 1024 bytes)
	keyValChan = mapreduce.Map(makeFileHashMapFnFrom(valChan, true))
	valChan = mapreduce.Reduce(keyValChan, reduceByHash)

	// Map by SHA1 hash (full file hashing)
	keyValChan = mapreduce.Map(makeFileHashMapFnFrom(valChan, false))
	valChan = mapreduce.Reduce(keyValChan, reduceByHash)

	// Final reduce before reporting
	dups := make(chan Dup)
	go func(out chan<- Dup) {
		byHash := make(map[string][]Node)

		for x := range valChan {
			n := x.Value().(Node) // Type assert

			// Aggregate
			if v, ok := byHash[n.Hash]; ok {
				// Found node with the same file size
				byHash[n.Hash] = append(v, n)
			} else {
				byHash[n.Hash] = []Node{n}
			}
		}

		// Reduce
		for hash, nodes := range byHash {
			count := len(nodes)
			if count > 1 {
				// Update free size stats
				atomic.AddUint64(&stats.TotalFreeSize, uint64(nodes[0].Size*int64(count-1)))

				for _, node := range nodes {
					// Update dups number stats
					atomic.AddUint64(&stats.TotalCopies, uint64(1))
					out <- Dup{Count: count, Size: node.Size, Hash: hash, Path: node.Path}
				}
			}
		}
		close(out)
	}(dups)

	// Report
	for d := range dups {
		fmt.Println(d)
	}
	// Stats report
	fmt.Printf("Stats: examined %d files, found %d dups, total free size: %.2fGB\n",
		stats.TotlalNodes, stats.TotalCopies, float64(stats.TotalFreeSize)/(1024*1024*1024))
}

// makeNodeMapFnWithPaths
func makeNodeMapFnWithPaths(paths []string) mapreduce.MapFn {
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
						mapreduce.KeyTypeFromString(path),
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

// reduceByFileName custom function remove nodes with duplicate paths
func reduceByFileName(out chan<- mapreduce.Value, in <-chan mapreduce.KeyValue) {
	bySize := make(map[mapreduce.KeyType]mapreduce.Value)

	for x := range in {
		path := x.Key()          // Get key
		node := x.Value().(Node) // Assert type

		// Add values to the map for aggregation, skip nodes with the same path
		if _, ok := bySize[path]; !ok {
			bySize[path] = node
		}
	}

	// Send potential duplicates one-by-one to the downstream processing
	for _, node := range bySize {
		// Increase seen files counter
		atomic.AddUint64(&stats.TotlalNodes, 1)

		out <- node
	}
}

// Very simple function to map nodes by size
func makeFileSizeMapFnFrom(in <-chan mapreduce.Value) mapreduce.MapFn {
	return func(out chan<- mapreduce.KeyValue) {
		for x := range in {
			node := x.Value().(Node) // Assert type

			out <- mapreduce.NewKVType(
				mapreduce.KeyTypeFromInt64(node.Size),
				node,
			)
		}
	}
}

// reduceByFileSize custom function to filter files by size
func reduceByFileSize(out chan<- mapreduce.Value, in <-chan mapreduce.KeyValue) {
	bySize := make(map[mapreduce.KeyType][]mapreduce.Value)

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
					defer wg.Done() // Signal done

					// Default hash value for files < 1024 if fast is true
					hash := "0h"

					// Little optimization to avoid calculating fast hash on small files
					if !fast || node.Size > blockSize {
						// Always calculate hash if fast == false or file size > blockSize
						hash = n.calculateHash(fast) // Fast hash calculation - SHA1 of first 1024 bytes
					}

					// Don't process files for which we failed to calculate SHA1 hash
					if hash == "" {
						log.Printf("WARN Unable calculate SHA1 hash for %q\n", node.Path)
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
	byHash := make(map[mapreduce.KeyType][]Node)

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
				node.Hash = hash.String()
				out <- mapreduce.Value(node)
			}
		}
	}
}

func IsFile(fi os.FileInfo) bool {
	return fi.Mode()&os.ModeType == 0
}
