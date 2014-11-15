package finder

import (
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"

	"github.com/caelifer/dups/balancer"
	"github.com/caelifer/dups/fstree"
	"github.com/caelifer/dups/mapreduce"
	"github.com/caelifer/dups/node"
)

// Dup type describes found duplicate file
type Dup struct {
	*node.Node     // Embed Node type Go type "inheritance"
	Count      int // Number of identical copies for the hash
}

// Value implements mapreduce.Value interface
func (d Dup) Value() interface{} {
	return d
}

// Pretty printer for the report
func (d Dup) String() string {
	return fmt.Sprintf("%s:%d:%d:%q", d.Hash, d.Count, d.Size, d.Path)
}

// Finder
type Finder struct {
	// Work Queue
	workQueue chan<- balancer.Request

	// Stats
	totalDirs        uint64
	totalFiles       uint64
	totalCopies      uint64
	totalWastedSpace uint64
}

func NewFinder(nworkers int) *Finder {
	return &Finder{workQueue: balancer.NewWorkQueue(nworkers)}
}

func (f Finder) ReportStats() string {
	// Stats report
	return fmt.Sprintf("Examined %d files in %d directories, found %d dups, total wasted space %.2fGB\n",
		f.totalFiles, f.totalDirs, f.totalCopies, float64(f.totalWastedSpace)/(1024*1024*1024))
}

func (f *Finder) AllDups(paths []string) <-chan mapreduce.Value {
	// Build a processing pipeline
	return mapreduce.Pipeline(
		[]mapreduce.MapReducePair{
			{
				makeNodeMap(f, paths),
				reduceByFileName(f),
			}, {
				makeFileSizeMap(f),
				reduceByFileSize(f),
			}, {
				makeFileHashMap(f, true),
				reduceByHash(f),
			}, {
				makeFileHashMap(f, false),
				reduceByHash(f),
			}, {
				reportMapDups(f),
				reportReduceDups(f),
			},
		}...,
	)
}

// makeNodeMap
func makeNodeMap(f *Finder, paths []string) mapreduce.MapFn {
	return func(out chan<- mapreduce.KeyValue, _ <-chan mapreduce.Value) {
		// Process all command line paths
		for _, path_ := range paths {
			// err := filepath.Walk(path_, func(path string, info os.FileInfo, err error) error {
			err := fstree.Walk(f.workQueue, path_, func(path string, info os.FileInfo, err error) error {
				// Handle passthrough error
				if err != nil {
					log.Println("WARN", err)
					return nil
				}

				// Only process simple files
				if info.IsDir() {
					// Increase seen directory counter
					atomic.AddUint64(&f.totalDirs, 1)
				}

				// Only process simple files
				if IsRegularFile(info) {
					size := info.Size()

					out <- mapreduce.NewKVType(
						mapreduce.KeyTypeFromString(path),
						&node.Node{Path: path, Size: size},
					)
					// Increase seen files counter
					atomic.AddUint64(&f.totalFiles, 1)
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
func reduceByFileName(*Finder) mapreduce.ReduceFn {
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
func makeFileSizeMap(*Finder) mapreduce.MapFn {
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
func reduceByFileSize(*Finder) mapreduce.ReduceFn {
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

func makeFileHashMap(f *Finder, fast bool) mapreduce.MapFn {
	return func(out chan<- mapreduce.KeyValue, in <-chan mapreduce.Value) {
		var wg sync.WaitGroup
		for x := range in {
			node_ := x.Value().(*node.Node) // Assert type

			// Add to wait group
			wg.Add(1)

			// Calculate hash using balancer
			go func(n *node.Node) {
				f.workQueue <- func() {
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

func reduceByHash(*Finder) mapreduce.ReduceFn {
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

// fanal map
func reportMapDups(*Finder) mapreduce.MapFn {
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
func reportReduceDups(f *Finder) mapreduce.ReduceFn {
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
				atomic.AddUint64(&f.totalWastedSpace, uint64(nodes[0].Size*int64(count-1)))

				for _, n := range nodes {
					// Update dups number stats
					atomic.AddUint64(&f.totalCopies, 1)
					out <- &Dup{n, count}
				}
			}
		}
	}
}

func IsRegularFile(fi os.FileInfo) bool {
	return fi.Mode()&os.ModeType == 0
}
