package finder

import (
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/caelifer/dups/fstree"
	"github.com/caelifer/dups/mapreduce"
	"github.com/caelifer/dups/node"
	"github.com/caelifer/scheduler"
)

var nodePool *sync.Pool

func init() {
	nodePool = &sync.Pool{
		New: func() interface{} { return new(node.Node) },
	}
}

// Finder
type Finder struct {
	// Work Queue
	sched scheduler.Scheduler

	// Stats
	totalDirs        uint64
	totalFiles       uint64
	totalCopies      uint64
	totalWastedSpace uint64
	totalTime        time.Duration
}

func New(nworkers, njobs int) *Finder {
	return &Finder{sched: scheduler.New(nworkers, njobs)}
}

func (f *Finder) SetTimeSpent(d time.Duration) {
	f.totalTime = d
}

func (f Finder) Stats() string {
	// Stats report
	return fmt.Sprintf("Examined %d files in %d directories [%s], found %d dups, total wasted space %.2fGiB",
		f.totalFiles, f.totalDirs, f.totalTime, f.totalCopies, float64(f.totalWastedSpace)/(1024*1024*1024))
}

func (f *Finder) AllDups(paths []string) <-chan mapreduce.Value {
	// Build a processing pipeline
	return mapreduce.Pipeline(
		[]mapreduce.MapReducePair{
			{
				f.makeNodeMap(paths),
				f.reduceByFileName(),
			}, {
				f.makeFileSizeMap(),
				f.reduceByFileSize(),
			}, {
				f.makeFileHashMap(true),
				f.reduceByHash(),
			}, {
				f.makeFileHashMap(false),
				f.reduceByHash(),
			}, {
				f.reportMapDups(),
				f.reportReduceDups(),
			},
		}...,
	)
}

// makeNodeMap
func (f *Finder) makeNodeMap(paths []string) mapreduce.MapFn {
	return func(out chan<- mapreduce.KeyValue, _ <-chan mapreduce.Value) {
		// Process all command line paths
		for _, path_ := range paths {
			// err := filepath.Walk(path_, func(path string, info os.FileInfo, err error) error {
			err := fstree.Walk(f.sched, path_, func(path string, info os.FileInfo, err error) error {
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

					node := nodePool.Get().(*node.Node)
					node.Path =  path
					node.Size = size

					out <- mapreduce.NewKVType(
						mapreduce.KeyTypeFromString(path),
						node,
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
func (f *Finder) reduceByFileName() mapreduce.ReduceFn {
	return func(out chan<- mapreduce.Value, in <-chan mapreduce.KeyValue) {
		byName := make(map[mapreduce.KeyType]*node.Node)

		for x := range in {
			path := x.Key()                // Get key
			node := x.Value().(*node.Node) // Assert type

			// Add values to the map for aggregation, skip nodes with the same path
			if _, ok := byName[path]; !ok {
				byName[path] = node
				out <- node // send first copy
			} else {
				// Return node to pool
				nodePool.Put(node)
			}
		}
	}
}

// Very simple function to map nodes by size
func (*Finder) makeFileSizeMap() mapreduce.MapFn {
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
func (*Finder) reduceByFileSize() mapreduce.ReduceFn {
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

		// Clean-up
		for _, v := range bySize {
			if len(v) == 1 {
				// Return node back to pool
				nodePool.Put(v[0])
			}
		}
		// Finalize map
		bySize = nil
	}
}

func (f *Finder) makeFileHashMap(fast bool) mapreduce.MapFn {
	return func(out chan<- mapreduce.KeyValue, in <-chan mapreduce.Value) {
		wg := new(sync.WaitGroup) // Heap
		for x := range in {
			// Add to wait group
			wg.Add(1)

			node_ := x.Value().(*node.Node) // Assert type

			// Calculate hash using balancer
			go func(n *node.Node) {
				f.sched.Schedule(func() {
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
				})
			}(node_)
		}
		// Wait for all results be submitted
		wg.Wait()
	}
}

func (*Finder) reduceByHash() mapreduce.ReduceFn {
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

		// Clean-up
		for _, v := range byHash {
			if len(v) == 1 {
				// Return node back to pool
				nodePool.Put(v[0])
			}
		}
		// Finalize map
		byHash = nil
	}
}

// fanal map
func (*Finder) reportMapDups() mapreduce.MapFn {
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
func (f *Finder) reportReduceDups() mapreduce.ReduceFn {
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
