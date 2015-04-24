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
				mapreduce.FilterUnique,
			}, {
				f.makeFileSizeMap(),
				mapreduce.FilterMatching,
			}, {
				f.makeFileHashMap(true),
				mapreduce.FilterMatching,
			}, {
				f.makeFileHashMap(false),
				mapreduce.FilterMatching,
			}, {
				f.mapDups(),
				f.reduceDups(),
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
				if isRegularFile(info) {
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

// fanal map
func (f *Finder) mapDups() mapreduce.MapFn {
	return func(out chan<- mapreduce.KeyValue, in <-chan mapreduce.Value) {
		for x := range in {
			// Update stats
			atomic.AddUint64(&f.totalCopies, 1)
			n := x.Value().(*node.Node) // Type assert
			out <- mapreduce.NewKVType(
				mapreduce.KeyType(n.Hash),
				&Dup{Node: n},
			)
		}
	}
}

// final reduce
func (f *Finder) reduceDups() mapreduce.ReduceFn {
	return func(out chan<- mapreduce.Value, in <-chan mapreduce.KeyValue) {
		byHash := make(map[string][]Dup)

		for x := range in {
			d := x.Value().(Dup) // Type assert

			// Aggregate
			if v, ok := byHash[d.Hash]; ok {
				// Found node with the same content
				byHash[d.Hash] = append(v, d)
			} else {
				byHash[d.Hash] = []Dup{d}
			}
		}

		// Reduce
		for _, dups := range byHash {
			count := len(dups)
			// Update free size stats
			atomic.AddUint64(&f.totalWastedSpace, uint64(dups[0].Size*int64(count-1)))

			for _, d := range dups {
				// Update dups number stats
				d.Count = count
				out <- d
			}
		}

		byHash = nil
	}
}

func isRegularFile(fi os.FileInfo) bool {
	return fi.Mode()&os.ModeType == 0
}
