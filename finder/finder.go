package finder

import (
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/caelifer/scheduler"

	"github.com/caelifer/dups/fstree"
	"github.com/caelifer/dups/mapreduce"
	"github.com/caelifer/dups/node"
)

// Finder
type Finder struct {
	// Work Queue
	scheduler scheduler.Scheduler

	// Stats
	totalDirs        uint64
	totalFiles       uint64
	totalCopies      uint64
	totalWastedSpace uint64
	totalTime        time.Duration
}

func New(nWorkers int) *Finder {
	return &Finder{scheduler: scheduler.New(nWorkers)}
}

func (f *Finder) SetTimeSpent(d time.Duration) {
	f.totalTime = d
}

func (f Finder) Stats() string {
	// Stats report
	return fmt.Sprintf("examined %d files in %d directories [%s], found %d dups, total wasted space %.2fGiB",
		f.totalFiles, f.totalDirs, f.totalTime, f.totalCopies, float64(f.totalWastedSpace)/(1024*1024*1024))
}

func (f *Finder) AllDuplicateFiles(paths []string) <-chan mapreduce.Value {
	// Build a processing pipeline
	return mapreduce.Pipeline(
		[]mapreduce.MapReducePair{
			{
				Map:    f.makeNodeMap(paths),
				Reduce: mapreduce.FilterOutDuplicates,
			}, {
				Map:    f.makeFileSizeMap(),
				Reduce: mapreduce.FilterOutUniques,
			}, {
				Map:    f.makeFileHashMap(),
				Reduce: mapreduce.FilterOutUniques,
			}, {
				Map:    f.mapDups(),
				Reduce: f.reduceDups(),
			},
		}...,
	)
}

// makeNodeMap
func (f *Finder) makeNodeMap(paths []string) mapreduce.MapFn {
	return func(out chan<- mapreduce.KeyValue, _ <-chan mapreduce.Value) {
		// Process all command line paths
		for _, p := range paths {
			// err := filepath.Walk(path_, func(path string, info os.FileInfo, err error) error {
			err := fstree.Walk(f.scheduler, p, func(path string, info os.FileInfo, err error) error {
				// Handle passthroughs error
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
			n := x.Value().(*node.Node) // Assert type
			out <- mapreduce.NewKVType(mapreduce.KeyTypeFromInt64(n.Size), n)
		}
	}
}

func (f *Finder) makeFileHashMap() mapreduce.MapFn {
	return func(out chan<- mapreduce.KeyValue, in <-chan mapreduce.Value) {
		wg := new(sync.WaitGroup) // Heap
		for x := range in {
			// Add to wait group
			wg.Add(1)
			// Calculate hash using balancer
			go func(n *node.Node) {
				f.scheduler.Schedule(func() {
					defer wg.Done() // Signal done
					err := n.CalculateHash()
					if err != nil {
						// Skip files for which we failed to calculate SHA1 hash
						// log.Printf("WARN Unable calculate SHA1 hash for %q\n", node.Path)
						return
					}
					// Report result
					out <- mapreduce.NewKVType(
						mapreduce.KeyTypeFromString(n.Hash),
						n,
					)
				})
			}(x.Value().(*node.Node))
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
	}
}

func isRegularFile(fi os.FileInfo) bool {
	return fi.Mode()&os.ModeType == 0
}
