package main

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/caelifer/dups/balancer"
	"github.com/caelifer/dups/mapreduce"
)

// Node type
type Node struct {
	Path string // File path
	Size int64  // File size
	Hash string // Crypto signature in string form
}

func (n Node) Value() interface{} {
	return n
}

// Calculate hash
func (node *Node) calculateHash() {
	// Open file
	file, err := os.Open(node.Path)
	if err != nil {
		log.Println("WARN", err)
		return
	}
	defer file.Close()

	var n int64 // bytes read
	hash := sha1.New()

	// Filesystem and memory optimal read
	n, err = io.Copy(hash, file)

	// Check for normal errors
	if err != nil {
		log.Println("WARN", err)
		return
	}

	// Paranoid sanity check
	if n != node.Size {
		err = errors.New("Partial read")
		log.Println("WARN", err)
		return
	}

	// Add hash value
	node.Hash = fmt.Sprintf("%0x", hash.Sum(nil))

	// Collect garbage
	hash = nil
}

// Dup type
type Dup struct {
	Hash  string   // Crypto signature
	Paths []string // Paths with matching signatures
}

func (d Dup) Value() interface{} {
	return d
}

func (d Dup) String() string {
	out := ""
	hash := d.Hash
	num := len(d.Paths)

	for _, p := range d.Paths {
		out += fmt.Sprintf("%d:%s:%q\n", num, hash, p)
	}
	return out
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

	// Start map-reduce
	in := mapreduce.Map(makeMapFnWithPaths(paths))
	out := mapreduce.Reduce(in, reduceByFileSize)
	out = mapreduce.Reduce(out, reduceByHash)

	for dup := range out {
		fmt.Println(dup)
	}
}

type sameSizeNodes struct {
	nodes []Node
}

func (ssn sameSizeNodes) Value() interface{} {
	return ssn.nodes
}

func reduceByFileSize(out chan<- mapreduce.Result, in <-chan mapreduce.Result) {
	bySize := make(map[int64][]Node)

	for x := range in {
		n := x.Value().(Node) // Assert Node type
		if v, ok := bySize[n.Size]; ok {
			// Found node with the same file size
			bySize[n.Size] = append(v, n)
		} else {
			bySize[n.Size] = []Node{n}
		}
	}

	for _, nodes := range bySize {
		if len(nodes) > 1 {
			// Send output for potential duplicates
			out <- mapreduce.Result(sameSizeNodes{nodes: nodes})
		}
	}
	close(out)
}

func reduceByHash(out chan<- mapreduce.Result, in <-chan mapreduce.Result) {
	byHash := make(map[string][]string)

	for x := range in {
		nodes := x.Value().([]Node) // Assert type

		// Map sha1 hash to each node and reduce
		in := mapreduce.Map(func(out chan<- mapreduce.Result) {
			var wg sync.WaitGroup

			// Process all command line paths
			for _, node := range nodes {
				// Add to wait group
				wg.Add(1)

				// Calculate hash using balancer
				go func(n Node) {
					WorkQueue <- func() {
						defer wg.Done() // Signal done
						n.calculateHash()
						out <- n
					}
				}(node)

			}

			// Wait until all results are in
			wg.Wait()

			// Close output channel when all nodes were processed
			close(out)
		})

		// Populate byHash map
		for x := range in { // shadow top-level x
			n := x.(Node) // Assert type
			if v, ok := byHash[n.Hash]; ok {
				// Found node with the same file size
				byHash[n.Hash] = append(v, n.Path)
			} else {
				byHash[n.Hash] = []string{n.Path}
			}
		}
	}

	for hash, paths := range byHash {
		if len(paths) > 1 {
			// Send output for potential duplicates
			out <- mapreduce.Result(Dup{Hash: hash, Paths: paths})
		}
	}
	close(out)
}

func makeMapFnWithPaths(paths []string) mapreduce.MapFn {
	return func(out chan<- mapreduce.Result) {

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
					out <- Node{Path: path, Size: info.Size()}
					// log.Printf("Accepted %q\n", path)
				}
				return nil
			})

			if err != nil {
				log.Fatal(err)
			}
		}

		// Close output channel when all nodes were processed
		close(out)
	}
}

func IsFile(fi os.FileInfo) bool {
	return fi.Mode()&os.ModeType == 0
}
