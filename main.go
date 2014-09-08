package main

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
)

// Node type
type Node struct {
	Path string // File path
	Hash string // Crypto signature in string form
}

// Constructor function from
func MakeNode(path string, fi os.FileInfo) (Node, error) {
	node := Node{Path: path}

	// Open file
	file, err := os.Open(path)
	if err != nil {
		return node, err
	}
	defer file.Close()

	var n int64 // bytes read
	hash := sha1.New()

	// Filesystem and memory optimal read
	n, err = io.Copy(hash, file)

	// Check for normal errors
	if err != nil {
		return node, err
	}

	// Paranoid sanity check
	if n != fi.Size() {
		return node, errors.New("Partial read")
	}

	// Add hash value
	node.Hash = fmt.Sprintf("%0x", hash.Sum(nil))

	// Collect garbage
	hash = nil

	return node, nil
}

// Dup type
type Dup struct {
	Hash  string   // Crypto signature
	Paths []string // Paths with matching signatures
}

func (d Dup) String() string {
	return fmt.Sprintf("%s %+v", d.Hash, d.Paths)
}

func main() {
	// Process command line params
	paths := os.Args[1:]

	if len(paths) == 0 {
		// Default is current directory
		paths = []string{"."}
	}

	// Start map-reduce
	for dup := range Reduce(Map(paths)) {
		fmt.Println(dup)
	}
}

func Reduce(in <-chan Node) <-chan Dup {
	out := make(chan Dup)
	nodes := make(map[string][]string)

	// Synchronously populate thread-unsafe map
	for n := range in {
		if v, ok := nodes[n.Hash]; ok {
			// Found dups
			nodes[n.Hash] = append(v, n.Path)
			// log.Printf("DEBUG Found dups for %s - %+v\n", n.Hash, nodes[n.Hash])
		} else {
			// Add new node
			nodes[n.Hash] = []string{n.Path}
		}
	}

	// Asyncronously send output
	go func() {
		for hash, paths := range nodes {
			if len(paths) > 1 {
				// Construct Dup and send it out
				out <- Dup{
					Hash:  hash,
					Paths: paths,
				}
			}
		}
		// Don't foget to clean-up
		close(out)
	}()

	return out
}

func Map(paths []string) <-chan Node {
	out := make(chan Node)

	// Start filesystem tree walking
	go func() {
		var wg sync.WaitGroup

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
					// Add to wait group
					wg.Add(1)

					// Calculate hash
					go func(path string, fi os.FileInfo) {
						defer wg.Done() // Signal done
						n, err := MakeNode(path, fi)
						if err != nil {
							log.Println("WARN", err)
						}
						out <- n
					}(path, info)
				}
				return nil
			})

			if err != nil {
				log.Fatal(err)
			}
		}

		// Wait until all results are in
		wg.Wait()

		// Close output channel when all nodes were processed
		close(out)
	}()

	return out
}

func IsFile(fi os.FileInfo) bool {
	return fi.Mode()&os.ModeType == 0
}
