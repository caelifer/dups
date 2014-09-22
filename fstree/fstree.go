package fstree

import (
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/caelifer/dups/balancer"
)

// Distributed file system tree walker

// Heler type - matches parameter signature of filepath.Walk()
type nodeFn func(path string, info os.FileInfo, err error) error

// Primary interface - matches signature of filepath.Walk()
func Walk(workQueue chan<- balancer.Request, path string, fn nodeFn) error {
	// Create walker object
	w := newWalker(path, workQueue)

	// Construct node from provided path
	info, err := os.Lstat(path)

	// On success ...
	if err == nil {
		n := newNode(path, info)

		err = w.walkNode(n, nil, fn)

		// Check if node is directory
		if n.info.IsDir() {
			// Traverse directrory asnyncronously
			w.walkDir(n, err, fn)

		}
	}

	// Wait util all nodes are processed
	w.wg.Wait()

	return err
}

type node struct {
	path string
	info os.FileInfo
}

func newNode(path string, info os.FileInfo) *node {
	return &node{path: filepath.Clean(path), info: info}
}

type walker struct {
	root      string
	workQueue chan<- balancer.Request
	wg        sync.WaitGroup
}

func newWalker(root string, workQueue chan<- balancer.Request) *walker {
	return &walker{
		root:      root,
		workQueue: workQueue,
	}
}

func (w *walker) walkNode(node *node, err error, fn nodeFn) error {
	// log.Println("XXX Processing", node.path)
	// Process node by calling client function
	return fn(node.path, node.info, err)
}

func (w *walker) walkDir(node *node, err error, fn nodeFn) {
	if err != nil {
		log.Println("WARN", err)
		return
	}

	// Make sure we are not finished until all recursive calls are done
	w.wg.Add(1)

	// Send to be processed in the workpool
	go func() {
		w.workQueue <- func() {
			defer w.wg.Done() // Signal done at the end of the function

			// Read directory entries
			dirents, err := ioutil.ReadDir(node.path)
			if err != nil {
				log.Println("WARN", err)

				// erly termination if we cannot read directory
				return
			}

			// Read all entries in current directory
			for _, entry := range dirents {
				path := node.path + string(os.PathSeparator) + entry.Name()

				// Create node and report error if we have problem with Lstat call
				n := newNode(path, entry)

				// Recursively process each node

				// First, process current node
				err = w.walkNode(n, err, fn)

				// Check if it is a directory
				if n.info.IsDir() {
					// Traverse directrory asnyncronously
					w.walkDir(n, err, fn)
				}
			}
		}
	}()
}
