package fstree

import (
	"io/ioutil"
	"log"
	"os"
	"path"
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
	w := newWalker(workQueue, path)

	// Construct node from provided path
	info, err := os.Lstat(path)

	// On success ...
	if err == nil {
		// Process node
		err = w.walkNode(newNode(path, info), nil, fn)
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

func newWalker(workQueue chan<- balancer.Request, root string) *walker {
	return &walker{
		root:      root,
		workQueue: workQueue,
	}
}

func (w *walker) walkNode(node *node, err error, fn nodeFn) error {
	// Process node by calling client function
	err = fn(node.path, node.info, err)

	// ... then, recursively process directories
	if node.info.IsDir() {
		// Traverse directrory asnyncronously using balancer
		w.walkDir(node, err, fn)
	}

	return err
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
				path := path.Join(node.path, entry.Name())

				// Process node, ignore errors
				w.walkNode(newNode(path, entry), nil, fn)
			}
		}
	}()
}
