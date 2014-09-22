package main

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
)

const blockSize = 1024 // Guestimate of a FS block-size for optimal read call

// Node type
type Node struct {
	Path string // File path
	Size int64  // File size
	Hash string // String form of SHA1 hash
}

func (n *Node) Value() interface{} {
	return n
}

// Calculate hash
func (node *Node) calculateHash(fast bool) string {
	// Open file
	file, err := os.Open(node.Path)
	if err != nil {
		log.Println("WARN", err)
		return ""
	}
	defer file.Close()

	var n int64 // bytes read
	hash := sha1.New()

	if fast && node.Size > blockSize {
		// Make one read call
		_, err = io.CopyN(hash, file, blockSize)

	} else {
		// Filesystem and memory optimal read
		n, err = io.Copy(hash, file)

		// Paranoid sanity check
		if n != node.Size {
			err = errors.New("Partial read: " + node.Path)
			log.Println("WARN", err)
			return ""
		}
	}

	// Check for normal errors
	if err != nil {
		log.Println("WARN", err)
		return ""
	}

	// Add hash value
	return fmt.Sprintf("%0x", hash.Sum(nil))
}

// Dup type describes found duplicate file
type Dup struct {
	Node      // Embed Node type Go type "inheritance"
	Count int // Number of identical copies for the hash
}

// Value implements mapreduce.Value interface
func (d Dup) Value() interface{} {
	return d
}

// Pretty printer
func (d Dup) String() string {
	return fmt.Sprintf("%s:%d:%d:%q", d.Hash, d.Count, d.Size, d.Path)
}
