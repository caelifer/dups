package main

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
)

// Node type
type Node struct {
	Path string // File path
	Size int64  // File size
}

func (n Node) Value() interface{} {
	return n
}

const blockSize = 1024 // Guestimate of a FS block-size for optimal read call

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
			err = errors.New("Partial read")
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
