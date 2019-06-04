package node

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"os"
)

// Node type
type Node struct {
	Path string // File path
	Size int64  // File size
	Hash string // String form of SHA1 hash
}

// Value returns node as a generic value.
func (n *Node) Value() interface{} {
	return n
}

// CalculateHash calculates SHA1 value of the Node.
func (n *Node) CalculateHash() error {
	// Open file
	file, err := os.Open(n.Path)
	if err != nil {
		log.Println("WARN", err)
		return err
	}
	// Never forget to close it
	defer func() {_ = file.Close()}()

	var nbytes int64 // bytes read
	hash := sha1.New()

	// Always read no more that the file size already determined
	nbytes, err = io.CopyN(hash, file, n.Size) // Use io.CopyN() for optimal filesystem and memory use
	if err != nil {
		log.Println("WARN", err)
		return err
	}

	// Paranoid sanity check
	if nbytes != n.Size {
		err = errors.New("Partial read: " + n.Path)
		log.Println("WARN", err)
		return err
	}

	// Add hash value
	n.Hash = hex.EncodeToString(hash.Sum(nil))
	return nil
}
