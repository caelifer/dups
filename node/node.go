package node

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"io"
	"log"

	"golang.org/x/exp/mmap"
)

const blockSize = 1024 * 4 // Guestimate of a block-size for optimal read call

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
func (n *Node) CalculateHash(fast bool) error {
	readSize := n.Size // by default, read the entire file

	if fast {
		if n.Size > blockSize {
			readSize = blockSize // Limit number of read bytes to BlockSize on fast pass
		} else {
			// Skip small file on a "fast" pass
			return nil
		}
	}

	// Open file
	rat, err := mmap.Open(n.Path)
	if err != nil {
		log.Println("WARN", err)
		return err
	}
	// Never forget to close it
	defer rat.Close()

	// Wrap MMap ReaderAt with io.SectionReader
	file := io.NewSectionReader(rat, 0, int64(rat.Len()))

	var nbytes int64 // bytes read
	hash := sha1.New()

	// Always read no more that the file size already determined
	nbytes, err = io.CopyN(hash, file, readSize) // Use io.CopyN() for optimal filesystem and memory use
	if err != nil {
		log.Println("WARN", err)
		return err
	}

	// Paranoid sanity check
	if nbytes != readSize {
		err = errors.New("Partial read: " + n.Path)
		log.Println("WARN", err)
		return err
	}

	// Add hash value
	n.Hash = hex.EncodeToString(hash.Sum(nil))
	return nil
}
