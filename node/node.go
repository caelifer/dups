package node

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
)

const BlockSize = 4096 // Guestimate of a FS block-size for optimal read call

// Node type
type Node struct {
	Path string // File path
	Size int64  // File size
	Hash string // String form of SHA1 hash
}

// Implement mapreduce Value interface
func (n *Node) Value() interface{} {
	return n
}

// Calculate hash
func (node *Node) CalculateHash(fast bool) error {
	readSize := node.Size // by default, read the entire file

	if fast {
		if node.Size > BlockSize {
			readSize = BlockSize // Limit number of read bytes to BlockSize on fast pass
		} else {
			// Skip small file on a "fast" pass
			return nil
		}
	}

	// Open file
	file, err := os.Open(node.Path)
	if err != nil {
		log.Println("WARN", err)
		return err
	}
	// Never forget to close it
	defer file.Close()

	var n int64 // bytes read
	hash := sha1.New()

	// Always read no more that the file size already determined
	n, err = io.CopyN(hash, file, readSize) // Use io.CopyN() for optimal filesystem and memory use
	if err != nil {
		log.Println("WARN", err)
		return err
	}

	// Paranoid sanity check
	if n != readSize {
		err = errors.New("Partial read: " + node.Path)
		log.Println("WARN", err)
		return err
	}

	// Add hash value
	// node.Hash = fmt.Sprintf("%0x", hash.Sum(nil))
	node.Hash = hashToString(hash.Sum(nil))
	return nil
}

func halfByteToHex(b byte) byte {
	c := b & 0xf
	switch c {
	case 0, 1, 2, 3, 4, 5, 6, 7, 8, 9:
		return byte('0') + c
	case 0xa, 0xb, 0xc, 0xd, 0xe, 0xf:
		return byte('a') + c - 0xa
	}
	return 0 // never reached
}

func byteToHex(b byte) (byte, byte) {
	return halfByteToHex(b >> 4), halfByteToHex(b)
}

func hashToString(bts []byte) string {
	res := make([]byte, len(bts)*2)
	for i, b := range bts {
		res[i*2], res[i*2+1] = byteToHex(b)
	}
	return string(res)
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

// Pretty printer for the report
func (d Dup) String() string {
	return fmt.Sprintf("%s:%d:%d:%q", d.Hash, d.Count, d.Size, d.Path)
}
