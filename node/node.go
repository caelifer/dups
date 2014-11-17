package node

import (
	"crypto/sha1"
	"errors"
	"io"
	"log"
	"os"
)

const blockSize = 4096 // Guestimate of a FS block-size for optimal read call

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
	file, err := os.Open(n.Path)
	if err != nil {
		log.Println("WARN", err)
		return err
	}
	// Never forget to close it
	defer file.Close()

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
	// node.Hash = fmt.Sprintf("%0x", hash.Sum(nil))
	n.Hash = hashToString(hash.Sum(nil))
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
