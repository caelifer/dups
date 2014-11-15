package finder

import (
	"fmt"

	"github.com/caelifer/dups/node"
)

// Dup type describes found duplicate file
type Dup struct {
	*node.Node     // Embed Node type Go type "inheritance"
	Count      int // Number of identical copies for the hash
}

// Value implements mapreduce.Value interface
func (d Dup) Value() interface{} {
	return d
}

// Pretty printer for the report
func (d Dup) String() string {
	return fmt.Sprintf("%s:%d:%d:%q", d.Hash, d.Count, d.Size, d.Path)
}
