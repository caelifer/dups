package main

import (
	"fmt"
	"log"
	"os"

	"github.com/caelifer/dups/balancer"
	"github.com/caelifer/dups/fstree"
)

func main() {
	for _, root := range os.Args[1:] {
		err := fstree.Walk(balancer.NewWorkQueue(4), root, func(path string, info os.FileInfo, err error) error {
			fmt.Println(path)
			return nil
		})
		if err != nil {
			log.Fatal(err)
		}
	}
}
