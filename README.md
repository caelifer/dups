# Dups - a fast duplicates finder [![Go Report Card](https://goreportcard.com/badge/github.com/caelifer/dups)](https://goreportcard.com/report/github.com/caelifer/dups)
`dups` finds duplicate files in the supplied directories regardless the file name. It also calculates the amount of wasted storage along the way.

## Installation
```
go get -u github.com/caelifer/dups
```
## Usage
```
dups -h
Usage of dups:
  -cpuprofile string
    	write cpu profile to file
  -jbuffer int
    	Number of pending work units (default 1024)
  -memprofile string
    	write memory profile to file
  -output string
    	write output to a file. Default: STDOUT (default "-")
  -stats
    	display runtime statistics on STDERR
  -tracefile string
    	write trace output to a file
  -workers int
    	Number of parallel jobs (default 64)
```
