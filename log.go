package etcdcache

import (
	"fmt"
	"log"
)

// LogFunc is a log function with the same interface as fmt.Printf
type LogFunc func(pattern string, v ...interface{})

// LogWithPrefix creates a LogFunc using the `log` package, prefixing each line with the given prefix.
func LogWithPrefix(prefix string) LogFunc {
	return func(pattern string, v ...interface{}) {
		log.Print(prefix, fmt.Sprintf(pattern, v...))
	}
}
