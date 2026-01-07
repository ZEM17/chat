package pool

import (
	"bytes"
	"sync"
)

// Global Buffer Pool
// We use bytes.Buffer because it's convenient for concatenation and JSON encoding.
var BufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

// GetBuffer returns a buffer from the pool
func GetBuffer() *bytes.Buffer {
	return BufferPool.Get().(*bytes.Buffer)
}

// PutBuffer resets and returns the buffer to the pool
func PutBuffer(buf *bytes.Buffer) {
	buf.Reset()
	BufferPool.Put(buf)
}
