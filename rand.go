package main

import (
	"math/rand/v2"
	"strings"
)

// Excluded for clarity: I, l, 1, O, o, 0
const humanCharset = "abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789"

func randomString(n int) string {
	// More efficient than make() because String() avoids copying
	// the buffer.
	b := strings.Builder{}
	b.Grow(n)

	for range n {
		b.WriteByte(humanCharset[rand.IntN(len(humanCharset))])
	}

	return b.String()
}
