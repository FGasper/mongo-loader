package main

import (
	"math/rand/v2"

	pool "github.com/libp2p/go-buffer-pool"
)

// Excluded for clarity: I, l, 1, O, o, 0
const humanCharset = "abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789"

func randomString(n int) string {
	b := pool.Get(n)
	defer pool.Put(b)

	for i := range b {
		b[i] = humanCharset[rand.IntN(len(humanCharset))]
	}

	return string(b)
}
