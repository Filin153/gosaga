package gosaga

import (
	"crypto/rand"
	"time"
)

// Hooks allow tests to stub time.Sleep and randomness without changing production behavior.
var (
	sleep      = time.Sleep
	randReader = rand.Read
)
