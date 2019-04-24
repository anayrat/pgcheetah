package pgcheetah

import (
	"math/rand"
)

// ThinkTime describe the time a WorkerPG wait between
// each query. The idea is to pass a distribution and
// a time range.
// Actually there is only one Distribution : uniform.
type ThinkTime struct {
	Distribution string
	Min          int
	Max          int
}

// ThinkTimer returns a random value between Min and Max
// according to a Distribution.
// Actually there is only one Distribution : uniform.
func ThinkTimer(t ThinkTime) int {
	switch t.Distribution {
	case "uniform":
		return int(float32(t.Min) + float32((t.Max+1-t.Min))*rand.Float32())
		//case "normal":
		// TODO : add normal distribution generator
	}
	return 0
}
