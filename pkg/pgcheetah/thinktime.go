package pgcheetah

import (
	"math/rand"
)

type Thinktime struct {
	Distribution string
	Min          int
	Max          int
}

func ThinkTime(t Thinktime) int {
	switch t.Distribution {
	case "uniform":
		return int(float32(t.Min) + float32((t.Max+1-t.Min))*rand.Float32())
		//case "normal":
		// TODO : add normal distribution generator
	}
	return 0
}
