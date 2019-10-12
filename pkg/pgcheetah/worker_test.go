package pgcheetah

import (
	"flag"
	"sync"
	"testing"
	"time"
)

var connStr = flag.String("constr", "user=postgres dbname=postgres", "pg connstring")
var data = make(map[int][]string, 100000)

var wg sync.WaitGroup
var (
	queriesCount int64
	xactCount    int64
)

func TestWorkerPG(t *testing.T) {
	think := ThinkTime{Distribution: "uniform", Min: 0, Max: 5}
	data[0] = []string{"SELECT 1;"}
	done := make(chan bool)
	var worker Worker
	delayXactUs := 100

	worker.ConnStr = connStr
	worker.Dataset = data
	worker.DatasetFraction = 0.5
	worker.DelayXactUs = &delayXactUs
	worker.Done = done
	worker.QueriesCount = &queriesCount
	worker.Think = &think
	worker.Wg = &wg
	worker.XactCount = &xactCount

	go WorkerPG(worker)

	worker.DatasetFraction = 1
	go WorkerPG(worker)
	time.Sleep(time.Duration(1) * time.Second)
}
func TestWaitEventCollector(t *testing.T) {

	waitEvent := make(map[string]int)
	go WaitEventCollector(waitEvent, connStr, 500)
	time.Sleep(time.Duration(1) * time.Second)

}
