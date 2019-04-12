package main

import (
	"flag"
	"fmt"
	"github.com/anayrat/pgcheetah/pkg/pgcheetah"
	"log"
	"os"
	"os/signal"
	"runtime/pprof"
	"runtime/trace"
	"sync"
	"sync/atomic"
	"time"
)

// we preallocate 100k transactions
var data = make(map[int][]string, 100000)

var wg sync.WaitGroup
var worker pgcheetah.Worker

// Command line arguments
var clients = flag.Int("clients", 100, "number of client")
var configFile = flag.String("configfile", "", "configfile")
var connStr = flag.String("constr", "user=postgres dbname=postgres", "pg connstring")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var debug = flag.Bool("debug", false, "debug mode")
var delaystart = flag.Int("delaystart", 0, "spread client start among seconds")
var delayxact = flag.Float64("delayxact", 5, "millisecond between each transaction")
var duration = flag.Int("duration", 0, "Test duration")
var queryfile = flag.String("queryfile", "", "file containing queries to play")
var thinktimemax = flag.Int("thinktimemax", 5, "millisecond thinktime")
var thinktimemin = flag.Int("thinktimemin", 5, "millisecond thinktime")
var tps = flag.Float64("tps", 0, "expected tps")
var traceprofile = flag.String("traceprofile", "", "write trace to file")

// Global counters
var (
	queriesCount int64 = 0
	xactCount    int64 = 0
)

func main() {

	data[0] = []string{""}
	wait_event := make(map[string]int)
	done := make(chan bool)
	var timer *time.Timer
	think := pgcheetah.Thinktime{"uniform", 0, 5}
	s := pgcheetah.State{"init", 0, false}
	var start time.Time

	flag.Parse()
	think.Min = *thinktimemin
	think.Max = *thinktimemax

	// Start profiling if enabled
	pproofing()

	// Start duration timer
	if *duration != 0 {
		timer = time.NewTimer(time.Duration(*duration) * time.Second)
	}

	// capture ctrl+c or end of timer to stop workers and display wait_event counters
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		select {
		case <-c:
			log.Print("Stop requested, stop clients\n")
			cleanup()
			for i := 0; i < (*clients + 1); i++ {
				done <- true
			}
			log.Print("Wait_event count:\n")
			for w, c := range wait_event {
				fmt.Printf("%s - %d\n", w, c)
			}
		case <-timer.C:

			log.Print("Test finished, stop clients\n")
			cleanup()
			for i := 0; i < (*clients + 1); i++ {
				done <- true
			}
			log.Print("Wait_event count:\n")
			for w, c := range wait_event {
				fmt.Printf("%s	- %d\n", w, c)
			}
		}
	}()

	// Add waitgroup for all clients
	wg.Add(*clients)

	log.Println("Start parsing")
	xact := pgcheetah.ParseXact(data, queryfile, &s, debug)

	log.Println("Parsing done, start workers. Transactions processed:", xact)

	//Naive tps limiting
	go func() {
		time.Sleep(time.Duration(*delaystart) * time.Second)
		var prevXactCount int64 = 0
		var prevQueriesCount int64 = 0
		for i := 0; true; i++ {

			if i%10 == 0 {
				log.Printf("TPS: %d QPS: %d Xact: %d Queries: %d Delay: %.1fms\n", (xactCount-prevXactCount)*10, (queriesCount-prevQueriesCount)*10, xactCount, queriesCount, *delayxact)
			}
			if *tps != 0 {
				if (xactCount-prevXactCount)*10 > int64(*tps*(1+0.1)) {
					//log.Printf("> TPS: %d	- tps diff %d	Delay: %.2fms\n", (xactCount-prevXactCount)*10, int64(*tps*(1+0.1)), *delayxact)
					*delayxact += 0.1

				} else if *delayxact > 0.0 && (xactCount-prevXactCount)*10 < int64(*tps*(1-0.1)) {
					//log.Printf("< TPS: %d	- tps diff %d	Delay: %.2fms\n", (xactCount-prevXactCount)*10, int64(*tps*(1-0.1)), *delayxact)
					*delayxact -= 0.1
				}
			}
			prevXactCount = xactCount
			prevQueriesCount = queriesCount
			time.Sleep(100 * time.Millisecond)
			select {
			case <-done:

				t := time.Now()
				elapsed := t.Sub(start)
				log.Printf("End test - Elapsed %s - Average TPS: %.f - Average QPS: %.f\n", elapsed.String(), float64(xactCount)/elapsed.Seconds(), float64(queriesCount)/elapsed.Seconds())
				return
			default:

			}

		}

	}()

	go pgcheetah.WaitEventCollector(wait_event, connStr)

	worker.ConnStr = connStr
	worker.Dataset = data
	worker.Delayxact = delayxact
	worker.Done = done
	worker.QueriesCount = &queriesCount
	worker.Think = &think
	worker.Wg = &wg
	worker.XactCount = &xactCount

	for i := 0; i < *clients; i++ {
		worker.Num = i
		time.Sleep(time.Duration(*delaystart*1000 / *clients) * time.Millisecond)
		go pgcheetah.WorkerPGv2(worker)
	}
	log.Println("All workers launched")
	// Workers had already processed transactions before all worker are started.
	//Reset counter in order to have accurate stats at the end of the test.
	atomic.StoreInt64(&queriesCount, 0)
	atomic.StoreInt64(&xactCount, 0)
	start = time.Now()

	wg.Wait()
}

func pproofing() {
	switch {
	case *cpuprofile != "":
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		log.Println("Start cpuprofiler")
	case *traceprofile != "":
		f, err := os.Create(*traceprofile)
		if err != nil {
			log.Fatal(err)
		}
		log.Println("Start trace")
		trace.Start(f)
	}
}

func cleanup() {
	switch {
	case *cpuprofile != "":
		pprof.StopCPUProfile()
	case *traceprofile != "":
		trace.Stop()
	}

}
