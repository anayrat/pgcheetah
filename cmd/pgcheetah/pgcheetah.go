package main

import (
	"flag"
	"fmt"
	"github.com/anayrat/pgcheetah/pkg/pgcheetah"
	"log"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"
)

// Preallocate 100k transactions
var data = make(map[int][]string, 100000)

var delayXactUs int
var start time.Time
var wg sync.WaitGroup
var worker pgcheetah.Worker
var done chan bool

// Command line arguments
var clients = flag.Int("clients", 100, "number of client")
var connStr = flag.String("constr", "user=postgres dbname=postgres", "pg connstring")
var debug = flag.Bool("debug", false, "debug mode")
var delaystart = flag.Int("delaystart", 0, "spread client start among seconds")
var delayxact = flag.Float64("delayxact", 5, "millisecond between each transaction")
var duration = flag.Int("duration", 0, "Test duration")
var interval = flag.Int("interval", 1, "Interval stats report")
var queryfile = flag.String("queryfile", "", "file containing queries to play")
var thinktimemax = flag.Int("thinktimemax", 5, "millisecond thinktime")
var thinktimemin = flag.Int("thinktimemin", 5, "millisecond thinktime")
var tps = flag.Float64("tps", 0, "expected tps")
var netpprof = flag.Bool("netpprof", false, "enable internal pprof web server")

// Global counters
var (
	queriesCount int64
	xactCount    int64
)

func main() {

	data[0] = []string{""}
	waitEvent := make(map[string]int)
	done = make(chan bool)
	var timer *time.Timer
	think := pgcheetah.ThinkTime{Distribution: "uniform", Min: 0, Max: 5}
	s := pgcheetah.State{Statedesc: "init", Xact: 0, XactInProgress: false}

	flag.Parse()
	if *queryfile == "" {
		log.Println("Provide queryfile with -queryfile")
		os.Exit(1)
	}

	if *netpprof {
		go func() {
			log.Println("Start pprof http server on http://localhost:6060/debug/pprof/")
			log.Println(http.ListenAndServe("localhost:6060", nil))
		}()
	}
	think.Min = *thinktimemin
	think.Max = *thinktimemax

	// Convert delayxact from ms to µs
	delayXactUs = int(*delayxact * 1000)

	// Initiate timer, will be reseted later
	timer = time.NewTimer(time.Duration(*duration) * time.Second)
	timer.Stop()

	// capture ctrl+c or end of timer to stop workers and display wait_event counters
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		select {
		case <-c:
			log.Print("Stop requested, stop clients\n")
			for i := 0; i < (*clients + 1); i++ {
				done <- true
			}
			log.Print("Wait_event count:\n")
			for w, c := range waitEvent {
				fmt.Printf("%s - %d\n", w, c)
			}
		case <-timer.C:

			log.Print("Test finished, stop clients\n")
			for i := 0; i < (*clients + 1); i++ {
				done <- true
			}
			log.Print("Wait_event count:\n")
			for w, c := range waitEvent {
				fmt.Printf("%s	- %d\n", w, c)
			}
		}
	}()

	// Add waitgroup for all clients
	wg.Add(*clients)

	log.Println("Start parsing")
	xact, err := pgcheetah.ParseXact(data, queryfile, &s, debug)
	if err != nil {
		log.Fatalf("Error during parsing %s", err)
	}
	log.Println("Parsing done, start workers. Transactions processed:", xact)

	go rateLimiter()

	worker.ConnStr = connStr
	worker.Dataset = data
	worker.DelayXactUs = &delayXactUs
	worker.Done = done
	worker.QueriesCount = &queriesCount
	worker.Think = &think
	worker.Wg = &wg
	worker.XactCount = &xactCount

	for i := 0; i < *clients; i++ {
		time.Sleep(time.Duration(*delaystart*1000 / *clients) * time.Millisecond)
		go pgcheetah.WorkerPG(worker)
	}
	log.Println("All workers launched")

	// Workers had already processed transactions before all worker have been started.
	// Reset counter in order to have accurate stats at the end of the test.
	atomic.StoreInt64(&queriesCount, 0)
	atomic.StoreInt64(&xactCount, 0)
	start = time.Now()

	// Start timer
	if *duration != 0 {
		timer.Reset(time.Duration(*duration) * time.Second)
	}

	go pgcheetah.WaitEventCollector(waitEvent, connStr)

	wg.Wait()

}

// Naive tps limiting/throttle
func rateLimiter() {

	// Start rate limiter after workers. Keep it simple without
	// synchronisation.
	time.Sleep(time.Duration(*delaystart+1) * time.Second)
	var prevXactCount int64
	var prevQueriesCount int64
	var curtps float64
	step := 10 // 10µs by default
	wg.Add(1)

	// Loop every 100ms to calculate throttle to reach wanted tps
	for i := 0; true; i++ {

		curtps = float64(xactCount-prevXactCount) * 10

		// Reports stats for each inverval
		if i%(*interval*10) == 0 {
			if *duration == 0 {
				log.Printf("TPS: %.f QPS: %d Xact: %d Queries: %d Delay: %s Test duration: %.fs\n",
					curtps, (queriesCount-prevQueriesCount)*10, xactCount, queriesCount,
					time.Duration(delayXactUs)*time.Microsecond, time.Since(start).Seconds())
			} else {
				log.Printf("TPS: %.f QPS: %d Xact: %d Queries: %d Delay: %s Remaining: %.fs\n",
					curtps, (queriesCount-prevQueriesCount)*10, xactCount, queriesCount,
					time.Duration(delayXactUs)*time.Microsecond, float64(*duration)-time.Since(start).Seconds())
			}
		}
		if *tps != 0 {

			// We change the step if we are above +/- 1% of wanted tps
			if int64(curtps) > int64(*tps*(1+0.01)) {

				// step is calculated in order to, the more we have a difference between wanted tps and current tps
				// bigger the step is. Inversely, the more we are close to desirated tps, smaller is the step.
				// The empirical formula is:
				// step = 10 * deltatps ^ 1.6 + 20 * deltatps
				// where delta tps is a ratio between wanted tps and current tps.

				step = int(10*math.Pow(curtps / *tps, 1.6) + 30*curtps / *tps)
				//log.Printf("> TPS: %d	- tps diff %d	Delay: %d => %d\n", (xactCount-prevXactCount)*10, int64(*tps*(1+0.1)), delayXactUs, delayXactUs+step)

			} else if int64(curtps) < int64(*tps*(1-0.01)) {

				// We keep the min between calculated step and current delayXactUs to avoid negative delayXactUs
				step = -int(math.Min(10*math.Pow(*tps/curtps, 1.6)+30**tps/curtps, float64(delayXactUs)))
				//log.Printf("< TPS: %d	- tps diff %d	Delay: %d => %d\n", (xactCount-prevXactCount)*10, int64(*tps*(1-0.1)), delayXactUs, delayXactUs-step)
			}
			delayXactUs += step
		}
		prevXactCount = xactCount
		prevQueriesCount = queriesCount
		time.Sleep(100 * time.Millisecond)
		select {
		case <-done:

			t := time.Now()
			elapsed := t.Sub(start)
			log.Printf("End test - Clients: %d - Elapsed: %s - Average TPS: %.f - Average QPS: %.f\n",
				*clients, elapsed.String(), float64(xactCount)/elapsed.Seconds(), float64(queriesCount)/elapsed.Seconds())
			wg.Done()
			return
		default:

		}

	}

}
