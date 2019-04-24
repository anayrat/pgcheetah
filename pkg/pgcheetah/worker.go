package pgcheetah

import (
	"github.com/jackc/pgx"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// The Worker type contains all informations needed to start a WorkerPG.
// Earch Worker has access to several shared structures through pointers:
// - The Dataset containing all transactions
// - DelayXactUs to limit global throughput
// - Two global counters for transactions and queries, XactCount and
// QueriesCount respectively
// - A Done channel used to stop worker
// - A WaitGroup to wait all workers ended
type Worker struct {
	ConnStr      *string
	Dataset      map[int][]string
	DelayXactUs  *int
	Done         chan bool
	QueriesCount *int64
	Think        *ThinkTime
	Wg           *sync.WaitGroup
	XactCount    *int64
}

// WorkerPG execute all queries from a randomly
// chosen transaction.
// If ThinkTime is specified, add a random delay between Think.Min ms
// and Think.Max ms after each query.
// Also add a delay after earch transaction to limit global throughput.
func WorkerPG(w Worker) {

	var randXact, i int
	cfg, err := pgx.ParseConnectionString(*w.ConnStr)
	if err != nil {
		log.Fatal(err)
	}
	// Use simple protocol in order to work with pgbouncer
	cfg.PreferSimpleProtocol = true
	db, err := pgx.Connect(cfg)

	if err != nil {
		log.Fatal(err, " Connection params : ", string(*w.ConnStr))
	}

	func() {
		for {
			randXact = rand.Intn(len(w.Dataset))
			for i = 0; i < len(w.Dataset[randXact]); i++ {
				_, err = db.Exec(w.Dataset[randXact][i])

				// Ignore SQL error
				//if err != nil {
				//	log.Fatal(err)
				//}

				atomic.AddInt64(w.QueriesCount, 1)

				// Avoid ThinkTime calculaton when not necessary
				if (*w.Think).Min != 0 && (*w.Think).Max != 0 {
					time.Sleep(time.Duration(ThinkTimer(*w.Think)) * time.Millisecond)
				}
				select {
				case <-w.Done:
					return
				default:

				}

			}
			time.Sleep(time.Duration(*w.DelayXactUs) * time.Microsecond)
			atomic.AddInt64(w.XactCount, 1)
		}
	}()
	db.Close()
	w.Wg.Done()

}

// WaitEventCollector collects postgres wait event every 500ms
// All wait events are stored in a map.
func WaitEventCollector(we map[string]int, connStr *string) {

	var count int
	var waitEvent string
	cfg, _ := pgx.ParseConnectionString(*connStr)
	// use simple protocol in order to work with pgbouncer
	cfg.PreferSimpleProtocol = true
	db, err := pgx.Connect(cfg)
	defer db.Close()

	if err != nil {
		log.Fatal(err, " Connection params : ", string(*connStr))
	}

	// Wait event query for postgres 9.6
	query := `SELECT
				  wait_event_type || '-' || wait_event as wait_event,
				  count(*) as count
				FROM
				  pg_stat_activity
				WHERE
				   wait_event IS NOT NULL
				GROUP BY
				  wait_event_type,
				  wait_event;
`

	for {
		row, err := db.Query(query)
		if err != nil {
			log.Fatal(err)
		}
		for row.Next() {
			err = row.Scan(&waitEvent, &count)

			we[waitEvent] += count
			if err != nil {
				log.Fatal(err)
			}
		}

		time.Sleep(500 * time.Millisecond)

	}

}
