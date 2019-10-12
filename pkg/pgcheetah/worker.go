package pgcheetah

import (
	"context"
	"github.com/jackc/pgx/v4"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// The Worker type contains all informations needed to start a WorkerPG.
// Earch Worker has access to several shared structures through pointers.
type Worker struct {
	ConnStr         *string          // URI or a DSN connection string
	Dataset         map[int][]string // Dataset containing all transactions
	DatasetFraction float64          // Fraction of dataset to use
	DelayXactUs     *int             // Delay to limit global throughput
	Done            chan bool        // Used to stop workers
	QueriesCount    *int64           // Global counter for queries
	Think           *ThinkTime       // Used to add random delay between each query
	Wg              *sync.WaitGroup
	XactCount       *int64 // Global counter for transactions
}

// WorkerPG execute all queries from a randomly
// chosen transaction.
// If ThinkTime is specified, add a random delay between Think.Min ms
// and Think.Max ms after each query.
// Also add a delay after earch transaction to limit global throughput.
func WorkerPG(w Worker) {

	var randXact, i int
	cfg, err := pgx.ParseConfig(*w.ConnStr)
	if err != nil {
		log.Fatal(err)
	}
	// Use simple protocol in order to work with pgbouncer
	cfg.PreferSimpleProtocol = true
	db, err := pgx.ConnectConfig(context.Background(), cfg)

	if err != nil {
		log.Fatal(err, " Connection params : ", string(*w.ConnStr))
	}
	setSize := len(w.Dataset)
	func() {
		for {
			if w.DatasetFraction != 1.0 {
				randXact = int(float64(rand.Intn(setSize)) * w.DatasetFraction)
			} else {
				randXact = rand.Intn(setSize)
			}
			for i = 0; i < len(w.Dataset[randXact]); i++ {
				_, err = db.Exec(context.Background(), w.Dataset[randXact][i])

				// Ignore SQL error
				//if err != nil {
				//	log.Fatal(err)
				//}

				atomic.AddInt64(w.QueriesCount, 1)

				// Avoid ThinkTime calculaton when not necessary
				if (*w.Think).Max != 0 {
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
	err = db.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	w.Wg.Done()

}

// WaitEventCollector collects postgres wait event every 500ms
// All wait events are stored in a map.
func WaitEventCollector(we map[string]int, connStr *string, weInterval int) {

	var count int
	var waitEvent, pgVersion string
	var waitEventQuery = make(map[string]string, 3)
	cfg, _ := pgx.ParseConfig(*connStr)
	// use simple protocol in order to work with pgbouncer
	cfg.PreferSimpleProtocol = true
	db, err := pgx.ConnectConfig(context.Background(), cfg)

	if err != nil {
		log.Fatal(err, " Connection params : ", string(*connStr))
	}
	defer db.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	// Wait event query for postgres 9.6
	waitEventQuery["90600"] = `SELECT
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
	// Wait event query for postgres 10
	waitEventQuery["100000"] = `SELECT
				  wait_event_type || '-' || wait_event as wait_event,
				  count(*) as count
				FROM
				  pg_stat_activity
				WHERE
				   wait_event IS NOT NULL
				   AND
				   backend_type = 'client backend'
				GROUP BY
				  wait_event_type,
				  wait_event;
`
	// Wait event query for postgres 11
	waitEventQuery["110000"] = `SELECT
				  wait_event_type || '-' || wait_event as wait_event,
				  count(*) as count
				FROM
				  pg_stat_activity
				WHERE
				   wait_event IS NOT NULL
				   AND
				   backend_type = 'client backend'
				GROUP BY
				  wait_event_type,
				  wait_event;
`

	err = db.QueryRow(context.Background(), "SELECT (100*(setting::int/100))::text FROM pg_catalog.pg_settings WHERE name IN ('server_version_num') ORDER BY name = 'server_version_num';").Scan(&pgVersion)
	if err != nil {
		log.Fatal(err)
	}

	for {
		row, err := db.Query(context.Background(), waitEventQuery[pgVersion])
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

		time.Sleep(time.Duration(weInterval) * time.Millisecond)

	}

}
