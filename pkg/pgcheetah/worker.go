package pgcheetah

import (
	"database/sql"
	"github.com/jackc/pgx"
	_ "github.com/lib/pq"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type Worker struct {
	ConnStr      *string
	Dataset      map[int][]string
	Delayxact    *float64
	Done         chan bool
	Num          int
	QueriesCount *int64
	Think        *Thinktime
	Wg           *sync.WaitGroup
	XactCount    *int64
}

//standard driver
func WorkerPG(w Worker) {
	var randxact, i int

	db, err := sql.Open("postgres", *w.ConnStr)
	if err != nil {
		log.Fatal(err)
	}

	db.Ping()
	func() {
		for {
			randxact = rand.Intn(len(w.Dataset))
			for i = 0; i < len(w.Dataset[randxact]); i++ {
				_, err = db.Exec(w.Dataset[randxact][i])
				//if err != nil {
				//	log.Fatal(err)
				//}

				atomic.AddInt64(w.QueriesCount, 1)

				time.Sleep(time.Duration(ThinkTime(*w.Think)) * time.Millisecond)

			}
			time.Sleep(time.Duration(*w.Delayxact) * time.Millisecond)
			atomic.AddInt64(w.XactCount, 1)
			select {
			case <-w.Done:
				return
			default:

			}

		}
	}()
	db.Close()
	w.Wg.Done()

}

//pgx driver
func WorkerPGv2(w Worker) {

	var randxact, i int
	cfg, err := pgx.ParseConnectionString(*w.ConnStr)
	// use simple protocol in order to work with pgbouncer
	cfg.PreferSimpleProtocol = true
	db, err := pgx.Connect(cfg)

	if err != nil {
		log.Fatal(err)
	}

	func() {
		for {
			randxact = rand.Intn(len(w.Dataset))
			for i = 0; i < len(w.Dataset[randxact]); i++ {
				_, err = db.Exec(w.Dataset[randxact][i])
				//if err != nil {
				//	log.Fatal(err)
				//}

				atomic.AddInt64(w.QueriesCount, 1)

				time.Sleep(time.Duration(ThinkTime(*w.Think)) * time.Millisecond)
				select {
				case <-w.Done:
					return
				default:

				}

			}
			time.Sleep(time.Duration(*w.Delayxact) * time.Millisecond)
			atomic.AddInt64(w.XactCount, 1)
		}
	}()
	db.Close()
	w.Wg.Done()

}
func WaitEventCollector(we map[string]int, connStr *string) {

	var count int
	var wait_event string
	cfg, _ := pgx.ParseConnectionString(*connStr)
	// use simple protocol in order to work with pgbouncer
	cfg.PreferSimpleProtocol = true
	db, err := pgx.Connect(cfg)

	if err != nil {
		log.Fatal(err)
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
			err = row.Scan(&wait_event, &count)

			we[wait_event] += count
			if err != nil {
				log.Fatal(err)
			}
		}

		time.Sleep(500 * time.Millisecond)

	}
	db.Close()

}
